package cluster

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"go.etcd.io/bbolt"
)

// The cluster's metadata log grows by an entry for each change of placement
// or leadership, which is few: Raft's default of a snapshot every 8,192
// entries meant a cluster never had one. Every start replayed the whole log,
// which only grew, and the node acted on each assignment it went through on
// the way (see MetadataCurrent). The state is small (the nodes and one record
// per partition), so a snapshot is cheap and is taken after a few changes,
// and once more when the node stops.
const (
	// metadataSnapshotThreshold is how many entries since the last snapshot
	// make the next check (every metadataSnapshotInterval) take one.
	metadataSnapshotThreshold = 128
	metadataSnapshotInterval  = 60 * time.Second
	// metadataTrailingLogs is how many entries a snapshot leaves in the log, for
	// followers that are briefly behind; one further behind is sent the
	// snapshot.
	metadataTrailingLogs = 1024
	// metadataSettle is how long a follower's commit index has to stay where it
	// is, with everything up to it applied, before the follower counts its copy
	// as caught up after a start. Entries sent to catch it up come in batches
	// one after another, and between two of them it has applied all it holds.
	metadataSettle = time.Second
	// metadataBarrierTimeout bounds the wait of a new leader for the entries
	// committed before its term to be applied.
	metadataBarrierTimeout = 10 * time.Second
)

// RaftNode manages HashiCorp Raft consensus for durable cluster metadata
// (nodes, partition assignments) shared across the cluster.
type RaftNode struct {
	config        *ClusterConfig
	raft          *raft.Raft
	fsm           *ClusterFSM
	transport     *raft.NetworkTransport
	logStore      raft.LogStore
	stableStore   raft.StableStore
	snapshotStore raft.SnapshotStore

	// contactWithin is how recently a follower must have heard from the leader
	// for its copy to count as caught up: the election timeout.
	contactWithin time.Duration
	// caughtUp is set once this process's copy of the metadata has caught up
	// with the cluster's; see MetadataCurrent.
	caughtUp atomic.Bool
	// currentMu guards the rest, which MetadataCurrent keeps.
	currentMu sync.Mutex
	// settleIndex is the commit index a follower last saw change, at
	// settleSince.
	settleIndex uint64
	settleSince time.Time
	// barrierTerm is the last term in which this node, as leader, has seen
	// every earlier entry applied; barrierRunning is set while it waits for that.
	barrierTerm    uint64
	barrierRunning bool
}

// NewRaftNode creates a RaftNode with Bolt-backed log/stable stores and a file snapshot store.
func NewRaftNode(config *ClusterConfig) (*RaftNode, error) {
	// Create data directory
	if err := os.MkdirAll(config.RaftDataDir, 0755); err != nil {
		return nil, fmt.Errorf("create raft data dir: %w", err)
	}

	// Create FSM
	fsm := NewClusterFSM()

	// Create transport
	transport, err := newRaftTransport(config)
	if err != nil {
		return nil, fmt.Errorf("create transport: %w", err)
	}

	// Create log store and stable store with timeout to prevent blocking
	boltDBPath := filepath.Join(config.RaftDataDir, "raft.db")
	boltDB, err := raftboltdb.New(raftboltdb.Options{
		Path: boltDBPath,
		BoltOptions: &bbolt.Options{
			Timeout: 5 * time.Second, // Don't block forever on lock
		},
	})
	if err != nil {
		return nil, fmt.Errorf("create bolt store: %w", err)
	}

	// Create snapshot store
	snapshotStore, err := raft.NewFileSnapshotStore(config.RaftDataDir, 2, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("create snapshot store: %w", err)
	}

	// Create Raft configuration
	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(config.NodeID)

	// Ensure minimum timeouts for Raft stability
	heartbeatTimeout := config.HeartbeatInterval
	if heartbeatTimeout < 500*time.Millisecond {
		heartbeatTimeout = 500 * time.Millisecond
	}
	electionTimeout := config.ElectionTimeout
	if electionTimeout < 1*time.Second {
		electionTimeout = 1 * time.Second
	}
	// Election timeout must be greater than heartbeat timeout
	if electionTimeout <= heartbeatTimeout {
		electionTimeout = heartbeatTimeout * 2
	}

	raftConfig.HeartbeatTimeout = heartbeatTimeout
	raftConfig.ElectionTimeout = electionTimeout
	raftConfig.CommitTimeout = 500 * time.Millisecond
	raftConfig.SnapshotInterval = metadataSnapshotInterval
	raftConfig.SnapshotThreshold = metadataSnapshotThreshold
	raftConfig.TrailingLogs = metadataTrailingLogs

	// Create Raft instance
	r, err := raft.NewRaft(raftConfig, fsm, boltDB, boltDB, snapshotStore, transport)
	if err != nil {
		return nil, fmt.Errorf("create raft: %w", err)
	}

	return &RaftNode{
		config:        config,
		raft:          r,
		fsm:           fsm,
		transport:     transport,
		logStore:      boltDB,
		stableStore:   boltDB,
		snapshotStore: snapshotStore,
		contactWithin: electionTimeout,
	}, nil
}

// MetadataCurrent reports whether this node's copy of the committed cluster
// metadata may be acted on.
//
// A node that starts restores its last snapshot and then applies the entries
// after it as it learns they are committed; one with no snapshot replays the
// log from the start. On the way its copy names, one after another, every
// leader each partition has had, and the node used to act on each: it took up
// partitions it had led long before, let go of others and loaded them again.
// So the node waits until it has caught up: as a follower, until it has heard
// from the leader, the commit index has reached the end of its log and
// everything up to it has been applied, with the commit index settled for
// metadataSettle; as the leader, until a barrier has seen
// every entry of earlier terms applied. Once caught up, a follower stays so for
// the life of the process, as it lags only briefly; a leader waits again at
// the start of each term, since a node can win an election before it has
// applied everything it holds.
func (n *RaftNode) MetadataCurrent() bool {
	if n.raft.State() == raft.Leader {
		return n.leaderCurrent()
	}
	return n.followerCurrent(n.raft)
}

// raftProgress is what a follower reads of Raft to tell whether it has caught
// up; *raft.Raft has it.
type raftProgress interface {
	State() raft.RaftState
	LastContact() time.Time
	CommitIndex() uint64
	AppliedIndex() uint64
	LastIndex() uint64
}

func (n *RaftNode) followerCurrent(r raftProgress) bool {
	if n.caughtUp.Load() {
		return true
	}
	if r.State() != raft.Follower {
		return false
	}
	contact := r.LastContact()
	if contact.IsZero() || time.Since(contact) > n.contactWithin {
		return false // no leader heard from yet, or not lately
	}
	// Heartbeats tell a follower that the leader is there, not how far it has
	// committed: that comes with the entries, which after a restart can trail
	// the heartbeats by seconds. Until then the commit index is 0, everything
	// up to it is applied, and nothing of the log the node holds has been. So
	// the commit index has to reach the end of that log too.
	commit, applied, last := r.CommitIndex(), r.AppliedIndex(), r.LastIndex()
	n.currentMu.Lock()
	defer n.currentMu.Unlock()
	if commit == 0 || commit < last || applied < commit || commit != n.settleIndex || n.settleSince.IsZero() {
		n.settleIndex, n.settleSince = commit, time.Now()
		return false
	}
	if time.Since(n.settleSince) < metadataSettle {
		return false
	}
	n.caughtUp.Store(true)
	log.Printf("[RAFT] The cluster metadata is caught up (applied through index %d)", applied)
	return true
}

// leaderCurrent reports whether, as leader, this node has seen every entry
// committed before its term applied. The first call in a term starts a
// barrier and reports false until it has completed.
func (n *RaftNode) leaderCurrent() bool {
	term := n.raft.CurrentTerm()
	n.currentMu.Lock()
	defer n.currentMu.Unlock()
	if n.barrierTerm == term {
		return true
	}
	if !n.barrierRunning {
		n.barrierRunning = true
		go func() {
			err := n.raft.Barrier(metadataBarrierTimeout).Error()
			n.currentMu.Lock()
			n.barrierRunning = false
			if err == nil {
				n.barrierTerm = term
				n.caughtUp.Store(true)
			}
			n.currentMu.Unlock()
			if err != nil {
				log.Printf("[RAFT] Waiting for the metadata of earlier terms to be applied failed: %v", err)
			}
		}()
	}
	return false
}

// Bootstrap bootstraps a single-server Raft cluster. Only the first node should call this.
func (n *RaftNode) Bootstrap() error {
	configuration := raft.Configuration{
		Servers: []raft.Server{
			{
				ID:      raft.ServerID(n.config.NodeID),
				Address: raft.ServerAddress(n.config.RaftAddr),
			},
		},
	}

	future := n.raft.BootstrapCluster(configuration)
	if err := future.Error(); err != nil {
		if err != raft.ErrCantBootstrap {
			return fmt.Errorf("bootstrap cluster: %w", err)
		}
		// Already bootstrapped, which is fine
	}

	return nil
}

// Join adds nodeID at addr as a Raft voter. Must be called on the current leader.
func (n *RaftNode) Join(nodeID, addr string) error {
	log.Printf("[RAFT] Requesting to join node %s at %s", nodeID, addr)

	configFuture := n.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return fmt.Errorf("get configuration: %w", err)
	}

	// Check if already joined
	for _, srv := range configFuture.Configuration().Servers {
		if srv.ID == raft.ServerID(nodeID) {
			if srv.Address == raft.ServerAddress(addr) {
				log.Printf("[RAFT] Node %s already member of cluster", nodeID)
				return nil
			}
			// Node exists but with different address, remove it first
			future := n.raft.RemoveServer(srv.ID, 0, 0)
			if err := future.Error(); err != nil {
				return fmt.Errorf("remove existing server: %w", err)
			}
		}
	}

	// Add as voter
	future := n.raft.AddVoter(raft.ServerID(nodeID), raft.ServerAddress(addr), 0, 0)
	if err := future.Error(); err != nil {
		return fmt.Errorf("add voter: %w", err)
	}

	log.Printf("[RAFT] Node %s joined cluster", nodeID)
	return nil
}

// Leave removes nodeID from the Raft configuration. Must be called on the current leader.
func (n *RaftNode) Leave(nodeID string) error {
	log.Printf("[RAFT] Removing node %s from cluster", nodeID)

	future := n.raft.RemoveServer(raft.ServerID(nodeID), 0, 0)
	if err := future.Error(); err != nil {
		return fmt.Errorf("remove server: %w", err)
	}

	return nil
}

// Voter is a node that counts towards a majority in Raft.
type Voter struct {
	ID      string
	Address string
}

// Voters returns the nodes of the Raft configuration, as this node has it.
func (n *RaftNode) Voters() ([]Voter, error) {
	future := n.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		return nil, fmt.Errorf("get raft configuration: %w", err)
	}
	servers := future.Configuration().Servers
	voters := make([]Voter, 0, len(servers))
	for _, srv := range servers {
		voters = append(voters, Voter{ID: string(srv.ID), Address: string(srv.Address)})
	}
	return voters, nil
}

// SetOnNodeRemoved registers what is to happen on this node when a node is
// removed from the cluster.
func (n *RaftNode) SetOnNodeRemoved(fn func(nodeID string)) {
	n.fsm.SetOnNodeRemoved(fn)
}

// HasState reports whether this node belongs to a cluster: it created one or
// was added to one, and has the log or a snapshot to show for it.
func (n *RaftNode) HasState() bool {
	return n.raft.LastIndex() > 0
}

// IsLeader returns true if this node is the Raft leader
func (n *RaftNode) IsLeader() bool {
	return n.raft.State() == raft.Leader
}

// GetLeader returns the current leader address
func (n *RaftNode) GetLeader() string {
	addr, _ := n.raft.LeaderWithID()
	return string(addr)
}

func parseRaftIntStat(stats map[string]string, key string) int64 {
	if stats == nil {
		return 0
	}
	raw, ok := stats[key]
	if !ok || raw == "" {
		return 0
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0
	}
	return value
}

// GetCurrentTerm returns the current Raft term.
func (n *RaftNode) GetCurrentTerm() int64 {
	return parseRaftIntStat(n.raft.Stats(), "term")
}

// GetCommitIndex returns the current committed log index.
func (n *RaftNode) GetCommitIndex() int64 {
	return parseRaftIntStat(n.raft.Stats(), "commit_index")
}

// GetLastApplied returns the last applied log index.
func (n *RaftNode) GetLastApplied() int64 {
	return parseRaftIntStat(n.raft.Stats(), "last_applied")
}

// GetPeers returns all configured raft peer addresses.
func (n *RaftNode) GetPeers() ([]string, error) {
	future := n.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		return nil, fmt.Errorf("get raft configuration: %w", err)
	}

	peers := make([]string, 0, len(future.Configuration().Servers))
	for _, srv := range future.Configuration().Servers {
		peers = append(peers, string(srv.Address))
	}
	return peers, nil
}

// Apply proposes cmd to the Raft log and applies it to the FSM. Fails if not leader.
func (n *RaftNode) Apply(cmd *Command) error {
	if !n.IsLeader() {
		return fmt.Errorf("not leader")
	}

	data, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("marshal command: %w", err)
	}

	future := n.raft.Apply(data, 10*time.Second)
	if err := future.Error(); err != nil {
		return fmt.Errorf("apply: %w", err)
	}

	response := future.Response()
	if err, ok := response.(error); ok {
		return err
	}

	return nil
}

// GetState returns a deep copy of the current cluster state from the FSM.
func (n *RaftNode) GetState() *ClusterState {
	return n.fsm.GetState()
}

// Partition returns the committed assignment of one partition.
func (n *RaftNode) Partition(id int32) (PartitionInfo, bool) {
	return n.fsm.Partition(id)
}

// Partitions returns every committed partition assignment.
func (n *RaftNode) Partitions() map[int32]PartitionInfo {
	return n.fsm.Partitions()
}

// SetOnPartitionChange registers a non-blocking callback that runs whenever a
// partition assignment is applied on this node.
func (n *RaftNode) SetOnPartitionChange(fn func()) {
	n.fsm.SetOnPartitionChange(fn)
}

// ProposePartition commits a partition assignment. assign creates the entry;
// otherwise it updates an existing one, which also raises its epoch.
func (n *RaftNode) ProposePartition(info *PartitionInfo, assign bool) error {
	payload, err := json.Marshal(info)
	if err != nil {
		return fmt.Errorf("marshal partition info: %w", err)
	}
	cmdType := CommandTypeUpdatePartition
	if assign {
		cmdType = CommandTypeAssignPartition
	}
	return n.Apply(&Command{Type: cmdType, Payload: payload})
}

// RequirePartition commits that a partition requires caps of the nodes that
// hold it. See Manager.RequirePartitionCapabilities for when that may be.
func (n *RaftNode) RequirePartition(partitionID int32, caps []string) error {
	payload, err := json.Marshal(partitionRequirement{PartitionID: partitionID, Capabilities: caps})
	if err != nil {
		return fmt.Errorf("marshal partition requirement: %w", err)
	}
	return n.Apply(&Command{Type: CommandTypeRequirePartition, Payload: payload})
}

// WaitForLeader blocks until a Raft leader is known or timeout elapses.
func (n *RaftNode) WaitForLeader(timeout time.Duration) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		select {
		case <-ticker.C:
			if leader := n.GetLeader(); leader != "" {
				return nil
			}
		case <-timer.C:
			return fmt.Errorf("timeout waiting for leader")
		}
	}
}

// Shutdown cleanly shuts down the Raft instance, and closes its port and its
// store, which Raft leaves to their owner.
func (n *RaftNode) Shutdown() error {
	// A snapshot of what this node has applied, so that its next start
	// replays only what was committed after it stopped.
	if n.raft.AppliedIndex() > 0 {
		if err := n.raft.Snapshot().Error(); err != nil && err != raft.ErrNothingNewToSnapshot {
			log.Printf("[RAFT] Snapshot of the cluster metadata at shutdown failed: %v", err)
		}
	}
	err := n.raft.Shutdown().Error()
	if closeErr := n.transport.Close(); err == nil {
		err = closeErr
	}
	if store, ok := n.logStore.(io.Closer); ok {
		if closeErr := store.Close(); err == nil {
			err = closeErr
		}
	}
	return err
}

// Command is a Raft-replicated mutation applied to ClusterFSM.
type Command struct {
	// Type selects which FSM mutation to apply.
	Type CommandType `json:"type"`
	// Payload is the JSON-encoded command body (node or partition info).
	Payload json.RawMessage `json:"payload"`
}

// CommandType identifies a Raft FSM command kind.
type CommandType int

const (
	// CommandTypeAddNode adds a node to the FSM cluster state.
	CommandTypeAddNode CommandType = iota
	// CommandTypeRemoveNode removes a node from the FSM cluster state.
	CommandTypeRemoveNode
	// CommandTypeUpdateNode updates an existing node's metadata in the FSM.
	CommandTypeUpdateNode
	// CommandTypeAssignPartition creates or assigns partition metadata.
	CommandTypeAssignPartition
	// CommandTypeUpdatePartition updates partition leadership, ISR, or offsets.
	CommandTypeUpdatePartition
	// CommandTypeRequirePartition adds capabilities a partition requires of
	// the nodes that hold it. Builds before it answer the command with an
	// error and apply nothing, which is why it is committed only when every
	// voter advertises the capabilities (RequirePartitionCapabilities).
	CommandTypeRequirePartition
)

// ClusterFSM implements raft.FSM for durable cluster metadata (nodes and partitions).
type ClusterFSM struct {
	mu    sync.RWMutex
	state *ClusterState
	// onPartitionChange is called after a partition assignment is applied. It
	// runs on Raft's apply path and must not block.
	onPartitionChange func()
	// onNodeRemoved is called, on a goroutine of its own, after the removal of
	// a node is applied.
	onNodeRemoved func(nodeID string)
}

// SetOnNodeRemoved registers what is to happen on this node when a node is
// removed from the cluster.
func (f *ClusterFSM) SetOnNodeRemoved(fn func(nodeID string)) {
	f.mu.Lock()
	f.onNodeRemoved = fn
	f.mu.Unlock()
}

// SetOnPartitionChange registers a non-blocking callback for applied
// partition assignment changes.
func (f *ClusterFSM) SetOnPartitionChange(fn func()) {
	f.mu.Lock()
	f.onPartitionChange = fn
	f.mu.Unlock()
}

// Partition returns a copy of the committed assignment of one partition.
func (f *ClusterFSM) Partition(id int32) (PartitionInfo, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	info, ok := f.state.Partitions[id]
	if !ok || info == nil {
		return PartitionInfo{}, false
	}
	return info.clone(), true
}

// Partitions returns copies of every committed partition assignment.
func (f *ClusterFSM) Partitions() map[int32]PartitionInfo {
	f.mu.RLock()
	defer f.mu.RUnlock()
	out := make(map[int32]PartitionInfo, len(f.state.Partitions))
	for id, info := range f.state.Partitions {
		if info != nil {
			out[id] = info.clone()
		}
	}
	return out
}

// NewClusterFSM creates an empty ClusterFSM with initialized maps.
func NewClusterFSM() *ClusterFSM {
	return &ClusterFSM{
		state: &ClusterState{
			Nodes:      make(map[string]*Node),
			Partitions: make(map[int32]*PartitionInfo),
			UpdatedAt:  time.Now(),
		},
	}
}

// Apply decodes and applies a Raft log entry to the in-memory cluster state.
func (f *ClusterFSM) Apply(l *raft.Log) interface{} {
	var cmd Command
	if err := json.Unmarshal(l.Data, &cmd); err != nil {
		log.Printf("[FSM] Failed to unmarshal command: %v", err)
		return err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	switch cmd.Type {
	case CommandTypeAddNode:
		return f.applyAddNode(cmd.Payload)
	case CommandTypeRemoveNode:
		return f.applyRemoveNode(cmd.Payload)
	case CommandTypeUpdateNode:
		return f.applyUpdateNode(cmd.Payload)
	case CommandTypeAssignPartition:
		return f.applyAssignPartition(cmd.Payload)
	case CommandTypeUpdatePartition:
		return f.applyUpdatePartition(cmd.Payload)
	case CommandTypeRequirePartition:
		return f.applyRequirePartition(cmd.Payload)
	default:
		return fmt.Errorf("unknown command type: %d", cmd.Type)
	}
}

func (f *ClusterFSM) applyAddNode(payload json.RawMessage) interface{} {
	var node Node
	if err := json.Unmarshal(payload, &node); err != nil {
		return err
	}

	node.JoinedAt = time.Now()
	node.UpdatedAt = time.Now()
	node.State = NodeStateAlive

	f.state.Nodes[node.ID] = &node
	f.state.UpdatedAt = time.Now()

	log.Printf("[FSM] Added node %s", node.ID)
	return nil
}

func (f *ClusterFSM) applyRemoveNode(payload json.RawMessage) interface{} {
	var nodeID string
	if err := json.Unmarshal(payload, &nodeID); err != nil {
		return err
	}

	delete(f.state.Nodes, nodeID)
	f.state.UpdatedAt = time.Now()

	log.Printf("[FSM] Removed node %s", nodeID)
	if f.onNodeRemoved != nil {
		go f.onNodeRemoved(nodeID)
	}
	return nil
}

func (f *ClusterFSM) applyUpdateNode(payload json.RawMessage) interface{} {
	var node Node
	if err := json.Unmarshal(payload, &node); err != nil {
		return err
	}

	if existing, ok := f.state.Nodes[node.ID]; ok {
		existing.State = node.State
		existing.Role = node.Role
		existing.Meta = node.Meta
		existing.Partitions = node.Partitions
		existing.UpdatedAt = time.Now()
	}

	f.state.UpdatedAt = time.Now()
	return nil
}

func (f *ClusterFSM) applyAssignPartition(payload json.RawMessage) interface{} {
	var info PartitionInfo
	if err := json.Unmarshal(payload, &info); err != nil {
		return err
	}

	// An assignment creates the entry. Applying one for a partition that
	// already has an entry would reset its leader and epoch, so it is ignored;
	// later changes go through updates.
	if _, exists := f.state.Partitions[info.ID]; exists {
		return nil
	}
	f.state.Partitions[info.ID] = &info
	f.state.UpdatedAt = time.Now()

	log.Printf("[FSM] Assigned partition %d to leader %s", info.ID, info.LeaderID)
	if f.onPartitionChange != nil {
		f.onPartitionChange()
	}
	return nil
}

func (f *ClusterFSM) applyUpdatePartition(payload json.RawMessage) interface{} {
	var info PartitionInfo
	if err := json.Unmarshal(payload, &info); err != nil {
		return err
	}

	if existing, ok := f.state.Partitions[info.ID]; ok {
		existing.LeaderID = info.LeaderID
		existing.Replicas = info.Replicas
		existing.ISR = info.ISR
		existing.State = info.State
		existing.TransferTo = info.TransferTo
		existing.TransferStartedMs = info.TransferStartedMs
		existing.Retiring = info.Retiring
		// Every change raises the epoch. A proposal may ask for a higher one:
		// replicas restored from a backup have accepted epochs this cluster
		// never issued, and would refuse a leader below them.
		existing.Epoch = max(existing.Epoch+1, info.Epoch)
		// Preserve replica offsets across the update. Dropping them here left the
		// map nil, so failover election (electNewLeader) saw offset 0 for every
		// candidate and picked the alphabetically-first replica — an unclean
		// election that can lose acknowledged writes. Prefer the incoming offsets
		// when provided, otherwise keep the existing ones.
		if info.ReplicaOffsets != nil {
			existing.ReplicaOffsets = info.ReplicaOffsets
		}
		// Requirements only grow. An update proposed by a build that does not
		// know them carries none, and must not lift them.
		existing.Requires = unionCapabilities(existing.Requires, info.Requires)
	} else {
		f.state.Partitions[info.ID] = &info
	}

	f.state.UpdatedAt = time.Now()
	if f.onPartitionChange != nil {
		f.onPartitionChange()
	}
	return nil
}

// partitionRequirement is the payload of CommandTypeRequirePartition.
type partitionRequirement struct {
	PartitionID  int32    `json:"partition_id"`
	Capabilities []string `json:"capabilities"`
}

func (f *ClusterFSM) applyRequirePartition(payload json.RawMessage) interface{} {
	var req partitionRequirement
	if err := json.Unmarshal(payload, &req); err != nil {
		return err
	}
	existing, ok := f.state.Partitions[req.PartitionID]
	if !ok {
		return fmt.Errorf("partition %d has no assignment to add a requirement to", req.PartitionID)
	}
	existing.Requires = unionCapabilities(existing.Requires, req.Capabilities)
	f.state.UpdatedAt = time.Now()
	log.Printf("[FSM] Partition %d requires %v", req.PartitionID, existing.Requires)
	if f.onPartitionChange != nil {
		f.onPartitionChange()
	}
	return nil
}

// Snapshot returns a point-in-time snapshot of the FSM state for Raft persistence.
func (f *ClusterFSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	// Deep copy state
	data, err := json.Marshal(f.state)
	if err != nil {
		return nil, err
	}

	return &FSMSnapshot{data: data}, nil
}

// Restore replaces FSM state from a previously persisted Raft snapshot.
func (f *ClusterFSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()

	data, err := io.ReadAll(rc)
	if err != nil {
		return err
	}

	var state ClusterState
	if err := json.Unmarshal(data, &state); err != nil {
		return err
	}
	if state.Nodes == nil {
		state.Nodes = make(map[string]*Node)
	}
	if state.Partitions == nil {
		state.Partitions = make(map[int32]*PartitionInfo)
	}

	// A snapshot replaces the entries it covers, and with them what applying
	// them would have set off on this node: a running node that fell behind
	// and is sent one learns of new assignments and of nodes removed meanwhile
	// only here.
	f.mu.Lock()
	var removed []string
	for id := range f.state.Nodes {
		if _, kept := state.Nodes[id]; !kept {
			removed = append(removed, id)
		}
	}
	f.state = &state
	onPartitionChange, onNodeRemoved := f.onPartitionChange, f.onNodeRemoved
	nodes, partitions := len(state.Nodes), len(state.Partitions)
	f.mu.Unlock()

	log.Printf("[FSM] Restored from snapshot: %d nodes, %d partitions", nodes, partitions)
	if onNodeRemoved != nil {
		for _, id := range removed {
			go onNodeRemoved(id)
		}
	}
	if onPartitionChange != nil {
		onPartitionChange()
	}
	return nil
}

// GetState returns a deep copy of the current cluster state.
// FIX: Previously returned a live pointer to f.state, causing data races
// when callers read the returned state concurrently with FSM Apply mutations.
func (f *ClusterFSM) GetState() *ClusterState {
	f.mu.RLock()
	defer f.mu.RUnlock()

	// Deep copy via JSON to safely clone maps of pointers
	data, err := json.Marshal(f.state)
	if err != nil {
		log.Printf("[FSM] GetState marshal error: %v", err)
		return nil
	}
	var stateCopy ClusterState
	if err := json.Unmarshal(data, &stateCopy); err != nil {
		log.Printf("[FSM] GetState unmarshal error: %v", err)
		return nil
	}
	return &stateCopy
}

// FSMSnapshot is a serialized ClusterState used by Raft snapshotting.
type FSMSnapshot struct {
	data []byte
}

// Persist writes the snapshot bytes to sink.
func (s *FSMSnapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := sink.Write(s.data); err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

// Release is a no-op; FSMSnapshot holds no external resources.
func (s *FSMSnapshot) Release() {}

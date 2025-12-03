package cluster

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"go.etcd.io/bbolt"
)

// RaftNode manages Raft consensus for cluster metadata
type RaftNode struct {
	mu            sync.RWMutex
	config        *ClusterConfig
	raft          *raft.Raft
	fsm           *ClusterFSM
	transport     *raft.NetworkTransport
	logStore      raft.LogStore
	stableStore   raft.StableStore
	snapshotStore raft.SnapshotStore
}

// NewRaftNode creates a new Raft node
func NewRaftNode(config *ClusterConfig) (*RaftNode, error) {
	// Create data directory
	if err := os.MkdirAll(config.RaftDataDir, 0755); err != nil {
		return nil, fmt.Errorf("create raft data dir: %w", err)
	}

	// Create FSM
	fsm := NewClusterFSM()

	// Create transport
	addr, err := net.ResolveTCPAddr("tcp", config.RaftAddr)
	if err != nil {
		return nil, fmt.Errorf("resolve raft addr: %w", err)
	}

	transport, err := raft.NewTCPTransport(config.RaftAddr, addr, 3, 10*time.Second, os.Stderr)
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
	raftConfig.SnapshotInterval = 60 * time.Second
	raftConfig.SnapshotThreshold = 8192

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
	}, nil
}

// Bootstrap bootstraps the Raft cluster (only for first node)
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

// Join joins an existing cluster
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

// Leave removes a node from the cluster
func (n *RaftNode) Leave(nodeID string) error {
	log.Printf("[RAFT] Removing node %s from cluster", nodeID)

	future := n.raft.RemoveServer(raft.ServerID(nodeID), 0, 0)
	if err := future.Error(); err != nil {
		return fmt.Errorf("remove server: %w", err)
	}

	return nil
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

// Apply applies a command to the FSM
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

// GetState returns the current cluster state from FSM
func (n *RaftNode) GetState() *ClusterState {
	return n.fsm.GetState()
}

// WaitForLeader waits for a leader to be elected
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

// Shutdown shuts down the Raft node
func (n *RaftNode) Shutdown() error {
	future := n.raft.Shutdown()
	return future.Error()
}

// Command represents a Raft command
type Command struct {
	Type    CommandType     `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

// CommandType represents the type of Raft command
type CommandType int

const (
	CommandTypeAddNode CommandType = iota
	CommandTypeRemoveNode
	CommandTypeUpdateNode
	CommandTypeAssignPartition
	CommandTypeUpdatePartition
)

// ClusterFSM implements the Raft FSM for cluster metadata
type ClusterFSM struct {
	mu    sync.RWMutex
	state *ClusterState
}

// NewClusterFSM creates a new cluster FSM
func NewClusterFSM() *ClusterFSM {
	return &ClusterFSM{
		state: &ClusterState{
			Nodes:      make(map[string]*Node),
			Partitions: make(map[int32]*PartitionInfo),
			UpdatedAt:  time.Now(),
		},
	}
}

// Apply applies a Raft log entry to the FSM
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

	f.state.Partitions[info.ID] = &info
	f.state.UpdatedAt = time.Now()

	log.Printf("[FSM] Assigned partition %d to leader %s", info.ID, info.LeaderID)
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
		existing.Epoch++
	} else {
		f.state.Partitions[info.ID] = &info
	}

	f.state.UpdatedAt = time.Now()
	return nil
}

// Snapshot returns a snapshot of the FSM state
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

// Restore restores the FSM from a snapshot
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

	f.mu.Lock()
	f.state = &state
	f.mu.Unlock()

	log.Printf("[FSM] Restored from snapshot: %d nodes, %d partitions",
		len(f.state.Nodes), len(f.state.Partitions))

	return nil
}

// GetState returns the current cluster state
func (f *ClusterFSM) GetState() *ClusterState {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.state
}

// FSMSnapshot represents a snapshot of the FSM
type FSMSnapshot struct {
	data []byte
}

// Persist writes the snapshot to the given sink
func (s *FSMSnapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := sink.Write(s.data); err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

// Release releases the snapshot
func (s *FSMSnapshot) Release() {}

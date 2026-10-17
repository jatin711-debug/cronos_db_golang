package cluster

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// Manager is the main cluster coordinator: membership, partition routing, Raft
// metadata consensus, leadership reconciliation, and failover.
type Manager struct {
	mu                sync.RWMutex
	config            *ClusterConfig
	membership        MembershipService
	router            *Router
	raft              *RaftNode
	partitionAccessor PartitionAccessor
	// assignments is the committed partition metadata (Raft); positions reads
	// replica log positions. Either may be nil.
	assignments assignmentStore
	positions   ReplicaPositioner
	// reconcileCh wakes the background loop when an assignment is committed.
	reconcileCh chan struct{}
	// leadershipMu serializes the Raft leader's decisions about assignments.
	// They are triggered from a ticker and from membership callbacks, and each
	// decision must see the result of the one before it.
	leadershipMu sync.Mutex
	// membershipChangedAt is when a node last joined or left (Unix
	// nanoseconds), or 0 if membership has not been observed.
	membershipChangedAt atomic.Int64
	// transferRetryAt delays restarting an abandoned handoff; pulling marks
	// partitions this node is copying for a handoff. Both are guarded by mu.
	transferRetryAt map[int32]time.Time
	pulling         map[int32]bool
	// inSyncSeen is how many replicas were last seen in sync for each
	// partition this node leads. Only updateReplicaOffsets uses it.
	inSyncSeen map[int32]int

	started bool
	stopCh  chan struct{}
	ctx     context.Context
	cancel  context.CancelFunc

	// alive is the last reading of who is alive that seesEnoughReplicas took.
	alive atomic.Pointer[aliveView]
}

// NewManager creates a cluster Manager from the simplified startup Config.
// Membership, router, and Raft are initialized later in Start.
func NewManager(cfg *Config) *Manager {
	// Convert simple config to internal config
	config := &ClusterConfig{
		ClusterID:         "cronos-cluster",
		NodeID:            cfg.NodeID,
		BindAddr:          cfg.GossipAddr,
		AdvertiseAddr:     cfg.GossipAddr,
		GRPCAddr:          cfg.GRPCAddr,
		ClientAddr:        cfg.ClientAddr,
		RaftAddr:          cfg.RaftAddr,
		RaftDataDir:       cfg.RaftDir,
		SeedNodes:         cfg.SeedNodes,
		HeartbeatInterval: cfg.HeartbeatInterval,
		ElectionTimeout:   cfg.HeartbeatInterval * 5,
		FailureTimeout:    cfg.FailureTimeout,
		SuspectTimeout:    cfg.SuspectTimeout,
		ReplicationFactor: cfg.ReplicationFactor,
		MinInSyncReplicas: cfg.MinInSyncReplicas,
		ExpectedNodes:     cfg.ExpectedNodes,
		FormationWait:     cfg.FormationWait,
		NumPartitions:     cfg.PartitionCount,
		VirtualNodes:      cfg.VirtualNodes,
		ServerTLS:         cfg.ServerTLS,
		ClientTLS:         cfg.ClientTLS,
		ClientTLSNow:      cfg.ClientTLSNow,
		Bootstrap:         cfg.Bootstrap,
		Rack:              cfg.Rack,
		Zone:              cfg.Zone,
		Region:            cfg.Region,
	}

	if config.HeartbeatInterval == 0 {
		config.HeartbeatInterval = 1 * time.Second
	}
	if config.ElectionTimeout == 0 {
		config.ElectionTimeout = 5 * time.Second
	}
	if config.FailureTimeout == 0 {
		config.FailureTimeout = 5 * time.Second
	}
	if config.SuspectTimeout == 0 {
		config.SuspectTimeout = 3 * time.Second
	}
	if config.NumPartitions == 0 {
		config.NumPartitions = 16
	}
	if config.ReplicationFactor == 0 {
		config.ReplicationFactor = 1
	}
	if config.VirtualNodes == 0 {
		config.VirtualNodes = 150
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &Manager{
		config:      config,
		stopCh:      make(chan struct{}),
		reconcileCh: make(chan struct{}, 1),
		ctx:         ctx,
		cancel:      cancel,
	}
}

// Start initializes Raft (optional), membership, and the partition router, then
// begins background leader tasks. It is not safe to call more than once.
func (m *Manager) Start() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.started {
		return fmt.Errorf("already started")
	}

	log.Printf("[CLUSTER] Starting cluster manager for node %s", m.config.NodeID)
	if err := checkTransportTLS(m.config); err != nil {
		return err
	}

	// Initialize Raft FIRST (if enabled) - needed before membership starts
	if m.config.RaftAddr != "" {
		raft, err := NewRaftNode(m.config)
		if err != nil {
			return fmt.Errorf("initialize required Raft authority: %w", err)
		} else {
			m.raft = raft
			m.assignments = raft
			// React to committed assignments as they are applied instead of
			// waiting for the next tick: a deposed leader steps down and a newly
			// named one takes over within moments of the commit.
			raft.SetOnPartitionChange(m.kickReconcile)
			// A node that is removed is forgotten by every node, not only by
			// the one that was asked to remove it.
			raft.SetOnNodeRemoved(m.forgetNode)

			// Create the cluster only if there is none; see bootstrap.go.
			create, err := m.createsCluster(raft.HasState())
			if err != nil {
				_ = m.raft.Shutdown()
				m.raft = nil
				return err
			}
			if create {
				log.Printf("[CLUSTER] Bootstrapping new Raft cluster")
				if err := m.raft.Bootstrap(); err != nil {
					_ = m.raft.Shutdown()
					m.raft = nil
					return fmt.Errorf("bootstrap Raft: %w", err)
				}

				// Wait for leader election
				if err := m.raft.WaitForLeader(30 * time.Second); err != nil {
					_ = m.raft.Shutdown()
					m.raft = nil
					return fmt.Errorf("wait for Raft authority: %w", err)
				}
			} else if raft.HasState() {
				log.Printf("[CLUSTER] Continuing in the cluster this node has on disk")
			} else {
				log.Printf("[CLUSTER] Will join existing Raft cluster via membership")
			}
		}
	}

	// Create membership (custom TCP or HashiCorp Memberlist)
	var membership MembershipService
	var err error
	if m.config.UseMemberlist {
		membership, err = NewMemberlistMembership(m.config)
		if err != nil {
			return fmt.Errorf("create memberlist membership: %w", err)
		}
		log.Printf("[CLUSTER] Using HashiCorp Memberlist (SWIM) for cluster membership")
	} else {
		membership, err = NewMembership(m.config)
		if err != nil {
			return fmt.Errorf("create membership: %w", err)
		}
		log.Printf("[CLUSTER] Using custom TCP gossip for cluster membership")
	}
	m.membership = membership
	if asked, ok := membership.(interface{ SetClusterFormed(func() bool) }); ok && m.raft != nil {
		// What this node answers when another asks whether a cluster exists.
		asked.SetClusterFormed(m.raft.HasState)
	}

	// Set up membership callbacks to handle Raft cluster changes
	m.noteMembershipChange()
	m.membership.OnJoin(func(node *Node) {
		m.noteMembershipChange()
		// When a new node joins via gossip, add it to Raft if we're the leader
		if m.raft != nil && m.raft.IsLeader() && node.RaftAddr != "" {
			log.Printf("[CLUSTER] Adding node %s to Raft cluster at %s", node.ID, node.RaftAddr)
			// Retry with exponential backoff — the target node may still be booting
			var lastErr error
			for attempt := 1; attempt <= 3; attempt++ {
				if err := m.raft.Join(node.ID, node.RaftAddr); err != nil {
					lastErr = err
					backoff := time.Duration(attempt) * time.Second
					log.Printf("[CLUSTER] Raft join attempt %d/3 failed for %s: %v (retrying in %v)", attempt, node.ID, err, backoff)
					time.Sleep(backoff)
					continue
				}
				log.Printf("[CLUSTER] Successfully added node %s to Raft cluster", node.ID)
				return
			}
			log.Printf("[CLUSTER] Warning: Failed to add node %s to Raft after 3 attempts: %v", node.ID, lastErr)
		}
	})

	// A node leaves the list of members when it is removed (RemoveNode), which
	// has taken it out of Raft by then.
	m.membership.OnLeave(func(node *Node) {
		m.noteMembershipChange()
	})

	// Start membership (this starts the gossip listener)
	if err := m.membership.Start(m.ctx); err != nil {
		return fmt.Errorf("start membership: %w", err)
	}

	// Create router
	m.router = NewRouter(m.membership, m.config.NumPartitions, m.config.ReplicationFactor, m.config.VirtualNodes, m.partitionAccessor)
	// Routing and metadata report the committed leader, not the ring's wish.
	m.router.SetCommittedSource(m.committed)
	// When membership changes rebalance partition assignments, immediately sync
	// the new leader/replica mapping into Raft so metadata APIs don't return
	// stale assignments (e.g. a node showing 0 leader partitions).
	m.router.SetOnRebalance(func() {
		if m.IsLeader() {
			m.syncClusterState()
		}
		m.kickReconcile()
	})
	m.router.Start()

	// Start background tasks
	go m.leaderTasks()

	m.started = true
	log.Printf("[CLUSTER] Cluster manager started")

	return nil
}

// Stop shuts down Raft, membership, and background tasks. Safe if already stopped.
func (m *Manager) Stop() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.started {
		return nil
	}

	// Cancel context
	m.cancel()
	close(m.stopCh)

	// Stop Raft
	if m.raft != nil {
		if err := m.raft.Shutdown(); err != nil {
			log.Printf("[CLUSTER] Error shutting down Raft: %v", err)
		}
	}

	// Stop membership
	if m.membership != nil {
		m.membership.Stop()
	}

	m.started = false
	log.Printf("[CLUSTER] Cluster manager stopped")

	return nil
}

// GetPartitionNode returns the node that owns the given partition
func (m *Manager) GetPartitionNode(partitionID int32) *Node {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.router == nil {
		return nil
	}

	info, err := m.router.GetPartitionInfo(partitionID)
	if err != nil || info == nil {
		return nil
	}

	// Find node with matching ID
	for _, node := range m.membership.GetNodes() {
		if node.ID == info.LeaderID {
			return node
		}
	}

	return nil
}

// leaderTasks runs the background reconciliation. Every node follows the
// committed assignment; the Raft leader additionally maintains it.
func (m *Manager) leaderTasks() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	// Handoffs refuse publishes while they run, so they are driven on a
	// shorter tick than the rest.
	fast := time.NewTicker(500 * time.Millisecond)
	defer fast.Stop()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-m.stopCh:
			return
		case <-m.reconcileCh:
			m.reconcileLocalLeadership()
		case <-fast.C:
			if m.IsLeader() {
				m.syncClusterState()
				m.advanceTransfers()
			}
		case <-ticker.C:
			// Every node reconciles the partitions it leads so streaming
			// replication is established on initial cluster formation, not only
			// after a failover.
			m.reconcileLocalLeadership()
			m.updateReplicaOffsets()
			if m.IsLeader() {
				m.performLeaderTasks()
			}
		}
	}
}

// performLeaderTasks performs leader-only tasks
func (m *Manager) performLeaderTasks() {
	// Elect a leader for partitions whose leader is dead
	m.checkPartitionHealth()

	// Update cluster state in Raft
	m.syncClusterState()
}

// updateReplicaOffsets puts the offsets and the in-sync replicas of the
// partitions this node leads into its router, and logs when the number of
// replicas in sync with one of them changes while it is below the
// replication factor, or reaches it again. Every node does this for the
// partitions it leads: which followers keep up is known to the leader alone,
// and the cluster state does not record it.
func (m *Manager) updateReplicaOffsets() {
	if m.partitionAccessor == nil || m.router == nil {
		return
	}
	if m.inSyncSeen == nil {
		m.inSyncSeen = make(map[int32]int)
	}

	partitions := m.router.GetAllPartitions()
	for partitionID, info := range partitions {
		if info.LeaderID != m.config.NodeID {
			delete(m.inSyncSeen, partitionID)
			continue
		}
		offsets := m.partitionAccessor.GetPartitionReplicaOffsets(partitionID)
		if len(offsets) > 0 {
			m.router.UpdateReplicaOffsets(partitionID, offsets)
		}
		isr := m.partitionAccessor.GetPartitionInSyncReplicas(partitionID)
		if len(isr) > 0 {
			m.router.UpdatePartitionISR(partitionID, isr)
		}
		m.noteInSync(partitionID, max(len(isr), 1))
	}
}

// noteInSync logs a change in the number of replicas in sync with a
// partition this node leads, when the number is or was below the replication
// factor. The leader itself counts.
func (m *Manager) noteInSync(partitionID int32, inSync int) {
	want := m.config.ReplicationFactor
	before, seen := m.inSyncSeen[partitionID]
	m.inSyncSeen[partitionID] = inSync
	switch {
	case seen && before == inSync:
	case inSync < want:
		log.Printf("[CLUSTER] Partition %d under-replicated: %d of %d replicas in sync", partitionID, inSync, want)
	case seen && before < want:
		log.Printf("[CLUSTER] Partition %d has all %d replicas in sync again", partitionID, inSync)
	}
}

// partitionAssignmentChanged returns true if the leader or replica set has
// changed between the FSM and the router assignment.
func partitionAssignmentChanged(fsm, router *PartitionInfo) bool {
	if fsm == nil || router == nil {
		return fsm != router
	}
	if fsm.LeaderID != router.LeaderID {
		return true
	}
	if fsm.State != router.State {
		return true
	}
	if !stringSliceSetEqual(fsm.Replicas, router.Replicas) {
		return true
	}
	if !stringSliceSetEqual(fsm.ISR, router.ISR) {
		return true
	}
	return false
}

func stringSliceSetEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	if len(a) == 0 {
		return true
	}
	seen := make(map[string]int, len(a))
	for _, s := range a {
		seen[s]++
	}
	for _, s := range b {
		if seen[s] == 0 {
			return false
		}
		seen[s]--
	}
	return true
}

// SetPartitionAccessor sets the partition accessor for state transfer operations
func (m *Manager) SetPartitionAccessor(accessor PartitionAccessor) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.partitionAccessor = accessor
	if positioner, ok := accessor.(ReplicaPositioner); ok {
		m.positions = positioner
	}
	// If router already exists, update it
	if m.router != nil {
		// Router is created in Start(), so this should be called before Start()
	}
}

// JoinCluster joins an existing cluster via the given leader address
// It contacts the leader to be added to the Raft cluster and initiates
// state transfer to sync partition data
func (m *Manager) JoinCluster(leaderAddr string) error {
	m.mu.RLock()
	if m.raft == nil {
		m.mu.RUnlock()
		return fmt.Errorf("raft not initialized")
	}
	if m.partitionAccessor == nil {
		m.mu.RUnlock()
		return fmt.Errorf("partition accessor not set - call SetPartitionAccessor first")
	}
	if m.router == nil || m.membership == nil {
		m.mu.RUnlock()
		return fmt.Errorf("cluster services not initialized")
	}
	partitionAccessor := m.partitionAccessor
	router := m.router
	membership := m.membership
	localNodeID := m.config.NodeID
	m.mu.RUnlock()

	log.Printf("[CLUSTER] Requesting to join cluster via %s", leaderAddr)

	// In a real implementation, this would use gRPC to contact the leader
	// and request to be added to the cluster. The leader would then:
	// 1. Add this node to Raft
	// 2. Initiate partition reassignment
	// 3. Start state transfer

	// Wait briefly for membership discovery so router assignments are not based
	// on a single-node view (which would incorrectly claim all partitions local).
	waitDeadline := time.Now().Add(3 * time.Second)
	for {
		if len(membership.GetAliveNodes()) > 1 || time.Now().After(waitDeadline) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	localPartitions := router.GetLocalPartitions()
	if len(localPartitions) == 0 {
		log.Printf("[CLUSTER] Join cluster complete, no local partitions assigned yet")
		return nil
	}

	synced := 0
	for _, partitionID := range localPartitions {
		leader, err := router.GetPartitionLeader(partitionID)
		if err != nil {
			log.Printf("[CLUSTER] Cannot resolve leader for partition %d: %v", partitionID, err)
			continue
		}
		if leader == nil || leader.Address == "" {
			log.Printf("[CLUSTER] No leader address for partition %d", partitionID)
			continue
		}
		if leader.ID == localNodeID {
			continue // Nothing to sync from remote
		}

		log.Printf("[CLUSTER] Syncing partition %d from leader %s", partitionID, leader.Address)
		if err := partitionAccessor.GetOrCreatePartition(partitionID); err != nil {
			log.Printf("[CLUSTER] Failed to create partition %d: %v", partitionID, err)
			continue
		}
		if err := partitionAccessor.SyncPartitionFromLeader(partitionID, leader.Address); err != nil {
			log.Printf("[CLUSTER] Failed to sync partition %d: %v", partitionID, err)
			continue
		}
		synced++
	}

	log.Printf("[CLUSTER] Join cluster complete, synced %d/%d local partitions", synced, len(localPartitions))
	return nil
}

// GetMembership returns the membership manager
func (m *Manager) GetMembership() MembershipService {
	return m.membership
}

// GetRouter returns the partition router
func (m *Manager) GetRouter() *Router {
	return m.router
}

// GetRaft returns the Raft node
func (m *Manager) GetRaft() *RaftNode {
	return m.raft
}

// IsLeader returns true if this node is the cluster leader
func (m *Manager) IsLeader() bool {
	if m.raft != nil {
		return m.raft.IsLeader()
	}
	// Single node mode - always leader
	return true
}

// GetLeader returns the current leader
func (m *Manager) GetLeader() *Node {
	if m.raft != nil {
		leaderAddr := m.raft.GetLeader()
		// Find node with this Raft address
		for _, node := range m.membership.GetNodes() {
			if node.RaftAddr == leaderAddr {
				return node
			}
		}
	}
	return m.membership.GetLocalNode()
}

// AddNode adds a node to the cluster
func (m *Manager) AddNode(node *Node) error {
	// Add to membership
	if err := m.membership.Join(node); err != nil {
		return err
	}

	// If Raft enabled and we're leader, add to Raft cluster
	if m.raft != nil && m.raft.IsLeader() {
		// Add node to Raft
		if err := m.raft.Join(node.ID, node.RaftAddr); err != nil {
			return fmt.Errorf("add to raft: %w", err)
		}

		// Add node to FSM
		payload, _ := json.Marshal(node)
		cmd := &Command{
			Type:    CommandTypeAddNode,
			Payload: payload,
		}
		if err := m.raft.Apply(cmd); err != nil {
			return fmt.Errorf("apply add node: %w", err)
		}
	}

	return nil
}

// ErrNotCoordinator is what RemoveNode answers on a node that does not
// coordinate the cluster: the change has to be made by the one that does.
var ErrNotCoordinator = errors.New("this node does not coordinate the cluster")

// RemoveNode takes a node that is gone for good out of the cluster.
//
// A node that dies stays a member until it is removed. That is what lets it
// come back: its replicas are kept track of and it is taken back when it is
// heard from. It also keeps counting towards what a majority is in Raft. A
// cluster of three with one node dead still has two of three; give it a
// fourth node to replace the dead one and it needs three of four, which is
// every node that runs, so that one more failure stops it. Removing the dead
// node makes it three again.
//
// Only a node that is not running can be removed: one that runs is heard
// from and joins again. Asked of a node that does not coordinate the cluster
// it answers ErrNotCoordinator; see RemoveNodeAnywhere.
func (m *Manager) RemoveNode(nodeID string) error {
	m.mu.RLock()
	raftNode, membership := m.raft, m.membership
	m.mu.RUnlock()
	if raftNode == nil || membership == nil {
		return fmt.Errorf("this node is not part of a cluster")
	}
	if nodeID == "" {
		return fmt.Errorf("a node ID is required")
	}
	if nodeID == m.config.NodeID {
		return fmt.Errorf("node %s was asked to remove itself; stop it and ask another node", nodeID)
	}
	if !raftNode.IsLeader() {
		return ErrNotCoordinator
	}

	voters, err := raftNode.Voters()
	if err != nil {
		return err
	}
	remaining := len(voters)
	isVoter := false
	for _, voter := range voters {
		if voter.ID == nodeID {
			isVoter = true
			remaining--
		}
	}
	node, _ := membership.GetNode(nodeID)
	if node == nil && !isVoter {
		return fmt.Errorf("node %s is not a member of the cluster", nodeID)
	}
	if node != nil && node.State == NodeStateAlive {
		return fmt.Errorf("node %s is running; stop it first, since a node that runs joins again", nodeID)
	}

	if isVoter {
		if err := raftNode.Leave(nodeID); err != nil {
			return fmt.Errorf("take node %s out of Raft: %w", nodeID, err)
		}
	}
	// Through the log, so that every node hears of it and forgets the node.
	payload, _ := json.Marshal(nodeID)
	if err := raftNode.Apply(&Command{Type: CommandTypeRemoveNode, Payload: payload}); err != nil {
		return fmt.Errorf("record the removal of node %s: %w", nodeID, err)
	}
	log.Printf("[CLUSTER] Node %s was removed from the cluster; %d nodes count towards a majority now", nodeID, remaining)
	return nil
}

// forgetNode drops a removed node from this node's list of members. It runs
// on every node when the removal is applied, which is also when a node reads
// its log again after a restart: a node that has come back since, and is
// heard from, is left alone.
func (m *Manager) forgetNode(nodeID string) {
	m.mu.RLock()
	membership := m.membership
	m.mu.RUnlock()
	if membership == nil || nodeID == m.config.NodeID {
		return
	}
	node, err := membership.GetNode(nodeID)
	if err != nil || node == nil || node.State == NodeStateAlive {
		return
	}
	if err := membership.Leave(nodeID); err == nil {
		log.Printf("[CLUSTER] Node %s was removed from the cluster and is forgotten here", nodeID)
	}
}

// RemoveNodeAnywhere removes a node whichever node is asked: the node that
// coordinates the cluster does it, and any other passes the request on to
// that one. forwarded says that the request was passed on already, and is
// not passed on a second time. ask is how to ask the node at an address.
func (m *Manager) RemoveNodeAnywhere(ctx context.Context, nodeID string, forwarded bool, ask func(ctx context.Context, addr string, clientTLS *tls.Config) error) error {
	err := m.RemoveNode(nodeID)
	if !errors.Is(err, ErrNotCoordinator) {
		return err
	}
	if forwarded {
		return fmt.Errorf("the node that was thought to coordinate the cluster does not; try again")
	}
	coordinator := m.GetLeader()
	if coordinator == nil || coordinator.ID == m.config.NodeID || coordinator.Address == "" {
		return fmt.Errorf("the cluster has no coordinator at the moment; try again")
	}
	if err := ask(ctx, coordinator.Address, m.config.dialTLS()); err != nil {
		return fmt.Errorf("node %s coordinates the cluster and was asked: %w", coordinator.ID, err)
	}
	return nil
}

// GetClusterState returns the current cluster state
func (m *Manager) GetClusterState() *ClusterState {
	if m.raft != nil {
		return m.raft.GetState()
	}
	return m.membership.GetClusterState()
}

// GetPartitionInfo returns partition information
func (m *Manager) GetPartitionInfo(partitionID int32) (*PartitionInfo, error) {
	if m.router == nil {
		return nil, fmt.Errorf("router not initialized")
	}
	return m.router.GetPartitionInfo(partitionID)
}

// GetAllPartitionInfo returns metadata for all partitions in the cluster view.
func (m *Manager) GetAllPartitionInfo() map[int32]*PartitionInfo {
	if m.router == nil {
		return map[int32]*PartitionInfo{}
	}
	return m.router.GetAllPartitions()
}

// RouteRequest routes a request to the correct partition
func (m *Manager) RouteRequest(topic string) (*RouteInfo, error) {
	return m.router.RouteRequest(topic)
}

// IsLocalPartition returns true if this node owns the partition
func (m *Manager) IsLocalPartition(partitionID int32) bool {
	return m.router.IsLocalPartition(partitionID)
}

// IsPartitionLeader returns true if this node is the partition leader
func (m *Manager) IsPartitionLeader(partitionID int32) bool {
	return m.router.IsPartitionLeader(partitionID)
}

// GetPartitionEpoch returns the cluster epoch for a partition.
func (m *Manager) GetPartitionEpoch(partitionID int32) int64 {
	return m.router.GetPartitionEpoch(partitionID)
}

// GetLocalPartitions returns partitions owned by this node
func (m *Manager) GetLocalPartitions() []int32 {
	return m.router.GetLocalPartitions()
}

// GetStats returns cluster statistics
func (m *Manager) GetStats() *ClusterStats {
	nodes := m.membership.GetNodes()
	aliveCount := 0
	for _, node := range nodes {
		if node.State == NodeStateAlive {
			aliveCount++
		}
	}

	return &ClusterStats{
		NodeID:           m.config.NodeID,
		ClusterID:        m.config.ClusterID,
		IsLeader:         m.IsLeader(),
		TotalNodes:       len(nodes),
		AliveNodes:       aliveCount,
		NumPartitions:    m.config.NumPartitions,
		LocalPartitions:  len(m.router.GetLocalPartitions()),
		LeaderPartitions: len(m.router.GetLeaderPartitions()),
	}
}

// ClusterStats is a summary of this node's view of cluster health and ownership.
type ClusterStats struct {
	// NodeID is this node's identifier.
	NodeID string `json:"node_id"`
	// ClusterID is the logical cluster identifier.
	ClusterID string `json:"cluster_id"`
	// IsLeader is true when this node is the Raft/cluster metadata leader.
	IsLeader bool `json:"is_leader"`
	// TotalNodes is the number of known nodes in the membership view.
	TotalNodes int `json:"total_nodes"`
	// AliveNodes is the number of nodes currently considered alive.
	AliveNodes int `json:"alive_nodes"`
	// NumPartitions is the configured partition count.
	NumPartitions int `json:"num_partitions"`
	// LocalPartitions is how many partitions this node hosts as a replica.
	LocalPartitions int `json:"local_partitions"`
	// LeaderPartitions is how many partitions this node currently leads.
	LeaderPartitions int `json:"leader_partitions"`
}

// AssignPartition proposes a new partition assignment to the Raft cluster.
func (m *Manager) AssignPartition(info *PartitionInfo) error {
	m.mu.RLock()
	store := m.assignments
	m.mu.RUnlock()
	if store == nil {
		return fmt.Errorf("raft node not initialized")
	}
	return store.ProposePartition(info, true)
}

// UpdatePartition proposes a partition metadata update to the Raft cluster.
func (m *Manager) UpdatePartition(info *PartitionInfo) error {
	m.mu.RLock()
	store := m.assignments
	m.mu.RUnlock()
	if store == nil {
		return fmt.Errorf("raft node not initialized")
	}
	return store.ProposePartition(info, false)
}

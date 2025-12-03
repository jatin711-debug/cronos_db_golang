package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"
)

// Manager is the main cluster manager that coordinates all distributed components
type Manager struct {
	mu         sync.RWMutex
	config     *ClusterConfig
	membership *Membership
	router     *Router
	raft       *RaftNode

	started bool
	stopCh  chan struct{}
	ctx     context.Context
	cancel  context.CancelFunc
}

// NewManager creates a new cluster manager from simplified config
func NewManager(cfg *Config) *Manager {
	// Convert simple config to internal config
	config := &ClusterConfig{
		ClusterID:         "cronos-cluster",
		NodeID:            cfg.NodeID,
		BindAddr:          cfg.GossipAddr,
		AdvertiseAddr:     cfg.GossipAddr,
		GRPCAddr:          cfg.GRPCAddr,
		RaftAddr:          cfg.RaftAddr,
		RaftDataDir:       cfg.RaftDir,
		SeedNodes:         cfg.SeedNodes,
		HeartbeatInterval: cfg.HeartbeatInterval,
		ElectionTimeout:   cfg.HeartbeatInterval * 5, // Election timeout should be > heartbeat
		FailureTimeout:    cfg.FailureTimeout,
		SuspectTimeout:    cfg.SuspectTimeout,
		ReplicationFactor: cfg.ReplicationFactor,
		NumPartitions:     cfg.PartitionCount,
		VirtualNodes:      cfg.VirtualNodes,
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
		config: config,
		stopCh: make(chan struct{}),
		ctx:    ctx,
		cancel: cancel,
	}
}

// Start starts the cluster manager
func (m *Manager) Start() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.started {
		return fmt.Errorf("already started")
	}

	log.Printf("[CLUSTER] Starting cluster manager for node %s", m.config.NodeID)

	// Initialize Raft FIRST (if enabled) - needed before membership starts
	if m.config.RaftAddr != "" {
		raft, err := NewRaftNode(m.config)
		if err != nil {
			log.Printf("[CLUSTER] Warning: Failed to create Raft node: %v (continuing without Raft)", err)
		} else {
			m.raft = raft

			// Bootstrap only if we're the first node (no seeds)
			if len(m.config.SeedNodes) == 0 {
				log.Printf("[CLUSTER] Bootstrapping new Raft cluster")
				if err := m.raft.Bootstrap(); err != nil {
					log.Printf("[CLUSTER] Warning: Failed to bootstrap Raft: %v", err)
				}

				// Wait for leader election
				if err := m.raft.WaitForLeader(30 * time.Second); err != nil {
					log.Printf("[CLUSTER] Warning: no leader elected yet: %v", err)
				}
			} else {
				log.Printf("[CLUSTER] Will join existing Raft cluster via membership")
			}
		}
	}

	// Create membership
	membership, err := NewMembership(m.config)
	if err != nil {
		return fmt.Errorf("create membership: %w", err)
	}
	m.membership = membership

	// Set up membership callbacks to handle Raft cluster changes
	m.membership.OnJoin(func(node *Node) {
		// When a new node joins via gossip, add it to Raft if we're the leader
		if m.raft != nil && m.raft.IsLeader() && node.RaftAddr != "" {
			log.Printf("[CLUSTER] Adding node %s to Raft cluster at %s", node.ID, node.RaftAddr)
			if err := m.raft.Join(node.ID, node.RaftAddr); err != nil {
				log.Printf("[CLUSTER] Warning: Failed to add node %s to Raft: %v", node.ID, err)
			}
		}
	})

	m.membership.OnLeave(func(node *Node) {
		// When a node leaves, remove it from Raft if we're the leader
		if m.raft != nil && m.raft.IsLeader() {
			log.Printf("[CLUSTER] Removing node %s from Raft cluster", node.ID)
			if err := m.raft.Leave(node.ID); err != nil {
				log.Printf("[CLUSTER] Warning: Failed to remove node %s from Raft: %v", node.ID, err)
			}
		}
	})

	// Start membership (this starts the gossip listener)
	if err := m.membership.Start(m.ctx); err != nil {
		return fmt.Errorf("start membership: %w", err)
	}

	// Create router
	m.router = NewRouter(m.membership, m.config.NumPartitions, m.config.ReplicationFactor)

	// Start background tasks
	go m.leaderTasks()

	m.started = true
	log.Printf("[CLUSTER] Cluster manager started")

	return nil
}

// Stop stops the cluster manager
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

// leaderTasks runs periodic tasks that only the leader should perform
func (m *Manager) leaderTasks() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-m.stopCh:
			return
		case <-ticker.C:
			if m.IsLeader() {
				m.performLeaderTasks()
			}
		}
	}
}

// performLeaderTasks performs leader-only tasks
func (m *Manager) performLeaderTasks() {
	// Check for under-replicated partitions
	m.checkPartitionHealth()

	// Update cluster state in Raft
	m.syncClusterState()
}

// checkPartitionHealth checks for unhealthy partitions
func (m *Manager) checkPartitionHealth() {
	partitions := m.router.GetAllPartitions()
	aliveNodes := m.membership.GetAliveNodes()
	aliveNodeIDs := make(map[string]bool)
	for _, node := range aliveNodes {
		aliveNodeIDs[node.ID] = true
	}

	for partitionID, info := range partitions {
		// Check if leader is alive
		if !aliveNodeIDs[info.LeaderID] {
			log.Printf("[CLUSTER] Partition %d leader %s is dead, triggering leader election",
				partitionID, info.LeaderID)
			m.electNewLeader(partitionID, info)
		}

		// Check ISR size
		isrCount := 0
		for _, nodeID := range info.ISR {
			if aliveNodeIDs[nodeID] {
				isrCount++
			}
		}

		if isrCount < m.config.ReplicationFactor {
			log.Printf("[CLUSTER] Partition %d under-replicated (ISR=%d, RF=%d)",
				partitionID, isrCount, m.config.ReplicationFactor)
		}
	}
}

// electNewLeader elects a new leader for a partition
func (m *Manager) electNewLeader(partitionID int32, info *PartitionInfo) {
	aliveNodes := m.membership.GetAliveNodes()
	aliveNodeIDs := make(map[string]bool)
	for _, node := range aliveNodes {
		aliveNodeIDs[node.ID] = true
	}

	// Find first alive replica to be new leader
	var newLeader string
	for _, nodeID := range info.Replicas {
		if aliveNodeIDs[nodeID] && nodeID != info.LeaderID {
			newLeader = nodeID
			break
		}
	}

	if newLeader == "" {
		log.Printf("[CLUSTER] No available replica for partition %d", partitionID)
		return
	}

	// Update partition via Raft
	newInfo := &PartitionInfo{
		ID:       partitionID,
		Topic:    info.Topic,
		LeaderID: newLeader,
		Replicas: info.Replicas,
		ISR:      info.ISR,
		Epoch:    info.Epoch + 1,
		State:    PartitionStateOnline,
	}

	if m.raft != nil {
		payload, _ := json.Marshal(newInfo)
		cmd := &Command{
			Type:    CommandTypeUpdatePartition,
			Payload: payload,
		}
		if err := m.raft.Apply(cmd); err != nil {
			log.Printf("[CLUSTER] Failed to update partition leader: %v", err)
			return
		}
	}

	log.Printf("[CLUSTER] Partition %d new leader: %s (epoch=%d)",
		partitionID, newLeader, newInfo.Epoch)
}

// syncClusterState syncs cluster state to Raft
func (m *Manager) syncClusterState() {
	// This would sync any local state changes to Raft
	// For now, just log
}

// JoinCluster joins an existing cluster
func (m *Manager) JoinCluster(leaderAddr string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.raft == nil {
		return fmt.Errorf("raft not initialized")
	}

	// Request leader to add us
	// In production, this would be a gRPC call to the leader
	log.Printf("[CLUSTER] Requesting to join cluster via %s", leaderAddr)

	return nil
}

// GetMembership returns the membership manager
func (m *Manager) GetMembership() *Membership {
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

// RemoveNode removes a node from the cluster
func (m *Manager) RemoveNode(nodeID string) error {
	// Remove from membership
	if err := m.membership.Leave(nodeID); err != nil {
		return err
	}

	// If Raft enabled and we're leader, remove from Raft cluster
	if m.raft != nil && m.raft.IsLeader() {
		if err := m.raft.Leave(nodeID); err != nil {
			return fmt.Errorf("remove from raft: %w", err)
		}

		payload, _ := json.Marshal(nodeID)
		cmd := &Command{
			Type:    CommandTypeRemoveNode,
			Payload: payload,
		}
		if err := m.raft.Apply(cmd); err != nil {
			return fmt.Errorf("apply remove node: %w", err)
		}
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
	return m.router.GetPartitionInfo(partitionID)
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

// ClusterStats represents cluster statistics
type ClusterStats struct {
	NodeID           string `json:"node_id"`
	ClusterID        string `json:"cluster_id"`
	IsLeader         bool   `json:"is_leader"`
	TotalNodes       int    `json:"total_nodes"`
	AliveNodes       int    `json:"alive_nodes"`
	NumPartitions    int    `json:"num_partitions"`
	LocalPartitions  int    `json:"local_partitions"`
	LeaderPartitions int    `json:"leader_partitions"`
}

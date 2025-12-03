package cluster

import (
	"fmt"
	"log"
	"sync"
)

// Router handles routing requests to the correct node/partition
type Router struct {
	mu            sync.RWMutex
	hashRing      *HashRing
	membership    *Membership
	numPartitions int
	localNodeID   string

	// Partition assignments cache
	assignments map[int32]*PartitionInfo
}

// NewRouter creates a new partition router
func NewRouter(membership *Membership, numPartitions, replicationFactor int) *Router {
	r := &Router{
		hashRing:      NewHashRing(150, replicationFactor),
		membership:    membership,
		numPartitions: numPartitions,
		localNodeID:   membership.GetLocalNode().ID,
		assignments:   make(map[int32]*PartitionInfo),
	}

	// Initialize partition assignments
	r.initializePartitions()

	// Subscribe to membership events
	go r.watchMembershipEvents()

	return r
}

// initializePartitions initializes partition assignments
func (r *Router) initializePartitions() {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Add existing nodes to hash ring
	for _, node := range r.membership.GetAliveNodes() {
		r.hashRing.AddNode(node.ID)
	}

	// Compute partition assignments
	for i := 0; i < r.numPartitions; i++ {
		partitionID := int32(i)
		nodes := r.hashRing.GetPartitionNodes(partitionID)

		leaderID := ""
		if len(nodes) > 0 {
			leaderID = nodes[0]
		}

		r.assignments[partitionID] = &PartitionInfo{
			ID:       partitionID,
			LeaderID: leaderID,
			Replicas: nodes,
			ISR:      nodes, // Initially all replicas are in-sync
			State:    PartitionStateOnline,
		}
	}

	log.Printf("[ROUTER] Initialized %d partitions across %d nodes", r.numPartitions, r.hashRing.Size())
}

// watchMembershipEvents watches for membership changes and rebalances
func (r *Router) watchMembershipEvents() {
	for event := range r.membership.Events() {
		switch event.Type {
		case EventTypeJoin:
			r.onNodeJoin(event.Node)
		case EventTypeLeave, EventTypeFailed:
			r.onNodeLeave(event.Node)
		}
	}
}

// onNodeJoin handles a new node joining
func (r *Router) onNodeJoin(node *Node) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Save old assignments for rebalancing
	oldAssignments := r.hashRing.GetPartitionAssignments(r.numPartitions)

	// Add node to hash ring
	r.hashRing.AddNode(node.ID)

	// Compute moves
	moves := r.hashRing.Rebalance(oldAssignments, r.numPartitions)

	// Update assignments
	r.updateAssignments()

	log.Printf("[ROUTER] Node %s joined, %d partition moves needed", node.ID, len(moves))

	// Trigger partition rebalancing (async)
	go r.executeRebalance(moves)
}

// onNodeLeave handles a node leaving
func (r *Router) onNodeLeave(node *Node) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Save old assignments for rebalancing
	oldAssignments := r.hashRing.GetPartitionAssignments(r.numPartitions)

	// Remove node from hash ring
	r.hashRing.RemoveNode(node.ID)

	// Compute moves
	moves := r.hashRing.Rebalance(oldAssignments, r.numPartitions)

	// Update assignments
	r.updateAssignments()

	log.Printf("[ROUTER] Node %s left, %d partition moves needed", node.ID, len(moves))

	// Trigger partition rebalancing (async)
	go r.executeRebalance(moves)
}

// updateAssignments updates partition assignments based on hash ring
func (r *Router) updateAssignments() {
	for i := 0; i < r.numPartitions; i++ {
		partitionID := int32(i)
		nodes := r.hashRing.GetPartitionNodes(partitionID)

		leaderID := ""
		if len(nodes) > 0 {
			leaderID = nodes[0]
		}

		if info, exists := r.assignments[partitionID]; exists {
			info.LeaderID = leaderID
			info.Replicas = nodes
		} else {
			r.assignments[partitionID] = &PartitionInfo{
				ID:       partitionID,
				LeaderID: leaderID,
				Replicas: nodes,
				ISR:      nodes,
				State:    PartitionStateOnline,
			}
		}
	}
}

// executeRebalance executes partition moves
func (r *Router) executeRebalance(moves []PartitionMove) {
	for _, move := range moves {
		log.Printf("[ROUTER] Rebalancing partition %d: %s -> %s (leader=%v)",
			move.PartitionID, move.FromNode, move.ToNode, move.IsLeader)

		// TODO: Implement actual data transfer
		// 1. If this node is FromNode and is leader, transfer leadership
		// 2. If this node is ToNode, prepare to receive data
		// 3. Coordinate with Raft for metadata updates
	}
}

// GetPartitionLeader returns the leader node for a partition
func (r *Router) GetPartitionLeader(partitionID int32) (*Node, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	info, exists := r.assignments[partitionID]
	if !exists {
		return nil, fmt.Errorf("partition %d not found", partitionID)
	}

	return r.membership.GetNode(info.LeaderID)
}

// GetPartitionReplicas returns all replica nodes for a partition
func (r *Router) GetPartitionReplicas(partitionID int32) ([]*Node, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	info, exists := r.assignments[partitionID]
	if !exists {
		return nil, fmt.Errorf("partition %d not found", partitionID)
	}

	nodes := make([]*Node, 0, len(info.Replicas))
	for _, nodeID := range info.Replicas {
		node, err := r.membership.GetNode(nodeID)
		if err != nil {
			continue
		}
		nodes = append(nodes, node)
	}

	return nodes, nil
}

// GetPartitionForTopic returns the partition ID for a topic
func (r *Router) GetPartitionForTopic(topic string) int32 {
	return r.hashRing.GetTopicPartition(topic, r.numPartitions)
}

// GetPartitionForKey returns the partition ID for a key
func (r *Router) GetPartitionForKey(key string) int32 {
	return r.hashRing.GetKeyPartition(key, r.numPartitions)
}

// IsLocalPartition returns true if the partition is owned by this node
func (r *Router) IsLocalPartition(partitionID int32) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	info, exists := r.assignments[partitionID]
	if !exists {
		return false
	}

	for _, nodeID := range info.Replicas {
		if nodeID == r.localNodeID {
			return true
		}
	}
	return false
}

// IsPartitionLeader returns true if this node is the leader for a partition
func (r *Router) IsPartitionLeader(partitionID int32) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	info, exists := r.assignments[partitionID]
	if !exists {
		return false
	}

	return info.LeaderID == r.localNodeID
}

// GetLocalPartitions returns partitions owned by this node
func (r *Router) GetLocalPartitions() []int32 {
	r.mu.RLock()
	defer r.mu.RUnlock()

	partitions := make([]int32, 0)
	for partitionID, info := range r.assignments {
		for _, nodeID := range info.Replicas {
			if nodeID == r.localNodeID {
				partitions = append(partitions, partitionID)
				break
			}
		}
	}
	return partitions
}

// GetLeaderPartitions returns partitions where this node is leader
func (r *Router) GetLeaderPartitions() []int32 {
	r.mu.RLock()
	defer r.mu.RUnlock()

	partitions := make([]int32, 0)
	for partitionID, info := range r.assignments {
		if info.LeaderID == r.localNodeID {
			partitions = append(partitions, partitionID)
		}
	}
	return partitions
}

// GetPartitionInfo returns partition information
func (r *Router) GetPartitionInfo(partitionID int32) (*PartitionInfo, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	info, exists := r.assignments[partitionID]
	if !exists {
		return nil, fmt.Errorf("partition %d not found", partitionID)
	}
	return info, nil
}

// GetAllPartitions returns all partition information
func (r *Router) GetAllPartitions() map[int32]*PartitionInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make(map[int32]*PartitionInfo)
	for k, v := range r.assignments {
		result[k] = v
	}
	return result
}

// RouteRequest determines where to route a request
func (r *Router) RouteRequest(topic string) (*RouteInfo, error) {
	partitionID := r.GetPartitionForTopic(topic)

	r.mu.RLock()
	defer r.mu.RUnlock()

	info, exists := r.assignments[partitionID]
	if !exists {
		return nil, fmt.Errorf("partition %d not found", partitionID)
	}

	leaderNode, err := r.membership.GetNode(info.LeaderID)
	if err != nil {
		return nil, fmt.Errorf("leader node %s not found: %w", info.LeaderID, err)
	}

	return &RouteInfo{
		PartitionID: partitionID,
		LeaderID:    info.LeaderID,
		LeaderAddr:  leaderNode.Address,
		IsLocal:     info.LeaderID == r.localNodeID,
		Replicas:    info.Replicas,
	}, nil
}

// RouteInfo contains routing information for a request
type RouteInfo struct {
	PartitionID int32
	LeaderID    string
	LeaderAddr  string
	IsLocal     bool
	Replicas    []string
}

package cluster

import (
	"fmt"
	"log"
	"sync"
	"time"
)

// PartitionAccessor is the bridge between the cluster router/manager and local
// partition lifecycle operations (sync, promote/demote, ISR/offset queries).
type PartitionAccessor interface {
	// SyncPartitionFromLeader syncs a local partition's data from its leader.
	SyncPartitionFromLeader(partitionID int32, leaderAddr string) error
	// GetOrCreatePartition gets or creates a local partition instance.
	GetOrCreatePartition(partitionID int32) error
	// PromoteToLeader promotes a local partition to leader and starts replication.
	PromoteToLeader(partitionID int32, epoch int64) error
	// AddFollower adds a follower to a local leader partition.
	AddFollower(partitionID int32, followerID string, followerAddr string) error
	// DemoteFromLeader demotes a local partition from leader.
	DemoteFromLeader(partitionID int32) error
	// GetPartitionReplicaOffsets returns the latest replica offsets for a local partition.
	GetPartitionReplicaOffsets(partitionID int32) map[string]int64
	// GetPartitionInSyncReplicas returns the current ISR for a local partition,
	// including the leader node ID.
	GetPartitionInSyncReplicas(partitionID int32) []string
}

// Router maps topics/keys to partitions and partitions to owning nodes using a
// consistent hash ring kept in sync with membership events.
type Router struct {
	mu                sync.RWMutex
	hashRing          *HashRing
	membership        MembershipService
	numPartitions     int
	localNodeID       string
	partitionAccessor PartitionAccessor

	// Partition assignments cache
	assignments map[int32]*PartitionInfo

	// onRebalance is called whenever the router recomputes assignments due to
	// membership changes. It lets the manager immediately sync metadata to Raft.
	onRebalance func()
}

// NewRouter creates a new partition router.
// virtualNodes controls hash-ring granularity and can materially impact
// partition leader distribution when partition counts are small.
func NewRouter(membership MembershipService, numPartitions, replicationFactor, virtualNodes int, accessor PartitionAccessor) *Router {
	r := &Router{
		hashRing:          NewHashRing(virtualNodes, replicationFactor),
		membership:        membership,
		numPartitions:     numPartitions,
		localNodeID:       membership.GetLocalNode().ID,
		partitionAccessor: accessor,
		assignments:       make(map[int32]*PartitionInfo),
	}

	// Initialize partition assignments
	r.initializePartitions()

	return r
}

// Start begins watching membership events. It should be called after any
// rebalance callbacks have been registered so that the first events are not
// missed.
func (r *Router) Start() {
	go r.watchMembershipEvents()
}

// SetOnRebalance registers a callback that is invoked after the router
// recomputes assignments due to a node join or leave. The callback should
// not block or call back into the router while holding the router lock.
func (r *Router) SetOnRebalance(cb func()) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.onRebalance = cb
}

// initializePartitions initializes partition assignments
func (r *Router) initializePartitions() {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Add existing nodes to hash ring
	for _, node := range r.membership.GetAliveNodes() {
		r.hashRing.AddNode(node.ID)
		r.hashRing.SetNodeTopology(node.ID, NodeTopology{
			Rack:   node.Rack,
			Zone:   node.Zone,
			Region: node.Region,
		})
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

// watchMembershipEvents watches for membership changes and rebalances. It also
// runs a periodic full reconcile of the hash ring against the authoritative
// alive-node set: membership events are delivered over a bounded channel that
// drops on overflow, so an event-only ring could diverge permanently (stale ring
// → wrong partition owners → split-brain). The reconcile makes any dropped event
// self-healing.
func (r *Router) watchMembershipEvents() {
	ticker := time.NewTicker(ringReconcileInterval)
	defer ticker.Stop()

	events := r.membership.Events()
	for {
		select {
		case event, ok := <-events:
			if !ok {
				return
			}
			switch event.Type {
			case EventTypeJoin:
				r.onNodeJoin(event.Node)
			case EventTypeLeave, EventTypeFailed:
				r.onNodeLeave(event.Node)
			}
		case <-ticker.C:
			r.reconcileRing()
		}
	}
}

// ringReconcileInterval is how often the router re-syncs its hash ring with the
// authoritative membership view to recover from any dropped membership events.
const ringReconcileInterval = 10 * time.Second

// reconcileRing brings the hash ring into agreement with the current alive-node
// set, adding nodes that are alive but missing from the ring and removing nodes
// in the ring that are no longer alive. It is idempotent and only rebalances
// when there is an actual delta, so steady-state ticks are cheap.
func (r *Router) reconcileRing() {
	alive := r.membership.GetAliveNodes()
	aliveSet := make(map[string]*Node, len(alive))
	for _, n := range alive {
		aliveSet[n.ID] = n
	}

	r.mu.RLock()
	ringNodes := r.hashRing.Nodes()
	r.mu.RUnlock()
	ringSet := make(map[string]bool, len(ringNodes))
	for _, id := range ringNodes {
		ringSet[id] = true
	}

	// Alive but not in the ring → treat as a (missed) join.
	for id, n := range aliveSet {
		if !ringSet[id] {
			log.Printf("[ROUTER] Reconcile: adding missing node %s to ring", id)
			r.onNodeJoin(n)
		}
	}
	// In the ring but no longer alive → treat as a (missed) leave.
	for _, id := range ringNodes {
		if _, ok := aliveSet[id]; !ok {
			log.Printf("[ROUTER] Reconcile: removing dead node %s from ring", id)
			r.onNodeLeave(&Node{ID: id})
		}
	}
}

// onNodeJoin handles a new node joining
func (r *Router) onNodeJoin(node *Node) {
	// Save old assignments for rebalancing
	oldAssignments := r.hashRing.GetPartitionAssignments(r.numPartitions)

	// Add node to hash ring
	r.hashRing.AddNode(node.ID)
	r.hashRing.SetNodeTopology(node.ID, NodeTopology{
		Rack:   node.Rack,
		Zone:   node.Zone,
		Region: node.Region,
	})

	// Compute moves
	moves := r.hashRing.Rebalance(oldAssignments, r.numPartitions)

	// Update cached assignments with minimal router lock hold time.
	r.mu.Lock()
	r.updateAssignments()
	cb := r.onRebalance
	r.mu.Unlock()

	log.Printf("[ROUTER] Node %s joined, %d partition moves needed", node.ID, len(moves))

	// Notify the manager so it can immediately sync the updated assignments to Raft.
	if cb != nil {
		cb()
	}

	// Trigger partition rebalancing (async)
	go r.executeRebalance(moves)
}

// onNodeLeave handles a node leaving
func (r *Router) onNodeLeave(node *Node) {
	// Save old assignments for rebalancing
	oldAssignments := r.hashRing.GetPartitionAssignments(r.numPartitions)

	// Remove node from hash ring
	r.hashRing.RemoveNode(node.ID)

	// Compute moves
	moves := r.hashRing.Rebalance(oldAssignments, r.numPartitions)

	// Update cached assignments with minimal router lock hold time.
	r.mu.Lock()
	r.updateAssignments()
	cb := r.onRebalance
	r.mu.Unlock()

	log.Printf("[ROUTER] Node %s left, %d partition moves needed", node.ID, len(moves))

	// Notify the manager so it can immediately sync the updated assignments to Raft.
	if cb != nil {
		cb()
	}

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

		// Check if we are the destination node receiving the partition
		if move.ToNode == r.localNodeID {
			// Check if we have a partition accessor
			if r.partitionAccessor == nil {
				log.Printf("[ROUTER] No partition accessor available, cannot sync partition %d", move.PartitionID)
				continue
			}

			// Find the current leader to sync from
			leader, err := r.GetPartitionLeader(move.PartitionID)
			if err != nil {
				log.Printf("[ROUTER] Cannot sync partition %d: %v", move.PartitionID, err)
				continue
			}

			if leader == nil || leader.Address == "" {
				log.Printf("[ROUTER] No leader address for partition %d", move.PartitionID)
				continue
			}

			log.Printf("[ROUTER] Node %s initiating bulk file sync from %s for partition %d", r.localNodeID, leader.Address, move.PartitionID)

			// Get or create the partition first
			if err := r.partitionAccessor.GetOrCreatePartition(move.PartitionID); err != nil {
				log.Printf("[ROUTER] Failed to get/create partition %d: %v", move.PartitionID, err)
				continue
			}

			// Sync partition data from leader
			if err := r.partitionAccessor.SyncPartitionFromLeader(move.PartitionID, leader.Address); err != nil {
				log.Printf("[ROUTER] Sync failed for partition %d: %v", move.PartitionID, err)
				// Don't fail hard - allow retry
				continue
			}

			log.Printf("[ROUTER] Sync complete for partition %d, ready to serve requests", move.PartitionID)
		}
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

// GetPartitionEpoch returns the cluster epoch for a partition.
func (r *Router) GetPartitionEpoch(partitionID int32) int64 {
	r.mu.RLock()
	defer r.mu.RUnlock()

	info, exists := r.assignments[partitionID]
	if !exists {
		return 0
	}
	return info.Epoch
}

// UpdatePartitionAssignment updates the cached partition assignment for cases
// where the FSM is the source of truth (e.g. leader failover). It keeps the
// router's local view consistent with the authoritative cluster state so that
// subsequent health checks and routing decisions do not revert to stale data.
func (r *Router) UpdatePartitionAssignment(partitionID int32, leaderID string, replicas []string, isr []string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	info, exists := r.assignments[partitionID]
	if !exists {
		info = &PartitionInfo{ID: partitionID}
		r.assignments[partitionID] = info
	}

	info.LeaderID = leaderID
	if len(replicas) > 0 {
		info.Replicas = append([]string(nil), replicas...)
	} else {
		info.Replicas = nil
	}
	if len(isr) > 0 {
		info.ISR = append([]string(nil), isr...)
	} else {
		info.ISR = nil
	}
	info.State = PartitionStateOnline
}

// UpdatePartitionISR updates the in-sync replica set for a partition.
func (r *Router) UpdatePartitionISR(partitionID int32, isr []string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	info, exists := r.assignments[partitionID]
	if !exists {
		return
	}
	if len(isr) > 0 {
		info.ISR = append([]string(nil), isr...)
	} else {
		info.ISR = nil
	}
}

// UpdateReplicaOffsets updates the per-replica high-watermark offsets for a partition.
// This is used by the cluster leader to track replication lag and choose the least-lag
// replica during failover.
func (r *Router) UpdateReplicaOffsets(partitionID int32, offsets map[string]int64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	info, exists := r.assignments[partitionID]
	if !exists {
		return
	}
	if info.ReplicaOffsets == nil {
		info.ReplicaOffsets = make(map[string]int64)
	}
	for id, offset := range offsets {
		info.ReplicaOffsets[id] = offset
	}
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

// RouteInfo contains routing information for a publish or related request.
type RouteInfo struct {
	// PartitionID is the target partition for the request.
	PartitionID int32
	// LeaderID is the node ID of the partition leader.
	LeaderID string
	// LeaderAddr is the gRPC address of the partition leader.
	LeaderAddr string
	// IsLocal is true when this node is the partition leader.
	IsLocal bool
	// Replicas is the full replica set for the partition (node IDs).
	Replicas []string
}

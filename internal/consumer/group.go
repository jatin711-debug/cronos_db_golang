// Package consumer implements consumer-group membership, partition assignment,
// offset commit, and durable offset/commit-ID storage for CronosDB.
package consumer

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

// Subscription represents an active consumer subscription to a partition.
type Subscription struct {
	// ID is the subscription/member identifier.
	ID string
	// ConsumerGroup is the consumer group this subscription belongs to.
	ConsumerGroup string
	// Partition is the local partition being consumed, if already bound.
	Partition *types.Partition
	// StartOffset is the first offset the subscription should read from.
	StartOffset int64
	// Delivery is the channel used to push deliveries to the subscriber.
	Delivery chan<- *types.Delivery
	// Done is closed when the subscription should stop.
	Done <-chan struct{}
	quit chan struct{}
}

// GroupManager manages consumer groups, membership, rebalancing, and offset commits.
type GroupManager struct {
	mu          sync.RWMutex
	groups      map[string]*types.ConsumerGroup
	partitions  map[int32]*types.Partition // partition_id -> partition
	offsetStore *OffsetStore               // persistent offset storage
	completed   map[string]bool            // Used only by the in-memory manager.
	// floors holds each group's completion floor per partition, keyed by
	// completionPrefix: every offset below it is complete. Guarded by mu.
	floors map[string]int64
	// logStart holds, per partition, where its log starts (see SetLogStart).
	// Guarded by mu.
	logStart map[int32]int64

	// completedMax caches, per group and partition, the highest offset known
	// complete. Guarded by completedMaxMu, which is never held while waiting
	// for mu.
	completedMaxMu sync.Mutex
	completedMax   map[string]int64

	// commitMu serializes progress writes (acks and replicated progress): each
	// derives a group's new cursor from the one before it.
	commitMu sync.Mutex
	// progressVersion changes whenever any group's progress or membership does.
	progressVersion atomic.Uint64
}

// NewGroupManager creates an in-memory-only group manager.
// Use NewGroupManagerWithStore when durable offsets and group metadata are required.
func NewGroupManager() *GroupManager {
	return &GroupManager{
		groups:     make(map[string]*types.ConsumerGroup),
		partitions: make(map[int32]*types.Partition),
	}
}

// NewGroupManagerWithStore creates a group manager backed by a persistent OffsetStore
// and restores any previously persisted group metadata.
func NewGroupManagerWithStore(offsetStore *OffsetStore) *GroupManager {
	gm := &GroupManager{
		groups:      make(map[string]*types.ConsumerGroup),
		partitions:  make(map[int32]*types.Partition),
		offsetStore: offsetStore,
	}
	// Restore persisted group metadata if available.
	if offsetStore != nil {
		if floors, err := offsetStore.loadFloors(); err == nil {
			gm.floors = floors
		}
		for id, group := range offsetStore.LoadGroups() {
			// Progress is written with every ack; the group record only when
			// membership changes. Take whichever is further along.
			for _, partitionID := range group.Partitions {
				if offset, err := offsetStore.GetOffset(id, partitionID); err == nil && offset > group.CommittedOffsets[partitionID] {
					group.CommittedOffsets[partitionID] = offset
				}
				if floor := gm.floors[completionPrefix(id, partitionID)]; floor > group.CommittedOffsets[partitionID] {
					group.CommittedOffsets[partitionID] = floor
				}
			}
			gm.groups[id] = group
		}
	}
	return gm
}

// persistGroup writes group metadata to the offset store if one is configured.
func (g *GroupManager) persistGroup(group *types.ConsumerGroup) {
	g.progressVersion.Add(1)
	if g.offsetStore != nil {
		_ = g.offsetStore.PersistGroup(group)
	}
}

// CreateGroup creates a new consumer group for topic with the given partition set.
// Committed offsets are initialized to -1 (beginning of log).
func (g *GroupManager) CreateGroup(groupID, topic string, partitions []int32) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	// Check if group already exists
	if _, exists := g.groups[groupID]; exists {
		return fmt.Errorf("group %s already exists", groupID)
	}

	// Create consumer group
	group := &types.ConsumerGroup{
		GroupID:          groupID,
		Topic:            topic,
		Partitions:       partitions,
		CommittedOffsets: make(map[int32]int64),
		MemberOffsets:    make(map[string]int64),
		Members:          make(map[string]*types.ConsumerMember),
		CreatedTS:        time.Now().UnixMilli(),
		UpdatedTS:        time.Now().UnixMilli(),
	}

	// Initialize offsets to -1 (beginning)
	for _, partition := range partitions {
		group.CommittedOffsets[partition] = -1
	}

	g.groups[groupID] = group
	g.persistGroup(group)
	return nil
}

// GetGroup returns a copy of the consumer group with groupID, if it exists.
func (g *GroupManager) GetGroup(groupID string) (*types.ConsumerGroup, bool) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	group, exists := g.groups[groupID]
	if !exists {
		return nil, false
	}
	return copyGroup(group), true
}

// copyGroup returns a copy of a group that shares nothing with it. The
// manager's own group objects change under its lock, their maps included, so
// handing one out would let a reader race with a commit: reading a map while
// it is written ends the process.
func copyGroup(group *types.ConsumerGroup) *types.ConsumerGroup {
	out := *group
	out.Partitions = append([]int32(nil), group.Partitions...)
	out.CommittedOffsets = make(map[int32]int64, len(group.CommittedOffsets))
	for partitionID, offset := range group.CommittedOffsets {
		out.CommittedOffsets[partitionID] = offset
	}
	out.MemberOffsets = make(map[string]int64, len(group.MemberOffsets))
	for memberID, offset := range group.MemberOffsets {
		out.MemberOffsets[memberID] = offset
	}
	out.Members = make(map[string]*types.ConsumerMember, len(group.Members))
	for memberID, member := range group.Members {
		if member != nil {
			memberCopy := *member
			member = &memberCopy
		}
		out.Members[memberID] = member
	}
	return &out
}

// Close releases any persistent resources held by the group manager, including
// the underlying offset store. It is safe to call when no store is configured.
func (g *GroupManager) Close() error {
	g.mu.Lock()
	store := g.offsetStore
	g.mu.Unlock()
	if store == nil {
		return nil
	}
	return store.Close()
}

// CheckpointOffsetStore creates a point-in-time checkpoint of the persistent
// offset store if one is configured. Returns nil if no offset store is attached.
func (g *GroupManager) CheckpointOffsetStore(destDir string) error {
	g.mu.RLock()
	store := g.offsetStore
	g.mu.RUnlock()

	if store == nil {
		return nil
	}
	return store.Checkpoint(destDir)
}

// JoinGroup adds or re-joins a consumer member to a group and rebalances assignments.
// The group is auto-created when it does not yet exist.
func (g *GroupManager) JoinGroup(groupID, memberID, address, topic string, partitionID int32) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	group, exists := g.groups[groupID]
	if !exists {
		// Auto-create group if it doesn't exist
		group = &types.ConsumerGroup{
			GroupID:          groupID,
			Topic:            topic,
			Partitions:       []int32{partitionID},
			CommittedOffsets: make(map[int32]int64),
			MemberOffsets:    make(map[string]int64),
			Members:          make(map[string]*types.ConsumerMember),
			CreatedTS:        time.Now().UnixMilli(),
			UpdatedTS:        time.Now().UnixMilli(),
		}
		group.CommittedOffsets[partitionID] = -1
		g.groups[groupID] = group
	}
	if group.Topic != topic {
		return fmt.Errorf("consumer group belongs to a different topic")
	}

	// Re-join/update existing member instead of failing hard.
	// This makes reconnects resilient when clients restart quickly.
	if existing, exists := group.Members[memberID]; exists {
		validPartition := false
		for _, p := range group.Partitions {
			if p == partitionID {
				validPartition = true
				break
			}
		}
		if !validPartition {
			group.Partitions = append(group.Partitions, partitionID)
			if _, ok := group.CommittedOffsets[partitionID]; !ok {
				group.CommittedOffsets[partitionID] = -1
			}
		}

		now := time.Now().UnixMilli()
		existing.Address = address
		existing.AssignedPartition = partitionID
		existing.Active = true
		existing.LastSeenTS = now
		existing.ConnectedTS = now
		group.UpdatedTS = now
		if err := g.rebalanceGroup(group); err != nil {
			return err
		}
		g.persistGroup(group)
		return nil
	}

	// Validate partition assignment
	validPartition := false
	for _, p := range group.Partitions {
		if p == partitionID {
			validPartition = true
			break
		}
	}
	if !validPartition {
		return fmt.Errorf("partition %d not assigned to group", partitionID)
	}

	// Create member
	member := &types.ConsumerMember{
		MemberID:          memberID,
		Address:           address,
		AssignedPartition: partitionID,
		Active:            true,
		LastSeenTS:        time.Now().UnixMilli(),
		ConnectedTS:       time.Now().UnixMilli(),
	}

	group.Members[memberID] = member
	group.UpdatedTS = time.Now().UnixMilli()

	// Rebalance to ensure partition assignment
	if err := g.rebalanceGroup(group); err != nil {
		return err
	}
	g.persistGroup(group)
	return nil
}

// LeaveGroup removes a consumer member from a group and rebalances remaining members.
func (g *GroupManager) LeaveGroup(groupID, memberID string) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	group, exists := g.groups[groupID]
	if !exists {
		return fmt.Errorf("group %s not found", groupID)
	}

	// Check if member exists
	member, exists := group.Members[memberID]
	if !exists {
		return fmt.Errorf("member %s not found in group", memberID)
	}

	// Set member as inactive and remove immediately.
	// The 30s grace period was removed because:
	// 1. CommittedOffsets (per-group) already persist consumer progress.
	// 2. MemberOffsets (per-member) is not used for ack processing.
	// 3. Keeping inactive members risks delivering messages to disconnected consumers.
	member.Active = false
	member.LastSeenTS = time.Now().UnixMilli()
	delete(group.Members, memberID)
	delete(group.MemberOffsets, memberID)

	// Rebalance to reassign partition
	if err := g.rebalanceGroup(group); err != nil {
		return fmt.Errorf("rebalance after leave: %w", err)
	}
	g.persistGroup(group)

	return nil
}

// TriggerRebalance manually rebalances groupID and returns active member-to-partition assignments.
func (g *GroupManager) TriggerRebalance(groupID string) (map[string]int32, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	group, exists := g.groups[groupID]
	if !exists {
		return nil, fmt.Errorf("group %s not found", groupID)
	}

	if err := g.rebalanceGroup(group); err != nil {
		return nil, err
	}

	assignments := make(map[string]int32)
	for memberID, member := range group.Members {
		if member.Active {
			assignments[memberID] = member.AssignedPartition
		}
	}

	return assignments, nil
}

// rebalanceGroup rebalances partition assignments
func (g *GroupManager) rebalanceGroup(group *types.ConsumerGroup) error {
	// Get active members
	var activeMembers []*types.ConsumerMember
	for _, member := range group.Members {
		if member.Active {
			activeMembers = append(activeMembers, member)
		}
	}

	if len(activeMembers) == 0 {
		// No active members, clear assignments
		return nil
	}

	// Simple round-robin assignment
	partitionAssignments := make(map[int32]*types.ConsumerMember)

	for i, partition := range group.Partitions {
		member := activeMembers[i%len(activeMembers)]
		partitionAssignments[partition] = member
	}

	// Update member assignments
	for partition, member := range partitionAssignments {
		member.AssignedPartition = partition
	}

	return nil
}

// CommitOffset commits an offset for a consumer group.
// Holds the full write lock because Go's memory model does NOT permit
// concurrent map writes even to different keys, and RLock does not
// synchronize with other RLock holders for map mutations.
func (g *GroupManager) CommitOffset(groupID string, partitionID int64, offset int64) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	group, exists := g.groups[groupID]
	if !exists {
		return fmt.Errorf("group %s not found", groupID)
	}

	// Validate partition
	if partitionID < 0 {
		return fmt.Errorf("invalid partition %d", partitionID)
	}

	if offset < group.CommittedOffsets[int32(partitionID)] {
		return fmt.Errorf("committed offset cannot move backwards")
	}
	group.CommittedOffsets[int32(partitionID)] = offset
	group.UpdatedTS = time.Now().UnixMilli()

	// Persist to offset store if available
	if g.offsetStore != nil {
		if err := g.offsetStore.CommitOffset(groupID, int32(partitionID), offset); err != nil {
			return fmt.Errorf("persist offset: %w", err)
		}
	}

	return nil
}

// GetCommittedOffset returns the committed offset for groupID on partitionID.
// It checks in-memory state first, then falls back to the offset store.
// A return of -1 means the beginning of the partition.
func (g *GroupManager) GetCommittedOffset(groupID string, partitionID int32) (int64, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	// Try in-memory first
	group, exists := g.groups[groupID]
	if exists {
		offset, ok := group.CommittedOffsets[partitionID]
		if ok {
			return offset, nil
		}
	}

	// Fall back to persistent store
	if g.offsetStore != nil {
		offset, err := g.offsetStore.GetOffset(groupID, partitionID)
		if err != nil {
			return -1, err
		}
		return offset, nil
	}

	return -1, nil // Beginning of partition
}

// ListGroups returns a copy of every known consumer group.
func (g *GroupManager) ListGroups() []*types.ConsumerGroup {
	g.mu.RLock()
	defer g.mu.RUnlock()

	groups := make([]*types.ConsumerGroup, 0, len(g.groups))
	for _, group := range g.groups {
		groups = append(groups, copyGroup(group))
	}

	return groups
}

// DeleteGroup deletes a consumer group and its persisted metadata, if any.
func (g *GroupManager) DeleteGroup(groupID string) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	if _, exists := g.groups[groupID]; !exists {
		return fmt.Errorf("group %s not found", groupID)
	}

	delete(g.groups, groupID)
	if g.offsetStore != nil {
		_ = g.offsetStore.DeletePersistedGroup(groupID)
	}
	return nil
}

// Subscribe creates a Subscription from req and joins the corresponding consumer group.
func (g *GroupManager) Subscribe(req *types.SubscribeRequest) (*Subscription, error) {
	// Create a subscription (simplified)
	sub := &Subscription{
		ID:            req.GetSubscriptionId(),
		ConsumerGroup: req.GetConsumerGroup(),
		Partition:     nil, // Will be set by handler
		StartOffset:   req.GetStartOffset(),
		Delivery:      nil, // Will be set by handler
		Done:          nil, // Will be set by handler
	}

	// Add to group
	if err := g.JoinGroup(req.GetConsumerGroup(), req.GetSubscriptionId(), "", req.GetTopic(), req.GetPartitionId()); err != nil {
		return nil, fmt.Errorf("join group: %w", err)
	}

	return sub, nil
}

// Ack acknowledges event processing by committing the next offset encoded in the delivery ID.
func (g *GroupManager) Ack(req *types.AckRequest) error {
	if !req.GetSuccess() {
		return fmt.Errorf("unsuccessful acknowledgments cannot commit offsets")
	}
	if req.DeliveryId == "" {
		return fmt.Errorf("delivery_id is required")
	}

	// Delivery IDs are generated by dispatcher as:
	//   single: "group:partition:subscription-offset"
	//   batch:  "group:partition:subscription-batch-offset-count"
	// Parse only the first two colon-separated fields to avoid ambiguity
	// when group/subscription IDs contain '-' characters.
	parts := strings.SplitN(req.DeliveryId, ":", 3)
	if len(parts) < 3 {
		return fmt.Errorf("invalid delivery_id format: %s", req.DeliveryId)
	}

	partitionID, err := strconv.ParseInt(parts[1], 10, 32)
	if err != nil {
		return fmt.Errorf("invalid partition ID in delivery_id %s: %w", req.DeliveryId, err)
	}

	groupID := parts[0]
	return g.CommitOffset(groupID, partitionID, req.NextOffset)
}

// GetStats returns aggregate consumer-group membership statistics.
func (g *GroupManager) GetStats() *GroupManagerStats {
	g.mu.RLock()
	defer g.mu.RUnlock()

	totalGroups := len(g.groups)
	totalMembers := 0
	activeGroups := 0

	for _, group := range g.groups {
		active := false
		for _, member := range group.Members {
			totalMembers++
			if member.Active {
				active = true
			}
		}
		if active {
			activeGroups++
		}
	}

	return &GroupManagerStats{
		TotalGroups:   int64(totalGroups),
		ActiveGroups:  int64(activeGroups),
		TotalMembers:  int64(totalMembers),
		ActiveMembers: int64(totalMembers), // Simplified
	}
}

// GroupManagerStats is an aggregate snapshot of consumer-group membership.
type GroupManagerStats struct {
	// TotalGroups is the number of known consumer groups.
	TotalGroups int64
	// ActiveGroups is the number of groups with at least one active member.
	ActiveGroups int64
	// TotalMembers is the total member count across all groups.
	TotalMembers int64
	// ActiveMembers is the count of members currently considered active.
	ActiveMembers int64
}

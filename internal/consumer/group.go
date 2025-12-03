package consumer

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"cronos_db/pkg/types"
)

// Subscription represents a subscriber
type Subscription struct {
	ID            string
	ConsumerGroup string
	Partition     *types.Partition
	StartOffset   int64
	Delivery      chan<- *types.Delivery
	Done          <-chan struct{}
	quit          chan struct{}
}

// GroupManager manages consumer groups
type GroupManager struct {
	mu          sync.RWMutex
	groups      map[string]*types.ConsumerGroup
	partitions  map[int32]*types.Partition // partition_id -> partition
	offsetStore *OffsetStore               // persistent offset storage
}

// NewGroupManager creates a new group manager (in-memory only, use NewGroupManagerWithStore for persistence)
func NewGroupManager() *GroupManager {
	return &GroupManager{
		groups:     make(map[string]*types.ConsumerGroup),
		partitions: make(map[int32]*types.Partition),
	}
}

// NewGroupManagerWithStore creates a new group manager with persistent offset storage
func NewGroupManagerWithStore(offsetStore *OffsetStore) *GroupManager {
	return &GroupManager{
		groups:      make(map[string]*types.ConsumerGroup),
		partitions:  make(map[int32]*types.Partition),
		offsetStore: offsetStore,
	}
}

// CreateGroup creates a new consumer group
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
	return nil
}

// ensureGroup ensures a consumer group exists, creating it if necessary
func (g *GroupManager) ensureGroup(groupID, topic string, partitionID int32) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	// Check if group exists
	group, exists := g.groups[groupID]
	if exists {
		return nil
	}

	// Create new group with the single partition
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

	// Initialize offsets to -1 (beginning)
	group.CommittedOffsets[partitionID] = -1

	g.groups[groupID] = group
	return nil
}

// GetGroup returns a consumer group
func (g *GroupManager) GetGroup(groupID string) (*types.ConsumerGroup, bool) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	group, exists := g.groups[groupID]
	return group, exists
}

// JoinGroup adds a consumer to a group
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

	// Check if member already exists
	if _, exists := group.Members[memberID]; exists {
		return fmt.Errorf("member %s already in group", memberID)
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
	return g.rebalanceGroup(group)
}

// LeaveGroup removes a consumer from a group
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

	// Set member as inactive
	member.Active = false
	member.LastSeenTS = time.Now().UnixMilli()

	// Rebalance to reassign partition
	if err := g.rebalanceGroup(group); err != nil {
		return fmt.Errorf("rebalance after leave: %w", err)
	}

	// Remove member after delay (grace period)
	go func() {
		time.Sleep(30 * time.Second) // 30 second grace period
		g.mu.Lock()
		delete(group.Members, memberID)
		delete(group.MemberOffsets, memberID)
		g.mu.Unlock()
	}()

	return nil
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

// CommitOffset commits an offset for a consumer group
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

	// Update in-memory committed offset
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

// GetCommittedOffset gets committed offset for a partition
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

// ListGroups lists all consumer groups
func (g *GroupManager) ListGroups() []*types.ConsumerGroup {
	g.mu.RLock()
	defer g.mu.RUnlock()

	groups := make([]*types.ConsumerGroup, 0, len(g.groups))
	for _, group := range g.groups {
		groups = append(groups, group)
	}

	return groups
}

// DeleteGroup deletes a consumer group
func (g *GroupManager) DeleteGroup(groupID string) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	if _, exists := g.groups[groupID]; !exists {
		return fmt.Errorf("group %s not found", groupID)
	}

	delete(g.groups, groupID)
	return nil
}

// Subscribe creates a subscription
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

// Ack acknowledges event processing
func (g *GroupManager) Ack(req *types.AckRequest) error {
	// Parse consumer group from delivery ID
	// Delivery ID format: {consumerGroup}:{partitionId}:{subscriptionId}:{offset}
	groupID := ""
	if req.DeliveryId != "" {
		// Extract group ID from delivery ID
		parts := strings.Split(req.DeliveryId, ":")
		if len(parts) >= 1 {
			groupID = parts[0]
		}
	}

	// Commit the offset for this consumer group
	return g.CommitOffset(groupID, int64(req.NextOffset), req.NextOffset)
}

// GetStats returns group manager statistics
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

// GroupManagerStats represents group manager statistics
type GroupManagerStats struct {
	TotalGroups   int64
	ActiveGroups  int64
	TotalMembers  int64
	ActiveMembers int64
}

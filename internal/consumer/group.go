package consumer

import (
	"fmt"
	"sync"
	"time"

	"cronos_db/internal/api"
	"cronos_db/pkg/types"
)

// GroupManager manages consumer groups
type GroupManager struct {
	mu         sync.RWMutex
	groups     map[string]*types.ConsumerGroup
	partitions map[int32]*types.Partition // partition_id -> partition
}

// NewGroupManager creates a new group manager
func NewGroupManager() *GroupManager {
	return &GroupManager{
		groups:     make(map[string]*types.ConsumerGroup),
		partitions: make(map[int32]*types.Partition),
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

// GetGroup returns a consumer group
func (g *GroupManager) GetGroup(groupID string) (*types.ConsumerGroup, bool) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	group, exists := g.groups[groupID]
	return group, exists
}

// JoinGroup adds a consumer to a group
func (g *GroupManager) JoinGroup(groupID, memberID, address string, partitionID int32) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	group, exists := g.groups[groupID]
	if !exists {
		return fmt.Errorf("group %s not found", groupID)
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
		MemberID:         memberID,
		Address:          address,
		AssignedPartition: partitionID,
		Active:           true,
		LastSeenTS:       time.Now().UnixMilli(),
		ConnectedTS:      time.Now().UnixMilli(),
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
	if partitionID < 0 || partitionID > int64(len(group.Partitions)) {
		return fmt.Errorf("invalid partition %d", partitionID)
	}

	// Update committed offset
	group.CommittedOffsets[int32(partitionID)] = offset
	group.UpdatedTS = time.Now().UnixMilli()

	return nil
}

// GetCommittedOffset gets committed offset for a partition
func (g *GroupManager) GetCommittedOffset(groupID string, partitionID int32) (int64, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	group, exists := g.groups[groupID]
	if !exists {
		return 0, fmt.Errorf("group %s not found", groupID)
	}

	offset, exists := group.CommittedOffsets[partitionID]
	if !exists {
		return -1, nil // Beginning of partition
	}

	return offset, nil
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
func (g *GroupManager) Subscribe(req *types.SubscribeRequest) (*api.Subscription, error) {
	// Create a subscription (simplified)
	sub := &api.Subscription{
		ID:            req.SubscriptionID,
		ConsumerGroup: req.ConsumerGroup,
		Partition:     nil, // Will be set by handler
		StartOffset:   req.StartOffset,
		Delivery:      nil, // Will be set by handler
		Done:          nil, // Will be set by handler
	}

	// Add to group
	if err := g.JoinGroup(req.ConsumerGroup, req.SubscriptionID, "", req.PartitionID); err != nil {
		return nil, fmt.Errorf("join group: %w", err)
	}

	return sub, nil
}

// Ack acknowledges event processing
func (g *GroupManager) Ack(req *types.AckRequest) error {
	// Find the consumer group from the delivery ID or context
	// For now, we'll just commit the offset
	return g.CommitOffset("", int64(req.NextOffset), req.NextOffset)
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
		TotalGroups:    int64(totalGroups),
		ActiveGroups:   int64(activeGroups),
		TotalMembers:   int64(totalMembers),
		ActiveMembers:  int64(totalMembers), // Simplified
	}
}

// GroupManagerStats represents group manager statistics
type GroupManagerStats struct {
	TotalGroups   int64
	ActiveGroups  int64
	TotalMembers  int64
	ActiveMembers int64
}

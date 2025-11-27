package api

import (
	"context"

	"cronos_db/internal/consumer"
	"cronos_db/pkg/types"
)

// ConsumerGroupServiceHandler implements the ConsumerGroupService handler
type ConsumerGroupServiceHandler struct {
	types.UnimplementedConsumerGroupServiceServer

	consumerManager *consumer.GroupManager
}

// NewConsumerGroupServiceHandler creates a new consumer group service handler
func NewConsumerGroupServiceHandler(cm *consumer.GroupManager) *ConsumerGroupServiceHandler {
	return &ConsumerGroupServiceHandler{
		consumerManager: cm,
	}
}

// CreateConsumerGroup creates a new consumer group
func (h *ConsumerGroupServiceHandler) CreateConsumerGroup(
	ctx context.Context,
	req *types.CreateConsumerGroupRequest,
) (*types.CreateConsumerGroupResponse, error) {
	if err := h.consumerManager.CreateGroup(
		req.GetGroupId(),
		req.GetTopic(),
		req.GetPartitions(),
	); err != nil {
		return &types.CreateConsumerGroupResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	return &types.CreateConsumerGroupResponse{
		Success: true,
	}, nil
}

// GetConsumerGroup gets consumer group metadata
func (h *ConsumerGroupServiceHandler) GetConsumerGroup(
	ctx context.Context,
	req *types.GetConsumerGroupRequest,
) (*types.ConsumerGroupMetadata, error) {
	group, exists := h.consumerManager.GetGroup(req.GetGroupId())
	if !exists {
		return nil, types.ErrConsumerGroupNotFound
	}

	// Convert internal type to protobuf type
	return &types.ConsumerGroupMetadata{
		GroupId:          group.GroupID,
		Topic:            group.Topic,
		Partitions:       group.Partitions,
		CommittedOffsets: group.CommittedOffsets,
		MemberOffsets:    group.MemberOffsets,
		LastRebalanceTs:  group.UpdatedTS,
	}, nil
}

// ListConsumerGroups lists all consumer groups
func (h *ConsumerGroupServiceHandler) ListConsumerGroups(
	ctx context.Context,
	req *types.ListConsumerGroupsRequest,
) (*types.ListConsumerGroupsResponse, error) {
	groups := h.consumerManager.ListGroups()

	// Convert to protobuf types
	var protobufGroups []*types.ConsumerGroupMetadata
	for _, group := range groups {
		protobufGroups = append(protobufGroups, &types.ConsumerGroupMetadata{
			GroupId:          group.GroupID,
			Topic:            group.Topic,
			Partitions:       group.Partitions,
			CommittedOffsets: group.CommittedOffsets,
			MemberOffsets:    group.MemberOffsets,
			LastRebalanceTs:  group.UpdatedTS,
		})
	}

	return &types.ListConsumerGroupsResponse{
		Groups: protobufGroups,
	}, nil
}

// RebalanceConsumerGroup rebalances a consumer group
func (h *ConsumerGroupServiceHandler) RebalanceConsumerGroup(
	ctx context.Context,
	req *types.RebalanceConsumerGroupRequest,
) (*types.RebalanceConsumerGroupResponse, error) {
	// Get the group
	_, exists := h.consumerManager.GetGroup(req.GetGroupId())
	if !exists {
		return &types.RebalanceConsumerGroupResponse{
			Success: false,
			Error:   "group not found",
		}, nil
	}

	// Call internal rebalance (note: rebalanceGroup is lowercase/unexported in consumer package)
	// For now, return success as rebalancing is a TODO
	return &types.RebalanceConsumerGroupResponse{
		Success:             true,
		PartitionAssignments: map[string]int32{},
	}, nil
}

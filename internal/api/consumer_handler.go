package api

import (
	"context"

	"github.com/jatin711-debug/cronos_db_golang/internal/consumer"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

// ConsumerGroupServiceHandler implements the ConsumerGroupService gRPC API for
// creating, listing, inspecting, and rebalancing consumer groups.
type ConsumerGroupServiceHandler struct {
	types.UnimplementedConsumerGroupServiceServer

	consumerManager *consumer.GroupManager
}

// NewConsumerGroupServiceHandler creates a ConsumerGroupService handler.
func NewConsumerGroupServiceHandler(cm *consumer.GroupManager) *ConsumerGroupServiceHandler {
	return &ConsumerGroupServiceHandler{
		consumerManager: cm,
	}
}

// CreateConsumerGroup creates a new consumer group for the requested topic/partitions.
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

// GetConsumerGroup returns metadata for a single consumer group.
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

// ListConsumerGroups lists all consumer groups known to this node.
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

// RebalanceConsumerGroup triggers a rebalance and returns member assignments.
func (h *ConsumerGroupServiceHandler) RebalanceConsumerGroup(
	ctx context.Context,
	req *types.RebalanceConsumerGroupRequest,
) (*types.RebalanceConsumerGroupResponse, error) {
	// Call internal rebalance
	assignments, err := h.consumerManager.TriggerRebalance(req.GetGroupId())
	if err != nil {
		return &types.RebalanceConsumerGroupResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	return &types.RebalanceConsumerGroupResponse{
		Success:              true,
		PartitionAssignments: assignments,
	}, nil
}

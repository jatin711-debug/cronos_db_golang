package api

import (
	"context"
	"fmt"

	"cronos_db/pkg/types"
)

// EventServiceHandler implements the EventService handler
type EventServiceHandler struct {
	UnimplementedEventServiceServer

	partitionManager  types.PartitionManager
	dedupManager      DedupManager
	consumerManager   ConsumerManager
}

// DedupManager interface
type DedupManager interface {
	IsDuplicate(messageID string, offset int64) (bool, error)
}

// ConsumerManager interface
type ConsumerManager interface {
	Subscribe(request *types.SubscribeRequest) (*Subscription, error)
	Ack(request *types.AckRequest) error
}

// Subscription represents a subscriber
type Subscription struct {
	ID             string
	ConsumerGroup  string
	Partition      *types.Partition
	StartOffset    int64
	Delivery       chan<- *types.Delivery
	Done           <-chan struct{}
	quit           chan struct{}
}

// NewEventServiceHandler creates a new event service handler
func NewEventServiceHandler(
	pm types.PartitionManager,
	dm DedupManager,
	cm ConsumerManager,
) *EventServiceHandler {
	return &EventServiceHandler{
		partitionManager: pm,
		dedupManager:     dm,
		consumerManager:  cm,
	}
}

// Publish handles publish requests
func (h *EventServiceHandler) Publish(ctx context.Context, req *types.PublishRequest) (*types.PublishResponse, error) {
	event := req.Event

	// Validate event
	if event.MessageID == "" {
		return &types.PublishResponse{
			Success: false,
			Error:   "message_id is required",
		}, nil
	}

	if event.ScheduleTS <= 0 {
		return &types.PublishResponse{
			Success: false,
			Error:   "schedule_ts is required",
		}, nil
	}

	if len(event.Payload) == 0 {
		return &types.PublishResponse{
			Success: false,
			Error:   "payload is required",
		}, nil
	}

	// Get partition for topic
	partition, err := h.partitionManager.GetPartitionForTopic(event.Topic)
	if err != nil {
		return &types.PublishResponse{
			Success: false,
			Error:   fmt.Sprintf("get partition: %v", err),
		}, nil
	}

	// Check if duplicate (unless explicitly allowed)
	if !req.AllowDuplicate {
		isDuplicate, err := h.dedupManager.IsDuplicate(event.MessageID, 0) // offset will be assigned
		if err != nil {
			return &types.PublishResponse{
				Success: false,
				Error:   fmt.Sprintf("check duplicate: %v", err),
			}, nil
		}
		if isDuplicate {
			return &types.PublishResponse{
				Success: false,
				Error:   "duplicate message_id",
			}, nil
		}
	}

	// TODO: Append to WAL (handled by partition manager)
	// For now, return success with placeholder values

	return &types.PublishResponse{
		Success:     true,
		Error:       "",
		Offset:      0,  // Would be set by partition manager
		PartitionID: partition.ID,
		ScheduleTS:  event.ScheduleTS,
	}, nil
}

// Subscribe handles streaming subscription
func (h *EventServiceHandler) Subscribe(req *types.SubscribeRequest, stream types.EventService_SubscribeServer) error {
	// Create subscription
	_, err := h.consumerManager.Subscribe(req)
	if err != nil {
		return err
	}

	// TODO: Start delivery loop
	// For now, just wait for context cancellation
	<-stream.Context().Done()
	return nil
}

// Ack handles ack requests
func (h *EventServiceHandler) Ack(req *types.AckRequest) (*types.AckResponse, error) {
	err := h.consumerManager.Ack(req)
	if err != nil {
		return &types.AckResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	return &types.AckResponse{
		Success:         true,
		CommittedOffset: req.NextOffset,
	}, nil
}

// Replay handles replay requests
func (h *EventServiceHandler) Replay(req *types.ReplayRequest, stream types.EventService_ReplayServer) error {
	// Get partition
	if req.PartitionID >= 0 {
		_, err := h.partitionManager.GetPartition(req.PartitionID)
		if err != nil {
			return fmt.Errorf("get partition: %w", err)
		}
	} else {
		// TODO: Replay all partitions
		return fmt.Errorf("replay all partitions not implemented")
	}

	// TODO: Scan events in range using partition's WAL

	// For now, return empty stream
	return nil
}

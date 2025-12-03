package api

import (
	"context"
	"fmt"
	"time"

	"cronos_db/internal/consumer"
	"cronos_db/internal/delivery"
	"cronos_db/internal/partition"
	"cronos_db/internal/replay"
	"cronos_db/pkg/types"

	"google.golang.org/grpc"
)

// EventServiceHandler implements the EventService handler
type EventServiceHandler struct {
	types.UnimplementedEventServiceServer

	partitionManager *partition.PartitionManager
	dedupManager     DedupManager
	consumerManager  ConsumerManager
}

// DedupManager interface
type DedupManager interface {
	IsDuplicate(messageID string, offset int64) (bool, error)
}

// ConsumerManager interface
type ConsumerManager interface {
	Subscribe(request *types.SubscribeRequest) (*consumer.Subscription, error)
	Ack(request *types.AckRequest) error
	GetCommittedOffset(groupID string, partitionID int32) (int64, error)
}

// GRPCStream wraps gRPC stream to implement delivery.Stream interface
type GRPCStream struct {
	stream grpc.BidiStreamingServer[types.SubscribeRequest, types.Delivery]
}

// Send sends a delivery message
func (s *GRPCStream) Send(delivery *delivery.DeliveryMessage) error {
	// Convert delivery.DeliveryMessage to types.Delivery
	return s.stream.Send(&types.Delivery{
		DeliveryId:   delivery.DeliveryID,
		Event:        delivery.Event,
		Attempt:      delivery.Attempt,
		AckTimeoutMs: delivery.AckTimeout,
	})
}

// Recv receives a control message (not used in current implementation)
func (s *GRPCStream) Recv() (*delivery.Control, error) {
	return nil, fmt.Errorf("not implemented")
}

// Context returns the stream context
func (s *GRPCStream) Context() context.Context {
	return s.stream.Context()
}

// NewEventServiceHandler creates a new event service handler
func NewEventServiceHandler(
	pm *partition.PartitionManager,
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

	fmt.Printf("[API] Publish called: message_id=%s, topic=%s, schedule_ts=%d\n",
		event.GetMessageId(), event.Topic, event.GetScheduleTs())

	// Validate event
	if event.GetMessageId() == "" {
		return &types.PublishResponse{
			Success: false,
			Error:   "message_id is required",
		}, nil
	}

	if event.GetScheduleTs() <= 0 {
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

	fmt.Printf("[API] Validation passed, getting partition...\n")

	// Get internal partition
	partition, err := h.partitionManager.GetPartitionForTopic(event.Topic)
	if err != nil {
		return &types.PublishResponse{
			Success: false,
			Error:   fmt.Sprintf("get partition: %v", err),
		}, nil
	}

	fmt.Printf("[API] Got partition: %d\n", partition.ID)

	// Get internal partition object for WAL access
	partitionInternal, err := h.partitionManager.GetInternalPartition(partition.ID)
	if err != nil {
		return &types.PublishResponse{
			Success: false,
			Error:   fmt.Sprintf("get internal partition: %v", err),
		}, nil
	}

	fmt.Printf("[API] Got internal partition\n")

	// Check if duplicate (unless explicitly allowed)
	if !req.AllowDuplicate {
		isDuplicate, err := h.dedupManager.IsDuplicate(event.GetMessageId(), 0) // offset will be assigned
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

	// Append to WAL
	fmt.Printf("[API] Appending to WAL...\n")
	if err := partitionInternal.Wal.AppendEvent(event); err != nil {
		return &types.PublishResponse{
			Success: false,
			Error:   fmt.Sprintf("append to WAL: %v", err),
		}, nil
	}
	fmt.Printf("[API] Appended to WAL successfully\n")

	// Flush to ensure data is persisted
	fmt.Printf("[API] Flushing WAL...\n")
	if err := partitionInternal.Wal.Flush(); err != nil {
		return &types.PublishResponse{
			Success: false,
			Error:   fmt.Sprintf("flush WAL: %v", err),
		}, nil
	}
	fmt.Printf("[API] Flushed WAL successfully\n")

	// Schedule the event in timing wheel
	fmt.Printf("[API] Scheduling event...\n")
	if err := partitionInternal.Scheduler.Schedule(event); err != nil {
		return &types.PublishResponse{
			Success: false,
			Error:   fmt.Sprintf("schedule event: %v", err),
		}, nil
	}
	fmt.Printf("[API] Scheduled event successfully\n")

	return &types.PublishResponse{
		Success:     true,
		Error:       "",
		Offset:      event.Offset,
		PartitionId: partition.ID,
		ScheduleTs:  event.GetScheduleTs(),
	}, nil
}

// Subscribe handles streaming subscription
func (h *EventServiceHandler) Subscribe(stream grpc.BidiStreamingServer[types.SubscribeRequest, types.Delivery]) error {
	// Receive subscription request
	req, err := stream.Recv()
	if err != nil {
		return err
	}

	// Handle partition auto-assignment
	partitionID := req.GetPartitionId()
	if partitionID < 0 {
		// Auto-assign partition based on topic
		partitionInfo, err := h.partitionManager.GetPartitionForTopic(req.GetTopic())
		if err != nil {
			return fmt.Errorf("auto-assign partition for topic %s: %w", req.GetTopic(), err)
		}
		partitionID = partitionInfo.ID
	}

	// Get internal partition
	partitionInternal, err := h.partitionManager.GetInternalPartition(partitionID)
	if err != nil {
		return fmt.Errorf("get partition %d: %w", partitionID, err)
	}

	// Get consumer group offset
	startOffset, err := h.consumerManager.GetCommittedOffset(req.GetConsumerGroup(), partitionID)
	if err != nil {
		startOffset = -1 // Start from beginning if no offset
	}

	// Create subscription ID
	subID := fmt.Sprintf("%s:%d:%s", req.GetConsumerGroup(), partitionID, req.GetSubscriptionId())

	// Create subscription object for dispatcher
	subscription := &delivery.Subscription{
		ID:            subID,
		ConsumerGroup: req.GetConsumerGroup(),
		Partition:     &types.Partition{ID: int32(partitionID)},
		NextOffset:    startOffset + 1,
		MaxCredits:    100,
		CreatedTS:     time.Now().UnixMilli(),
		Stream:        &GRPCStream{stream: stream},
	}

	// Register subscription with partition's dispatcher
	if partitionInternal.Dispatcher != nil {
		if err := partitionInternal.Dispatcher.Subscribe(subscription); err != nil {
			return fmt.Errorf("register subscription: %w", err)
		}
		// Ensure cleanup on disconnect
		defer func() {
			if err := partitionInternal.Dispatcher.Unsubscribe(subID); err != nil {
				// Log but don't fail - subscription may already be cleaned up
				fmt.Printf("[SUBSCRIBE] Failed to unsubscribe %s: %v\n", subID, err)
			}
		}()
	}

	// Create consumer group subscription
	if h.consumerManager != nil {
		if _, err := h.consumerManager.Subscribe(req); err != nil {
			return fmt.Errorf("create consumer group: %w", err)
		}
	}

	// Wait for context cancellation (client disconnect)
	<-stream.Context().Done()
	return nil
}

// Ack handles streaming ack requests
func (h *EventServiceHandler) Ack(stream types.EventService_AckServer) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}

		err = h.consumerManager.Ack(req)
		if err != nil {
			resp := &types.AckResponse{
				Success: false,
				Error:   err.Error(),
			}
			if err := stream.Send(resp); err != nil {
				return err
			}
			continue
		}

		resp := &types.AckResponse{
			Success:         true,
			CommittedOffset: req.NextOffset,
		}
		if err := stream.Send(resp); err != nil {
			return err
		}
	}
}

// Replay handles replay requests
func (h *EventServiceHandler) Replay(req *types.ReplayRequest, stream types.EventService_ReplayServer) error {
	// Get partition
	partitionID := req.GetPartitionId()
	if partitionID < 0 {
		return fmt.Errorf("partition_id is required for replay")
	}

	partitionInternal, err := h.partitionManager.GetInternalPartition(partitionID)
	if err != nil {
		return fmt.Errorf("get partition %d: %w", partitionID, err)
	}

	// Create replay engine for this partition's WAL
	replayEngine := replay.NewReplayEngine(partitionInternal.Wal)

	// Create replay request
	replayReq := &replay.ReplayRequest{
		Topic:          req.GetTopic(),
		PartitionID:    partitionID,
		StartTS:        req.GetStartTs(),
		EndTS:          req.GetEndTs(),
		StartOffset:    req.GetStartOffset(),
		Count:          req.GetCount(),
		ConsumerGroup:  req.GetConsumerGroup(),
		SubscriptionID: req.GetSubscriptionId(),
		Speed:          req.GetSpeed(),
	}

	// Create channel for replay events
	eventCh := make(chan *replay.ReplayEvent, 100)

	// Start replay in goroutine
	errCh := make(chan error, 1)
	go func() {
		errCh <- replayEngine.ReplayStream(stream.Context(), replayReq, eventCh)
	}()

	// Stream events to client
	for event := range eventCh {
		replayEvent := &types.ReplayEvent{
			Event:        event.Event,
			ReplayOffset: event.ReplayOffset,
		}
		if err := stream.Send(replayEvent); err != nil {
			return fmt.Errorf("send replay event: %w", err)
		}
	}

	// Check for replay errors
	if err := <-errCh; err != nil {
		return fmt.Errorf("replay: %w", err)
	}

	return nil
}

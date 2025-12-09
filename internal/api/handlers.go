package api

import (
	"context"
	"fmt"
	"log/slog"
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

	// Get partition - use partition_key from meta if available, otherwise use message_id
	var partition *types.Partition
	var err error

	partitionKey := event.GetMessageId() // Default to message_id for distribution
	if pk, ok := event.Meta["partition_key"]; ok && pk != "" {
		partitionKey = pk
	}

	// Use key-based partitioning for better distribution
	partition, err = h.partitionManager.GetPartitionForKey(partitionKey)
	if err != nil {
		// Fallback to topic-based partitioning
		partition, err = h.partitionManager.GetPartitionForTopic(event.Topic)
		if err != nil {
			return &types.PublishResponse{
				Success: false,
				Error:   fmt.Sprintf("get partition: %v", err),
			}, nil
		}
	}

	// Get internal partition object for WAL access
	partitionInternal, err := h.partitionManager.GetInternalPartition(partition.ID)
	if err != nil {
		return &types.PublishResponse{
			Success: false,
			Error:   fmt.Sprintf("get internal partition: %v", err),
		}, nil
	}

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

	// Append to WAL (no sync on every write for performance - WAL handles periodic flush)
	if err := partitionInternal.Wal.AppendEvent(event); err != nil {
		return &types.PublishResponse{
			Success: false,
			Error:   fmt.Sprintf("append to WAL: %v", err),
		}, nil
	}

	// Schedule the event in timing wheel
	if err := partitionInternal.Scheduler.Schedule(event); err != nil {
		return &types.PublishResponse{
			Success: false,
			Error:   fmt.Sprintf("schedule event: %v", err),
		}, nil
	}

	return &types.PublishResponse{
		Success:     true,
		Error:       "",
		Offset:      event.Offset,
		PartitionId: partition.ID,
		ScheduleTs:  event.GetScheduleTs(),
	}, nil
}

// PublishBatch handles batch publish requests for high-throughput ingestion
func (h *EventServiceHandler) PublishBatch(ctx context.Context, req *types.PublishBatchRequest) (*types.PublishBatchResponse, error) {
	if len(req.Events) == 0 {
		return &types.PublishBatchResponse{
			Success: true,
		}, nil
	}

	var publishedCount, duplicateCount, errorCount int32
	var firstOffset, lastOffset int64 = -1, -1

	// Group events by partition for batch WAL writes
	partitionEvents := make(map[int32][]*types.Event)

	for _, event := range req.Events {
		// Basic validation
		if event.GetMessageId() == "" || event.GetScheduleTs() <= 0 || len(event.Payload) == 0 {
			errorCount++
			continue
		}

		// Get partition
		partitionKey := event.GetMessageId()
		if pk, ok := event.Meta["partition_key"]; ok && pk != "" {
			partitionKey = pk
		}

		partition, err := h.partitionManager.GetPartitionForKey(partitionKey)
		if err != nil {
			partition, err = h.partitionManager.GetPartitionForTopic(event.Topic)
			if err != nil {
				errorCount++
				continue
			}
		}

		// Check dedup
		if !req.AllowDuplicate {
			isDuplicate, err := h.dedupManager.IsDuplicate(event.GetMessageId(), 0)
			if err != nil {
				errorCount++
				continue
			}
			if isDuplicate {
				duplicateCount++
				continue
			}
		}

		partitionEvents[partition.ID] = append(partitionEvents[partition.ID], event)
	}

	// Batch write to each partition's WAL and schedule
	for partitionID, events := range partitionEvents {
		partitionInternal, err := h.partitionManager.GetInternalPartition(partitionID)
		if err != nil {
			errorCount += int32(len(events))
			continue
		}

		// Batch append to WAL (single syscall for all events)
		if err := partitionInternal.Wal.AppendBatch(events); err != nil {
			errorCount += int32(len(events))
			continue
		}

		// Batch schedule all events (single lock acquisition)
		if err := partitionInternal.Scheduler.ScheduleBatch(events); err != nil {
			// Events are in WAL, just log scheduling error
			continue
		}

		// Update stats
		for _, event := range events {
			publishedCount++

			if firstOffset == -1 || event.Offset < firstOffset {
				firstOffset = event.Offset
			}
			if event.Offset > lastOffset {
				lastOffset = event.Offset
			}
		}
	}

	return &types.PublishBatchResponse{
		Success:        errorCount == 0 && duplicateCount == 0,
		PublishedCount: publishedCount,
		DuplicateCount: duplicateCount,
		ErrorCount:     errorCount,
		FirstOffset:    firstOffset,
		LastOffset:     lastOffset,
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
		MaxCredits:    10000, // High credit limit for throughput
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
				slog.Warn("Failed to unsubscribe", "subscription_id", subID, "error", err)
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

		// Extract partition ID from delivery ID (format: "subID-offset")
		// We need to find the right dispatcher to return credits
		// For now, notify all dispatchers (they will ignore unknown delivery IDs)
		for _, p := range h.partitionManager.ListPartitions() {
			if p.Dispatcher != nil {
				p.Dispatcher.HandleAck(req.GetDeliveryId(), req.GetSuccess(), req.GetNextOffset())
			}
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

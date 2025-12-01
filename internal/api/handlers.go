package api

import (
	"context"
	"fmt"
	"time"

	"cronos_db/internal/consumer"
	"cronos_db/internal/delivery"
	"cronos_db/internal/partition"
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

	// Get internal partition
	partition, err := h.partitionManager.GetPartitionForTopic(event.Topic)
	if err != nil {
		return &types.PublishResponse{
			Success: false,
			Error:   fmt.Sprintf("get partition: %v", err),
		}, nil
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

	// Append to WAL
	if err := partitionInternal.Wal.AppendEvent(event); err != nil {
		return &types.PublishResponse{
			Success: false,
			Error:   fmt.Sprintf("append to WAL: %v", err),
		}, nil
	}

	// Flush to ensure data is persisted
	if err := partitionInternal.Wal.Flush(); err != nil {
		return &types.PublishResponse{
			Success: false,
			Error:   fmt.Sprintf("flush WAL: %v", err),
		}, nil
	}

	// Schedule event
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

// GRPCStreamWrapper wraps gRPC stream to implement delivery.Stream
type GRPCStreamWrapper struct {
	stream grpc.BidiStreamingServer[types.SubscribeRequest, types.Delivery]
}

func (s *GRPCStreamWrapper) Send(delivery *delivery.DeliveryMessage) error {
	return s.stream.Send(&types.Delivery{
		DeliveryId: delivery.DeliveryID,
		Event:      delivery.Event,
	})
}

func (s *GRPCStreamWrapper) Recv() (*delivery.Control, error) {
	// Not implemented for this simplified wrapper as we handle control messages via separate methods or callbacks
	// Ideally, this should receive messages from the stream
	return nil, nil
}

func (s *GRPCStreamWrapper) Context() context.Context {
	return s.stream.Context()
}

// Subscribe handles streaming subscription
func (h *EventServiceHandler) Subscribe(stream grpc.BidiStreamingServer[types.SubscribeRequest, types.Delivery]) error {
	// Receive subscription request
	req, err := stream.Recv()
	if err != nil {
		return err
	}

	// Get internal partition
	partitionInternal, err := h.partitionManager.GetInternalPartition(req.GetPartitionId())
	if err != nil {
		return fmt.Errorf("get partition %d: %w", req.GetPartitionId(), err)
	}

	// Get consumer group offset
	startOffset, err := h.consumerManager.GetCommittedOffset(req.GetConsumerGroup(), req.GetPartitionId())
	if err != nil {
		startOffset = -1 // Start from beginning if no offset
	}

	// Create subscription
	sub := &delivery.Subscription{
		ID:            fmt.Sprintf("%s-%s-%d", req.GetConsumerGroup(), req.Topic, time.Now().UnixNano()),
		ConsumerGroup: req.GetConsumerGroup(),
		Partition: &types.Partition{
			ID:    partitionInternal.ID,
			Topic: partitionInternal.Topic,
		},
		NextOffset: startOffset + 1,
		MaxCredits: 1000, // Default credits
		Stream:     &GRPCStreamWrapper{stream: stream},
		CreatedTS:  time.Now().UnixMilli(),
	}

	// Register with dispatcher
	if err := partitionInternal.Dispatcher.Subscribe(sub); err != nil {
		return fmt.Errorf("subscribe: %w", err)
	}
	defer partitionInternal.Dispatcher.Unsubscribe(sub.ID)

	// Replay historical events from WAL
	// Note: We only replay events that are already "due" (schedule_ts <= now).
	// Future events will be handled by the Scheduler -> Dispatcher flow when they fire.
	go func() {
		highWatermark := partitionInternal.Wal.GetHighWatermark()
		currentOffset := startOffset + 1
		if currentOffset < 0 {
			currentOffset = 0
		}

		if currentOffset <= highWatermark {
			events, err := partitionInternal.Wal.ReadEvents(currentOffset, highWatermark)
			if err != nil {
				// Log error (in a real system)
				return
			}

			for _, event := range events {
				// Only replay events that should have already fired
				if event.GetScheduleTs() > time.Now().UnixMilli() {
					continue
				}

				// Use Dispatcher delivery ID format
				delivery := &types.Delivery{
					DeliveryId: fmt.Sprintf("%d:%s:%d", partitionInternal.ID, sub.ID, event.Offset),
					Event:      event,
				}
				if err := stream.Send(delivery); err != nil {
					return
				}
			}
		}
	}()

	// Wait for context cancellation
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

		// Update consumer manager
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

		// Notify Dispatcher about Ack
		if req.DeliveryId != "" {
			// Parse PartitionID from DeliveryID (Format: PartitionID:SubscriptionID:Offset)
			var partitionID int32
			_, err := fmt.Sscanf(req.DeliveryId, "%d:", &partitionID)
			if err == nil {
				partitionInternal, err := h.partitionManager.GetInternalPartition(partitionID)
				if err == nil {
					partitionInternal.Dispatcher.HandleAck(req.DeliveryId, true, req.NextOffset)
				}
			}
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
	if req.GetPartitionId() >= 0 {
		_, err := h.partitionManager.GetPartition(req.GetPartitionId())
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

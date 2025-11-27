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

	partitionManager  *partition.PartitionManager
	dedupManager      DedupManager
	consumerManager   ConsumerManager
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
		DeliveryId: delivery.DeliveryID,
		Event:      delivery.Event,
		Attempt:    delivery.Attempt,
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
	subID := fmt.Sprintf("%s-%d-%s", req.GetConsumerGroup(), partitionID, req.GetSubscriptionId())

	// Create channel for deliveries
	deliveryChan := make(chan *types.Delivery, 100)

	// Create subscription object for dispatcher
	subscription := &delivery.Subscription{
		ID:             subID,
		ConsumerGroup:  req.GetConsumerGroup(),
		Partition:      &types.Partition{ID: int32(partitionID)},
		NextOffset:     startOffset + 1,
		MaxCredits:     100,
		CreatedTS:      time.Now().UnixMilli(),
		Stream:         &GRPCStream{stream: stream},
	}

	// Register subscription with partition's dispatcher
	if partitionInternal.Dispatcher != nil {
		if err := partitionInternal.Dispatcher.Subscribe(subscription); err != nil {
			return fmt.Errorf("register subscription: %w", err)
		}
	}

	// Start delivery goroutine to receive events from worker
	go func() {
		for {
			select {
			case <-stream.Context().Done():
				return
			case delivery := <-deliveryChan:
				// Send to subscriber
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

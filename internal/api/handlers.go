package api

	
import (
	"context"
	"fmt"
	"time"

	"cronos_db/internal/consumer"
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

	// Start delivery goroutine
	go func() {
		// Get start and end offsets
		highWatermark := partitionInternal.Wal.GetHighWatermark()
		currentOffset := startOffset + 1
		if currentOffset < 0 {
			currentOffset = 0
		}

		// Read events from WAL
		for currentOffset <= highWatermark {
			select {
			case <-stream.Context().Done():
				return
			default:
			}

			// Read event from WAL (for now, read in batches)
			events, err := partitionInternal.Wal.ReadEvents(currentOffset, highWatermark)
			if err != nil {
				// Log error and continue
				continue
			}

			// Send each event
			for _, event := range events {
				// Skip if already delivered
				if event.Offset < currentOffset {
					continue
				}

				currentOffset = event.Offset + 1

				// Create delivery
				delivery := &types.Delivery{
					DeliveryId: fmt.Sprintf("%s-%d-%d", event.GetMessageId(), event.Offset, time.Now().UnixNano()),
					Event:      event,
				}

				// Send to client
				if err := stream.Send(delivery); err != nil {
					// Client disconnected
					return
				}
			}

			// Update high watermark
			highWatermark = partitionInternal.Wal.GetHighWatermark()
		}

		// Keep reading new events
		for {
			select {
			case <-stream.Context().Done():
				return
			default:
			}

			// Check for new events
			newHighWatermark := partitionInternal.Wal.GetHighWatermark()
			if newHighWatermark >= currentOffset {
				events, err := partitionInternal.Wal.ReadEvents(currentOffset, newHighWatermark)
				if err != nil {
					time.Sleep(100 * time.Millisecond)
					continue
				}

				for _, event := range events {
					delivery := &types.Delivery{
						DeliveryId: fmt.Sprintf("%s-%d-%d", event.GetMessageId(), event.Offset, time.Now().UnixNano()),
						Event:      event,
					}

					if err := stream.Send(delivery); err != nil {
						return
					}
					currentOffset = event.Offset + 1
				}
			} else {
				// No new events, wait a bit
				time.Sleep(100 * time.Millisecond)
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

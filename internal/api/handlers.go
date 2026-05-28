package api

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/internal/audit"
	"github.com/jatin711-debug/cronos_db_golang/internal/auth"
	"github.com/jatin711-debug/cronos_db_golang/internal/consumer"
	"github.com/jatin711-debug/cronos_db_golang/internal/delivery"
	"github.com/jatin711-debug/cronos_db_golang/internal/partition"
	"github.com/jatin711-debug/cronos_db_golang/internal/replay"
	"github.com/jatin711-debug/cronos_db_golang/internal/schema"
	"github.com/jatin711-debug/cronos_db_golang/internal/tenant"
	"github.com/jatin711-debug/cronos_db_golang/internal/tracing"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// EventServiceHandler implements the EventService handler
type EventServiceHandler struct {
	types.UnimplementedEventServiceServer

	partitionManager *partition.PartitionManager
	dedupManager     DedupManager
	consumerManager  ConsumerManager
	clusterRouter    ClusterRouter // nil in standalone mode
	schemaRegistry   *schema.Registry
	tenantAccountant *tenant.Accountant
	auditLogger      *audit.Logger
}

// DedupManager interface
type DedupManager interface {
	IsDuplicate(messageID string, offset int64) (bool, error)
	IsDuplicateBatch(messageIDs []string, offsets []int64) ([]bool, error)
}

// ConsumerManager interface
type ConsumerManager interface {
	Subscribe(request *types.SubscribeRequest) (*consumer.Subscription, error)
	Ack(request *types.AckRequest) error
	GetCommittedOffset(groupID string, partitionID int32) (int64, error)
	LeaveGroup(groupID, memberID string) error
}

// ClusterRouter provides cluster-aware partition routing.
// When non-nil, the handler checks partition locality before processing.
type ClusterRouter interface {
	IsLocalPartition(partitionID int32) bool
	IsPartitionLeader(partitionID int32) bool
	GetPartitionEpoch(partitionID int32) int64
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
		Batch:        delivery.Batch,
	})
}

// Recv receives a control message from the subscriber.
// Currently not implemented — control messages (flow-control credits, etc.)
// are handled via the separate Ack streaming endpoint.
func (s *GRPCStream) Recv() (*delivery.Control, error) {
	return nil, fmt.Errorf("control message streaming not implemented: use the Ack endpoint for credits and flow control")
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

// SetClusterRouter sets the cluster router for partition-aware request routing.
// When set, Publish/Subscribe will reject requests for non-local partitions.
func (h *EventServiceHandler) SetClusterRouter(router ClusterRouter) {
	h.clusterRouter = router
}

// SetSchemaRegistry sets the schema registry for publish validation.
func (h *EventServiceHandler) SetSchemaRegistry(r *schema.Registry) {
	h.schemaRegistry = r
}

// SetTenantAccountant sets the tenant resource accountant.
func (h *EventServiceHandler) SetTenantAccountant(a *tenant.Accountant) {
	h.tenantAccountant = a
}

// SetAuditLogger sets the audit logger for handler-level events.
func (h *EventServiceHandler) SetAuditLogger(l *audit.Logger) {
	h.auditLogger = l
}

// ensureClusterPartitionWritable validates that this node should accept writes
// for the target partition when running in cluster mode.
func (h *EventServiceHandler) ensureClusterPartitionWritable(partitionID int32) error {
	if h.clusterRouter == nil {
		return nil
	}

	if !h.clusterRouter.IsLocalPartition(partitionID) {
		return status.Errorf(codes.Unavailable,
			"partition %d is not owned by this node; retry against the partition leader", partitionID)
	}

	if !h.clusterRouter.IsPartitionLeader(partitionID) {
		return status.Errorf(codes.FailedPrecondition,
			"partition %d is local but this node is not the leader; retry against the partition leader", partitionID)
	}

	// Epoch fencing: reject writes if local partition epoch is behind cluster epoch.
	// This prevents a split-brain scenario where an old leader hasn't realized
	// it was demoted and continues accepting writes.
	clusterEpoch := h.clusterRouter.GetPartitionEpoch(partitionID)
	localEpoch := h.partitionManager.GetPartitionEpoch(partitionID)
	if localEpoch < clusterEpoch {
		return status.Errorf(codes.FailedPrecondition,
			"partition %d epoch mismatch: local=%d cluster=%d; possible stale leader, retry", partitionID, localEpoch, clusterEpoch)
	}

	return nil
}

// Publish handles publish requests
func (h *EventServiceHandler) Publish(ctx context.Context, req *types.PublishRequest) (*types.PublishResponse, error) {
	ctx, span := tracing.StartSpan(ctx, "Publish")
	if span != nil {
		defer span.End()
	}

	event := req.Event

	// Validate event
	if event.GetMessageId() == "" {
		return &types.PublishResponse{
			Success: false,
			Error:   "message_id is required",
		}, nil
	}
	if len(event.GetMessageId()) > 128 {
		return &types.PublishResponse{
			Success: false,
			Error:   "message_id exceeds 128 characters",
		}, nil
	}
	if len(event.Topic) > 255 {
		return &types.PublishResponse{
			Success: false,
			Error:   "topic exceeds 255 characters",
		}, nil
	}
	if len(event.Payload) > 4*1024*1024 {
		return &types.PublishResponse{
			Success: false,
			Error:   "payload exceeds 4MB limit",
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

	// Schema validation
	if h.schemaRegistry != nil && event.Topic != "" {
		if err := h.schemaRegistry.Validate(event.Topic, event.Payload); err != nil {
			return &types.PublishResponse{
				Success: false,
				Error:   fmt.Sprintf("schema validation failed: %v", err),
			}, nil
		}
	}

	partitionKey := event.GetMessageId() // Default to message_id for distribution
	if pk, ok := event.Meta["partition_key"]; ok && pk != "" {
		partitionKey = pk
	}
	partitionID := h.partitionManager.GetPartitionIDForKey(partitionKey)
	if err := h.ensureClusterPartitionWritable(partitionID); err != nil {
		return nil, err
	}

	// Admission control: reject if partition is overloaded
	if !h.partitionManager.CanAccept(partitionID) {
		IncAdmissionRejected()
		return nil, status.Errorf(codes.ResourceExhausted,
			"partition %d is at capacity; retry with backoff", partitionID)
	}

	partitionInternal, err := h.partitionManager.GetOrCreateInternalPartition(partitionID, partitionKey)
	if err != nil {
		// Fallback to topic-based partitioning
		topicPartitionID := h.partitionManager.GetPartitionIDForTopic(event.Topic)
		if ownerErr := h.ensureClusterPartitionWritable(topicPartitionID); ownerErr != nil {
			return nil, ownerErr
		}

		// Check admission on fallback partition too
		if !h.partitionManager.CanAccept(topicPartitionID) {
			IncAdmissionRejected()
			return nil, status.Errorf(codes.ResourceExhausted,
				"partition %d is at capacity; retry with backoff", topicPartitionID)
		}

		partitionInternal, err = h.partitionManager.GetOrCreateInternalPartition(topicPartitionID, event.Topic)
		if err != nil {
			return &types.PublishResponse{
				Success: false,
				Error:   fmt.Sprintf("get partition: %v", err),
			}, nil
		}
	}

	// Check if duplicate (unless explicitly allowed)
	if !req.AllowDuplicate {
		if h.dedupManager == nil {
			return nil, status.Error(codes.Unavailable, "dedup manager not initialized on this node")
		}

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

	// Tenant quota check
	if h.tenantAccountant != nil {
		tenantID := tenant.ID("default")
		if claims, ok := auth.ClaimsFromContext(ctx); ok {
			tenantID = tenant.ID(claims.Subject)
		}
		if !h.tenantAccountant.AllowPublish(tenantID) {
			return nil, status.Errorf(codes.ResourceExhausted, "tenant quota exceeded")
		}
		h.tenantAccountant.RecordPublish(tenantID, int64(len(event.Payload)))
		if event.Meta == nil {
			event.Meta = make(map[string]string)
		}
		event.Meta["tenant_id"] = string(tenantID)
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
		PartitionId: partitionInternal.ID,
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

	if !req.AllowDuplicate && h.dedupManager == nil {
		return nil, status.Error(codes.Unavailable, "dedup manager not initialized on this node")
	}

	var publishedCount, duplicateCount, errorCount int32
	var firstOffset, lastOffset int64 = -1, -1
	var lastError string

	// Group events by partition for batch WAL writes
	partitionEvents := make(map[int32][]*types.Event)

	for _, event := range req.Events {
		// Basic validation
		if event.GetMessageId() == "" || event.GetScheduleTs() <= 0 || len(event.Payload) == 0 {
			atomic.AddInt32(&errorCount, 1)
			if lastError == "" {
				lastError = fmt.Sprintf("validation failed: msgId=%q, scheduleTs=%d, payloadLen=%d",
					event.GetMessageId(), event.GetScheduleTs(), len(event.Payload))
			}
			continue
		}

		// Schema validation
		if h.schemaRegistry != nil && event.Topic != "" {
			if err := h.schemaRegistry.Validate(event.Topic, event.Payload); err != nil {
				atomic.AddInt32(&errorCount, 1)
				if lastError == "" {
					lastError = fmt.Sprintf("schema validation failed for %s: %v", event.GetMessageId(), err)
				}
				continue
			}
		}

		// Get partition
		partitionKey := event.GetMessageId()
		if pk, ok := event.Meta["partition_key"]; ok && pk != "" {
			partitionKey = pk
		}

		partitionID := h.partitionManager.GetPartitionIDForKey(partitionKey)
		if ownerErr := h.ensureClusterPartitionWritable(partitionID); ownerErr != nil {
			atomic.AddInt32(&errorCount, 1)
			if lastError == "" {
				lastError = ownerErr.Error()
			}
			continue
		}

		// Admission control
		if !h.partitionManager.CanAccept(partitionID) {
			IncAdmissionRejected()
			atomic.AddInt32(&errorCount, 1)
			if lastError == "" {
				lastError = fmt.Sprintf("partition %d is at capacity", partitionID)
			}
			continue
		}

		// Check dedup
		if !req.AllowDuplicate {
			isDuplicate, err := h.dedupManager.IsDuplicate(event.GetMessageId(), 0)
			if err != nil {
				atomic.AddInt32(&errorCount, 1)
				if lastError == "" {
					lastError = fmt.Sprintf("dedup check failed for %s: %v", event.GetMessageId(), err)
				}
				continue
			}
			if isDuplicate {
				atomic.AddInt32(&duplicateCount, 1)
				continue
			}
		}

		// Tenant quota check
		if h.tenantAccountant != nil {
			tenantID := tenant.ID("default")
			if claims, ok := auth.ClaimsFromContext(ctx); ok {
				tenantID = tenant.ID(claims.Subject)
			}
			if !h.tenantAccountant.AllowPublish(tenantID) {
				atomic.AddInt32(&errorCount, 1)
				if lastError == "" {
					lastError = "tenant quota exceeded"
				}
				continue
			}
			h.tenantAccountant.RecordPublish(tenantID, int64(len(event.Payload)))
			if event.Meta == nil {
				event.Meta = make(map[string]string)
			}
			event.Meta["tenant_id"] = string(tenantID)
		}

		partitionEvents[partitionID] = append(partitionEvents[partitionID], event)
	}

	// Parallel batch write to each partition's WAL and schedule
	var wg sync.WaitGroup
	var mu sync.Mutex // protects firstOffset, lastOffset, lastError

	for partitionID, events := range partitionEvents {
		wg.Add(1)
		go func(pid int32, evts []*types.Event) {
			defer wg.Done()

			partitionInternal, err := h.partitionManager.GetOrCreateInternalPartition(pid, evts[0].Topic)
			if err != nil {
				// Fallback to topic-based partitioning
				topicPartitionID := h.partitionManager.GetPartitionIDForTopic(evts[0].Topic)
				if ownerErr := h.ensureClusterPartitionWritable(topicPartitionID); ownerErr != nil {
					atomic.AddInt32(&errorCount, int32(len(evts)))
					mu.Lock()
					if lastError == "" {
						lastError = fmt.Sprintf("get internal partition %d: %v", pid, err)
					}
					mu.Unlock()
					return
				}
				partitionInternal, err = h.partitionManager.GetOrCreateInternalPartition(topicPartitionID, evts[0].Topic)
				if err != nil {
					atomic.AddInt32(&errorCount, int32(len(evts)))
					mu.Lock()
					if lastError == "" {
						lastError = fmt.Sprintf("get internal partition %d: %v", topicPartitionID, err)
					}
					mu.Unlock()
					return
				}
			}

			// Batch append to WAL (single syscall for all events)
			if err := partitionInternal.Wal.AppendBatch(evts); err != nil {
				atomic.AddInt32(&errorCount, int32(len(evts)))
				mu.Lock()
				if lastError == "" {
					lastError = fmt.Sprintf("WAL append for partition %d: %v", pid, err)
				}
				mu.Unlock()
				return
			}

			// Batch schedule all events (single lock acquisition)
			if err := partitionInternal.Scheduler.ScheduleBatch(evts); err != nil {
				// Events are in WAL, just log scheduling error
				slog.Warn("batch schedule partially failed", "partition", pid, "count", len(evts), "error", err)
			}

			// Update stats
			localPublished := int32(len(evts))
			atomic.AddInt32(&publishedCount, localPublished)

			mu.Lock()
			for _, event := range evts {
				if firstOffset == -1 || event.Offset < firstOffset {
					firstOffset = event.Offset
				}
				if event.Offset > lastOffset {
					lastOffset = event.Offset
				}
			}
			mu.Unlock()
		}(partitionID, events)
	}

	wg.Wait()

	// Log errors periodically to help debug
	if errorCount > 0 && errorCount%1000 == 0 {
		slog.Warn("batch publish errors",
			"errorCount", errorCount,
			"duplicateCount", duplicateCount,
			"lastError", lastError)
	}

	return &types.PublishBatchResponse{
		Success:        errorCount == 0 && duplicateCount == 0,
		Error:          lastError,
		PublishedCount: publishedCount,
		DuplicateCount: duplicateCount,
		ErrorCount:     errorCount,
		FirstOffset:    firstOffset,
		LastOffset:     lastOffset,
	}, nil
}

// Subscribe handles streaming subscription
func (h *EventServiceHandler) Subscribe(stream grpc.BidiStreamingServer[types.SubscribeRequest, types.Delivery]) error {
	ctx, span := tracing.StartSpan(stream.Context(), "Subscribe")
	if span != nil {
		defer span.End()
	}
	_ = ctx

	if h.consumerManager == nil {
		return status.Error(codes.Unavailable, "consumer manager not initialized on this node")
	}

	// Receive subscription request
	req, err := stream.Recv()
	if err != nil {
		return err
	}

	// Handle partition auto-assignment
	partitionID := req.GetPartitionId()
	if partitionID < 0 {
		// Compute partition first so cluster checks do not trigger remote auto-creation.
		partitionID = h.partitionManager.GetPartitionIDForTopic(req.GetTopic())
	}

	if err := h.ensureClusterPartitionWritable(partitionID); err != nil {
		return err
	}

	if req.GetPartitionId() < 0 {
		// Ensure local state exists for auto-assigned partition.
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

	// Determine credit limit from request or use default
	maxCredits := req.GetMaxBufferSize()
	if maxCredits <= 0 {
		maxCredits = 10000 // Default high credit limit for throughput
	}
	if maxCredits > 50000 {
		maxCredits = 50000 // Hard cap to prevent memory abuse
	}

	// Create subscription object for dispatcher
	subscription := &delivery.Subscription{
		ID:            subID,
		ConsumerGroup: req.GetConsumerGroup(),
		Partition:     &types.Partition{ID: int32(partitionID)},
		NextOffset:    startOffset + 1,
		MaxCredits:    maxCredits,
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
	if _, err := h.consumerManager.Subscribe(req); err != nil {
		return fmt.Errorf("create consumer group: %w", err)
	}
	defer func() {
		if err := h.consumerManager.LeaveGroup(req.GetConsumerGroup(), req.GetSubscriptionId()); err != nil {
			slog.Warn("Failed to leave consumer group", "group", req.GetConsumerGroup(), "member", req.GetSubscriptionId(), "error", err)
		}
	}()

	// Wait for context cancellation (client disconnect)
	<-stream.Context().Done()
	return nil
}

// Ack handles streaming ack requests
func (h *EventServiceHandler) Ack(stream types.EventService_AckServer) error {
	ctx, span := tracing.StartSpan(stream.Context(), "Ack")
	if span != nil {
		defer span.End()
	}
	_ = ctx

	if h.consumerManager == nil {
		return status.Error(codes.Unavailable, "consumer manager not initialized on this node")
	}

	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}

		// Parse partition ID from delivery ID to route to the correct dispatcher.
		// Delivery ID format: "consumerGroup:partitionID:memberID-offset"
		// or batch format: "consumerGroup:partitionID:memberID-batch-offset-count"
		var targetPartitionID int32 = -1
		deliveryID := req.GetDeliveryId()
		parts := strings.Split(deliveryID, ":")
		if len(parts) >= 2 {
			if pid, err := strconv.ParseInt(parts[1], 10, 32); err == nil {
				targetPartitionID = int32(pid)
			}
		}

		if targetPartitionID >= 0 {
			// Fast path: route directly to the target partition's dispatcher
			p, err := h.partitionManager.GetInternalPartition(targetPartitionID)
			if err == nil && p.Dispatcher != nil {
				p.Dispatcher.HandleAck(deliveryID, req.GetSuccess(), req.GetNextOffset())
			}
		} else {
			// Skip dispatcher routing for malformed/legacy IDs to avoid O(partitions)
			// scans on the ack hot path. Consumer offset commit below will validate
			// delivery_id format and return a proper error when invalid.
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

	// Follower reads: if enabled, allow replay on any node that has the partition,
	// not just the leader. This offloads read traffic from leaders.
	if h.clusterRouter != nil {
		if !h.clusterRouter.IsLocalPartition(partitionID) {
			return status.Errorf(codes.Unavailable,
				"partition %d is not owned by this node", partitionID)
		}
		// If follower reads disabled, require leader
		if !h.clusterRouter.IsPartitionLeader(partitionID) {
			// Check if follower reads are enabled globally via partition manager config
			if pm, _ := h.partitionManager.GetPartition(partitionID); pm == nil || !h.partitionManager.CanAccept(partitionID) {
				return status.Errorf(codes.FailedPrecondition,
					"partition %d is not leader; follower reads not enabled", partitionID)
			}
		}
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

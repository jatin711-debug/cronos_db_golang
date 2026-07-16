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
	"github.com/jatin711-debug/cronos_db_golang/internal/metrics"
	"github.com/jatin711-debug/cronos_db_golang/internal/partition"
	"github.com/jatin711-debug/cronos_db_golang/internal/replay"
	"github.com/jatin711-debug/cronos_db_golang/internal/schema"
	"github.com/jatin711-debug/cronos_db_golang/internal/tenant"
	"github.com/jatin711-debug/cronos_db_golang/internal/tracing"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
	"github.com/jatin711-debug/cronos_db_golang/pkg/utils"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// durableAckEnabled returns true if the event explicitly requests a durable fsync
// before the publish response is returned. This lets clients opt into stronger
// durability guarantees per event without changing the gRPC API.
func durableAckEnabled(e *types.Event) bool {
	if e == nil || e.Meta == nil {
		return false
	}
	return e.Meta["cronos.durable_ack"] == "true"
}

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
	authPolicy       *auth.Policy
	authEnabled      bool // true when JWT authentication is active
}

// DedupManager interface
type DedupManager interface {
	IsDuplicate(messageID string, offset int64) (bool, error)
	IsDuplicateBatch(messageIDs []string, offsets []int64) ([]bool, error)
	// RollbackBatch undoes dedup claims when the durable write that followed the
	// claim failed, so a client retry is not dropped as a spurious duplicate.
	RollbackBatch(messageIDs []string) error
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

// dedupManagerForPartition returns only the dedup store owned by the target
// partition. Falling back to partition 0 is unsafe: in lazy cluster mode it
// can record a message ID in the wrong namespace before the real partition is
// materialized, allowing a retry to be accepted by the correct store.
func (h *EventServiceHandler) dedupManagerForPartition(partitionID int32) DedupManager {
	if p, err := h.partitionManager.GetInternalPartition(partitionID); err == nil && p != nil && p.DedupStore != nil {
		return p.DedupStore
	}
	return nil
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

// SetAuthPolicy sets the RBAC policy for topic-level authorization.
// Call this only when auth is enabled. When auth is disabled, leave
// authPolicy nil and authEnabled false so topic checks are skipped.
func (h *EventServiceHandler) SetAuthPolicy(p *auth.Policy) {
	h.authPolicy = p
	h.authEnabled = true
}

// checkTopicAuth is a thin wrapper around auth.CheckTopicPermission that skips
// the check entirely when auth is disabled (dev mode). When auth is enabled,
// a nil policy is treated as a misconfiguration and fails closed.
func (h *EventServiceHandler) checkTopicAuth(ctx context.Context, topic string, op string) error {
	if !h.authEnabled {
		return nil // auth disabled — allow all
	}
	return auth.CheckTopicPermission(ctx, topic, op, h.authPolicy)
}

// ensureClusterPartitionWritable validates that this node should accept writes
// for the target partition when running in cluster mode or under active splits.
func (h *EventServiceHandler) ensureClusterPartitionWritable(partitionID int32) error {
	if h.partitionManager.IsSplitting(partitionID) {
		return status.Errorf(codes.Unavailable,
			"partition %d is undergoing split, retry later", partitionID)
	}

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

	// Topic-level authorization
	if err := h.checkTopicAuth(ctx, event.Topic, "publish"); err != nil {
		return nil, err
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
		metrics.IncAdmissionRejected()
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
			metrics.IncAdmissionRejected()
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
		partitionID = topicPartitionID
	}

	if !partitionInternal.IsKeyInBounds(partitionKey) {
		return &types.PublishResponse{
			Success: false,
			Error:   fmt.Sprintf("key %s is out of partition bounds [%s, %s)", partitionKey, partitionInternal.MinKey, partitionInternal.MaxKey),
		}, nil
	}

	// Check if duplicate (unless explicitly allowed). dedupMgr is kept in scope so
	// the claim can be rolled back if the durable WAL append below fails.
	var dedupMgr DedupManager
	if !req.AllowDuplicate {
		dedupMgr = h.dedupManagerForPartition(partitionID)
		if dedupMgr == nil {
			return nil, status.Error(codes.Unavailable, "dedup manager not initialized on this node")
		}

		isDuplicate, err := dedupMgr.IsDuplicate(event.GetMessageId(), 0) // offset will be assigned
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
	if h.tenantAccountant != nil && h.tenantAccountant.HasConfiguredLimits() {
		tenantID := tenant.ID("default")
		if claims, ok := auth.ClaimsFromContext(ctx); ok {
			tenantID = tenant.ID(claims.Subject)
		}
		if !h.tenantAccountant.ReservePublishBatch(tenantID, 1, int64(len(event.Payload))) {
			return nil, status.Errorf(codes.ResourceExhausted, "tenant quota exceeded")
		}
		if event.Meta == nil {
			event.Meta = make(map[string]string)
		}
		event.Meta["tenant_id"] = string(tenantID)
	}

	// Append to WAL (sync behavior depends on fsync mode; durable_ack forces an
	// fsync). rollbackDedup undoes the dedup claim if the event never became
	// durable, so a client retry of this message ID is not silently dropped as a
	// duplicate. Failures AFTER a successful append (schedule, etc.) intentionally
	// keep the claim, since the event is in the WAL and will be recovered on replay.
	rollbackDedup := func() {
		if dedupMgr == nil {
			return
		}
		if rbErr := dedupMgr.RollbackBatch([]string{event.GetMessageId()}); rbErr != nil {
			slog.Warn("dedup rollback after WAL append failure failed",
				"messageId", event.GetMessageId(), "error", rbErr)
		}
	}

	// On a replication leader, append and replication must occur together in
	// strict offset order; serialize them under the partition's ReplicateMu.
	// RF=1 / non-leader partitions keep the fast path with no extra locking.
	if repl := partitionInternal.ReplLeader; repl != nil {
		partitionInternal.ReplicateMu.Lock()
		if err := partitionInternal.Wal.AppendEvent(event); err != nil {
			partitionInternal.ReplicateMu.Unlock()
			rollbackDedup()
			return &types.PublishResponse{
				Success: false,
				Error:   fmt.Sprintf("append to WAL: %v", err),
			}, nil
		}
		replErr := repl.Replicate([]*types.Event{event})
		partitionInternal.ReplicateMu.Unlock()
		if replErr != nil {
			// Durable locally but not replicated to the required in-sync replicas.
			// Report failure; keep the dedup claim (event is in the WAL).
			return &types.PublishResponse{
				Success: false,
				Error:   fmt.Sprintf("replication: %v", replErr),
			}, nil
		}
	} else if err := partitionInternal.Wal.AppendEvent(event); err != nil {
		rollbackDedup()
		return &types.PublishResponse{
			Success: false,
			Error:   fmt.Sprintf("append to WAL: %v", err),
		}, nil
	}
	if durableAckEnabled(event) {
		if err := partitionInternal.Wal.Flush(); err != nil {
			return &types.PublishResponse{
				Success: false,
				Error:   fmt.Sprintf("durable fsync: %v", err),
			}, nil
		}
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

// anyDurableAck returns true if any event in the batch requests a durable fsync.
func anyDurableAck(events []*types.Event) bool {
	for _, e := range events {
		if durableAckEnabled(e) {
			return true
		}
	}
	return false
}

// partitionEventsPool recycles the map[int32][]*types.Event used to group events
// by partition in PublishBatch. This removes one allocation per publish request.
var partitionEventsPool = sync.Pool{
	New: func() interface{} {
		return make(map[int32][]*types.Event, 8)
	},
}

func acquirePartitionEventsMap() map[int32][]*types.Event {
	m := partitionEventsPool.Get().(map[int32][]*types.Event)
	// Maps from the pool are returned empty, but guard against misuse.
	for k := range m {
		delete(m, k)
	}
	return m
}

func releasePartitionEventsMap(m map[int32][]*types.Event) {
	for k := range m {
		delete(m, k)
	}
	partitionEventsPool.Put(m)
}

// PublishBatch handles batch publish requests for high-throughput ingestion
func (h *EventServiceHandler) PublishBatch(ctx context.Context, req *types.PublishBatchRequest) (*types.PublishBatchResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "publish batch request is required")
	}
	if len(req.Events) == 0 {
		return &types.PublishBatchResponse{
			Success: true,
		}, nil
	}

	var publishedCount, duplicateCount, errorCount int32
	var firstOffset, lastOffset int64 = -1, -1
	var lastError string
	var resultMu sync.Mutex

	setError := func(count int32, message string) {
		if count <= 0 {
			return
		}
		atomic.AddInt32(&errorCount, count)
		resultMu.Lock()
		if lastError == "" {
			lastError = message
		}
		resultMu.Unlock()
	}

	partitionKeyOf := func(event *types.Event) string {
		key := event.GetMessageId()
		if pk, ok := event.Meta["partition_key"]; ok && pk != "" {
			key = pk
		}
		return key
	}

	// Group events by partition for batch WAL writes. Reuse a pooled map to
	// avoid one allocation per request.
	partitionEvents := acquirePartitionEventsMap()
	defer releasePartitionEventsMap(partitionEvents)

	// Authorization is invariant for all events with the same topic and request
	// context. Avoid taking the policy locks once per event in a large batch.
	authResults := make(map[string]error, 4)

	for i, event := range req.Events {
		if i&255 == 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			default:
			}
		}

		// Basic validation
		if event == nil || event.GetMessageId() == "" || len(event.GetMessageId()) > 128 ||
			event.GetScheduleTs() <= 0 || len(event.Payload) == 0 || len(event.Payload) > 4*1024*1024 || len(event.Topic) > 255 {
			if event == nil {
				setError(1, "validation failed: event is nil")
			} else {
				setError(1, fmt.Sprintf("validation failed: msgId=%q, scheduleTs=%d, payloadLen=%d",
					event.GetMessageId(), event.GetScheduleTs(), len(event.Payload)))
			}
			continue
		}

		// Topic-level authorization
		authErr, checked := authResults[event.Topic]
		if !checked {
			authErr = h.checkTopicAuth(ctx, event.Topic, "publish")
			authResults[event.Topic] = authErr
		}
		if authErr != nil {
			setError(1, authErr.Error())
			continue
		}

		// Schema validation
		if h.schemaRegistry != nil && event.Topic != "" {
			if err := h.schemaRegistry.Validate(event.Topic, event.Payload); err != nil {
				setError(1, fmt.Sprintf("schema validation failed for %s: %v", event.GetMessageId(), err))
				continue
			}
		}

		partitionKey := partitionKeyOf(event)
		partitionID := h.partitionManager.GetPartitionIDForKey(partitionKey)
		partitionEvents[partitionID] = append(partitionEvents[partitionID], event)
	}

	// Materialize and validate each partition exactly once. This also ensures
	// deduplication uses the partition-owned store rather than a node-wide
	// fallback. Bounds and admission are evaluated before any dedup claim so a
	// rejected event does not consume an ID or quota.
	partitionInternals := make(map[int32]*partition.Partition, len(partitionEvents))
	for pid, events := range partitionEvents {
		if ownerErr := h.ensureClusterPartitionWritable(pid); ownerErr != nil {
			setError(int32(len(events)), ownerErr.Error())
			delete(partitionEvents, pid)
			continue
		}

		partitionInternal, err := h.partitionManager.GetOrCreateInternalPartition(pid, partitionKeyOf(events[0]))
		if err != nil {
			setError(int32(len(events)), fmt.Sprintf("get internal partition %d: %v", pid, err))
			delete(partitionEvents, pid)
			continue
		}

		validEvents := events[:0]
		for _, event := range events {
			if !partitionInternal.IsKeyInBounds(partitionKeyOf(event)) {
				setError(1, fmt.Sprintf("key %s is out of partition bounds [%s, %s)", partitionKeyOf(event), partitionInternal.MinKey, partitionInternal.MaxKey))
				continue
			}
			validEvents = append(validEvents, event)
		}
		if len(validEvents) == 0 {
			delete(partitionEvents, pid)
			continue
		}

		if !h.partitionManager.CanAcceptBatch(pid, int64(len(validEvents))) {
			metrics.IncAdmissionRejected()
			setError(int32(len(validEvents)), fmt.Sprintf("partition %d is at capacity", pid))
			delete(partitionEvents, pid)
			continue
		}

		partitionEvents[pid] = validEvents
		partitionInternals[pid] = partitionInternal
	}

	// Tenant accounting is disabled unless at least one tenant has configured
	// limits. In the common unrestricted case this removes a map lookup,
	// atomic increment, and metadata allocation from every event.
	if h.tenantAccountant != nil && h.tenantAccountant.HasConfiguredLimits() && len(partitionEvents) > 0 {
		tenantID := tenant.ID("default")
		if claims, ok := auth.ClaimsFromContext(ctx); ok {
			tenantID = tenant.ID(claims.Subject)
		}
		var eventCount, payloadBytes int64
		for _, events := range partitionEvents {
			eventCount += int64(len(events))
			for _, event := range events {
				payloadBytes += int64(len(event.Payload))
			}
		}
		if !h.tenantAccountant.ReservePublishBatch(tenantID, eventCount, payloadBytes) {
			setError(int32(eventCount), "tenant quota exceeded")
			for pid := range partitionEvents {
				delete(partitionEvents, pid)
			}
		} else {
			for _, events := range partitionEvents {
				for _, event := range events {
					if event.Meta == nil {
						event.Meta = make(map[string]string, 1)
					}
					event.Meta["tenant_id"] = string(tenantID)
				}
			}
		}
	}

	// Batch dedup per partition. This turns N point lookups into one batched
	// bloom-filter + Pebble write-batch operation, which is much cheaper under
	// concurrent publish load. Each partition uses its OWN dedup store (bloom
	// filter + PebbleDB), so dedup throughput scales linearly with partition
	// count instead of funneling through a single global store.
	if !req.AllowDuplicate {
		for pid, evts := range partitionEvents {
			messageIDs := make([]string, len(evts))
			offsets := make([]int64, len(evts))
			for i, e := range evts {
				messageIDs[i] = e.GetMessageId()
				offsets[i] = 0
			}
			dedupMgr := partitionInternals[pid].DedupStore
			if dedupMgr == nil {
				setError(int32(len(evts)), fmt.Sprintf("dedup store not available for partition %d", pid))
				delete(partitionEvents, pid)
				continue
			}
			duplicates, err := dedupMgr.IsDuplicateBatch(messageIDs, offsets)
			if err != nil {
				// Treat a failed dedup check as an error for the whole partition batch.
				setError(int32(len(evts)), fmt.Sprintf("dedup check failed for partition %d: %v", pid, err))
				delete(partitionEvents, pid)
				continue
			}
			if len(duplicates) != len(evts) {
				setError(int32(len(evts)), fmt.Sprintf("dedup check returned %d results for %d events in partition %d", len(duplicates), len(evts), pid))
				delete(partitionEvents, pid)
				continue
			}
			kept := evts[:0]
			for i, e := range evts {
				if duplicates[i] {
					atomic.AddInt32(&duplicateCount, 1)
					continue
				}
				kept = append(kept, e)
			}
			if len(kept) == 0 {
				delete(partitionEvents, pid)
			} else {
				partitionEvents[pid] = kept
			}
		}
	}

	// Parallel batch write to each partition's WAL and schedule
	var wg sync.WaitGroup

	for partitionID, events := range partitionEvents {
		wg.Add(1)
		go func(pid int32, evts []*types.Event) {
			defer wg.Done()

			partitionInternal := partitionInternals[pid]
			if partitionInternal == nil {
				setError(int32(len(evts)), fmt.Sprintf("partition %d was not materialized", pid))
				return
			}

			// rollbackDedup undoes the dedup claims for this batch after a failed
			// durable append so client retries are not dropped as spurious
			// duplicates. Only meaningful when dedup actually claimed them
			// (i.e. !req.AllowDuplicate).
			rollbackDedup := func() {
				if req.AllowDuplicate || partitionInternal.DedupStore == nil {
					return
				}
				ids := make([]string, len(evts))
				for i, e := range evts {
					ids[i] = e.GetMessageId()
				}
				if rbErr := partitionInternal.DedupStore.RollbackBatch(ids); rbErr != nil {
					slog.Warn("dedup rollback after WAL append failure failed",
						"partition", pid, "count", len(ids), "error", rbErr)
				}
			}

			// Batch append to WAL (single syscall for all events). When this
			// partition is a replication leader, the append and the replication to
			// followers must occur together in strict offset order — Leader.Replicate
			// requires contiguous offsets and is not safe to call concurrently out of
			// order. ReplicateMu serializes the two for replicated partitions only;
			// RF=1 / non-leader partitions (ReplLeader == nil) keep the fully
			// pipelined fast path with no extra locking.
			if repl := partitionInternal.ReplLeader; repl != nil {
				partitionInternal.ReplicateMu.Lock()
				if err := partitionInternal.Wal.AppendBatch(evts); err != nil {
					partitionInternal.ReplicateMu.Unlock()
					rollbackDedup()
					setError(int32(len(evts)), fmt.Sprintf("WAL append for partition %d: %v", pid, err))
					return
				}
				replErr := repl.Replicate(evts)
				partitionInternal.ReplicateMu.Unlock()
				if replErr != nil {
					// The batch is durable in the local WAL but did not reach the
					// required in-sync replicas. Surface the failure so the client can
					// retry; do NOT roll back dedup — the event is in the WAL, will be
					// delivered/recovered locally, and a retry is correctly deduped.
					setError(int32(len(evts)), fmt.Sprintf("replication for partition %d: %v", pid, replErr))
					return
				}
			} else if err := partitionInternal.Wal.AppendBatch(evts); err != nil {
				rollbackDedup()
				setError(int32(len(evts)), fmt.Sprintf("WAL append for partition %d: %v", pid, err))
				return
			}

			// Force fsync if any event in the batch requested durable acknowledgement.
			if anyDurableAck(evts) {
				if err := partitionInternal.Wal.Flush(); err != nil {
					setError(int32(len(evts)), fmt.Sprintf("durable fsync for partition %d: %v", pid, err))
					return
				}
			}

			// Batch schedule all events (single lock acquisition)
			if err := partitionInternal.Scheduler.ScheduleBatch(evts); err != nil {
				// The events are in the WAL, but the publish is not complete until
				// they are scheduled. Recovery can replay them, so surface the
				// failure instead of silently reporting success.
				slog.Warn("batch schedule partially failed", "partition", pid, "count", len(evts), "error", err)
				setError(int32(len(evts)), fmt.Sprintf("schedule batch for partition %d: %v", pid, err))
				return
			}

			// Update stats
			localPublished := int32(len(evts))
			atomic.AddInt32(&publishedCount, localPublished)

			resultMu.Lock()
			for _, event := range evts {
				if firstOffset == -1 || event.Offset < firstOffset {
					firstOffset = event.Offset
				}
				if event.Offset > lastOffset {
					lastOffset = event.Offset
				}
			}
			resultMu.Unlock()
		}(partitionID, events)
	}

	wg.Wait()

	// Read counters atomically because partition writers update them in parallel.
	finalPublishedCount := atomic.LoadInt32(&publishedCount)
	finalDuplicateCount := atomic.LoadInt32(&duplicateCount)
	finalErrorCount := atomic.LoadInt32(&errorCount)

	// Log errors periodically to help debug
	if finalErrorCount > 0 && finalErrorCount%1000 == 0 {
		slog.Warn("batch publish errors",
			"errorCount", finalErrorCount,
			"duplicateCount", finalDuplicateCount,
			"lastError", lastError)
	}

	resultMu.Lock()
	responseError := lastError
	responseFirstOffset := firstOffset
	responseLastOffset := lastOffset
	resultMu.Unlock()

	return &types.PublishBatchResponse{
		// Duplicate IDs are an idempotent result, not a failed RPC. This is
		// important because clients must not retry an entire batch when only
		// some IDs were already accepted.
		Success:        finalErrorCount == 0,
		Error:          responseError,
		PublishedCount: finalPublishedCount,
		DuplicateCount: finalDuplicateCount,
		ErrorCount:     finalErrorCount,
		FirstOffset:    responseFirstOffset,
		LastOffset:     responseLastOffset,
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

	// Topic-level authorization
	if err := h.checkTopicAuth(ctx, req.GetTopic(), "subscribe"); err != nil {
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
		parts := strings.SplitN(deliveryID, ":", 3)
		groupID := ""
		if len(parts) >= 2 {
			groupID = parts[0]
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

		if h.partitionManager.ExactlyOnceCommitsEnabled() && targetPartitionID >= 0 && groupID != "" {
			committedOffset, commitErr := h.consumerManager.GetCommittedOffset(groupID, targetPartitionID)
			if commitErr == nil && req.GetNextOffset() <= committedOffset {
				resp := &types.AckResponse{
					Success: false,
					Error:   fmt.Sprintf("next_offset %d must be greater than committed offset %d", req.GetNextOffset(), committedOffset),
				}
				if err := stream.Send(resp); err != nil {
					return err
				}
				continue
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
	// Topic-level authorization: Replay reads full partition history, so it
	// requires at least subscribe permission on the topic. Skipped when auth is
	// disabled (checkTopicAuth is a no-op then).
	if err := h.checkTopicAuth(stream.Context(), req.GetTopic(), "subscribe"); err != nil {
		return err
	}

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
			if !h.partitionManager.FollowerReadsEnabled() {
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
	utils.GoSafe("replay-stream", func() {
		errCh <- replayEngine.ReplayStream(stream.Context(), replayReq, eventCh)
	})

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

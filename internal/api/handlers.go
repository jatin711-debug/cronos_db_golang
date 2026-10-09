// Package api implements CronosDB's public and internal gRPC services, HTTP
// health/admin dashboard endpoints, TLS helpers, rate limiting, and interceptors
// for auth, audit, metrics, versioning, and SLO tracking.
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
	"github.com/jatin711-debug/cronos_db_golang/internal/dedup"
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

// EventServiceHandler implements the client-facing EventService gRPC API
// (publish, subscribe, ack, replay, and related operations).
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

// DedupManager is the interface EventService uses for message-ID deduplication.
type DedupManager interface {
	// IsDuplicate reports whether messageID was already accepted at offset.
	IsDuplicate(messageID string, offset int64) (bool, error)
	// IsDuplicateBatch checks a batch of message IDs for duplicates.
	IsDuplicateBatch(messageIDs []string, offsets []int64) ([]bool, error)
	// RollbackBatch undoes dedup claims when the durable write that followed the
	// claim failed, so a client retry is not dropped as a spurious duplicate.
	RollbackBatch(messageIDs []string) error
}

// ConsumerManager is the interface EventService uses for subscribe/ack and offset reads.
type ConsumerManager interface {
	// Subscribe creates a subscription for the given request.
	Subscribe(request *types.SubscribeRequest) (*consumer.Subscription, error)
	// Ack acknowledges a delivery and advances the committed offset.
	Ack(request *types.AckRequest) error
	// GetCommittedOffset returns the group's committed offset for a partition.
	GetCommittedOffset(groupID string, partitionID int32) (int64, error)
	// LeaveGroup removes a member from a consumer group.
	LeaveGroup(groupID, memberID string) error
}

// ClusterRouter provides cluster-aware partition routing for EventService.
// When non-nil, the handler checks partition locality and leadership before processing.
type ClusterRouter interface {
	// IsLocalPartition reports whether partitionID is hosted on this node.
	IsLocalPartition(partitionID int32) bool
	// IsPartitionLeader reports whether this node is the leader for partitionID.
	IsPartitionLeader(partitionID int32) bool
	// PartitionHasLeader reports whether any node is known to lead partitionID.
	PartitionHasLeader(partitionID int32) bool
	// IsPartitionWritable reports whether this node may accept publishes for
	// partitionID now: it leads it and is not handing leadership over.
	IsPartitionWritable(partitionID int32) bool
	// GetPartitionEpoch returns the cluster leadership epoch for partitionID.
	GetPartitionEpoch(partitionID int32) int64
}

// GRPCStream adapts a gRPC bidi Subscribe stream to the delivery.Stream interface.
type GRPCStream struct {
	stream grpc.BidiStreamingServer[types.SubscribeRequest, types.Delivery]
	// sendMu serializes Send: first deliveries and retries come from different
	// goroutines, and a gRPC stream allows only one sender at a time.
	sendMu sync.Mutex
}

// Send sends a delivery message to the subscriber over the gRPC stream.
func (s *GRPCStream) Send(delivery *delivery.DeliveryMessage) error {
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
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

// Context returns the underlying gRPC stream context.
func (s *GRPCStream) Context() context.Context {
	return s.stream.Context()
}

// NewEventServiceHandler creates an EventService handler with the given dependencies.
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
	if err := h.ensureClusterPartitionLed(partitionID); err != nil {
		return err
	}
	return h.ensurePublishAllowed(partitionID)
}

// ensurePublishAllowed is the final check before a publish appends. The
// caller has already announced the publish with Partition.BeginPublish, which
// is what makes a refusal here definitive for a leadership handoff.
func (h *EventServiceHandler) ensurePublishAllowed(partitionID int32) error {
	if h.clusterRouter != nil && !h.clusterRouter.IsPartitionWritable(partitionID) {
		return status.Errorf(codes.Unavailable,
			"partition %d does not take publishes on this node now: its leadership is moving, or this node cannot reach enough of its replicas; retry against the partition leader", partitionID)
	}
	return nil
}

// ensureClusterPartitionLed validates that this node leads the partition, for
// requests that read from it as well as for writes.
func (h *EventServiceHandler) ensureClusterPartitionLed(partitionID int32) error {
	if h.partitionManager.IsSplitting(partitionID) {
		return status.Errorf(codes.Unavailable,
			"partition %d is undergoing split, retry later", partitionID)
	}

	if h.clusterRouter == nil {
		return nil
	}

	if !h.clusterRouter.PartitionHasLeader(partitionID) {
		// A new cluster has not assigned leaders yet, or the leader was lost
		// and its replacement is not elected. Either way it passes by itself.
		return status.Errorf(codes.Unavailable,
			"partition %d has no leader yet; retry", partitionID)
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
	// The other way round, the partition has taken the epoch of a newer
	// leader than the cluster's records on this node show. The records are
	// what is behind: this node was replaced, heard it from the partition's
	// other replicas, and has not heard it from the cluster yet, which can
	// take as long as the node stays cut off from where those records are
	// kept. It does not lead, and says so in the words that send a client to
	// look for the node that does.
	if localEpoch > clusterEpoch {
		return status.Errorf(codes.FailedPrecondition,
			"partition %d has a newer leader than this node's records of the cluster show (epoch %d here, %d there); retry against the partition leader", partitionID, localEpoch, clusterEpoch)
	}

	return nil
}

// ensureSubscriptionServed returns an error once this node has stopped
// leading the partition of a subscription it accepted.
//
// Deliveries are made by the partition's leader. A subscription left open on a
// node that no longer leads would wait there for good while the events are
// delivered, or wait to be, on another node; ending it sends the consumer to
// look for the node that leads now.
func (h *EventServiceHandler) ensureSubscriptionServed(p *partition.Partition) error {
	if h.clusterRouter == nil {
		return nil
	}
	if err := h.ensureClusterPartitionLed(p.ID); err != nil {
		return err
	}
	if !p.IsLeader() {
		// Stepped down for a newer leader that the cluster's records on this
		// node do not show yet.
		return status.Errorf(codes.FailedPrecondition,
			"partition %d is local but this node is not the leader any more; retry against the partition leader", p.ID)
	}
	return nil
}

// duplicateKind says how to answer a publish whose message ID is already
// recorded for its partition.
type duplicateKind int

const (
	// duplicateAccepted: the earlier publish completed.
	duplicateAccepted duplicateKind = iota
	// duplicateInProgress: another publish holds the ID and has not finished.
	duplicateInProgress
	// duplicateAppended: the earlier publish left its event in the log without
	// being accepted, so this one finishes it.
	duplicateAppended
	// duplicateReleased: the earlier event is no longer in the log. Its record
	// was dropped and this publish now holds the ID as a new one.
	duplicateReleased
)

// classifyDuplicate looks up what became of the earlier publish of messageID.
// For duplicateAppended it returns that publish's event as read from the log.
func classifyDuplicate(p *partition.Partition, messageID string) (duplicateKind, *types.Event, error) {
	outcome, offset, found, err := p.DedupStore.Outcome(messageID)
	if err != nil || !found {
		// Not found: the holder released the ID between the claim attempt and
		// this lookup. The caller's retry will claim it.
		return duplicateInProgress, nil, err
	}
	switch outcome {
	case dedup.Accepted:
		return duplicateAccepted, nil, nil
	case dedup.Claimed:
		return duplicateInProgress, nil, nil
	}

	logged, inLog, err := p.LoggedEvent(messageID, offset)
	if err != nil {
		return duplicateInProgress, nil, err
	}
	if inLog {
		return duplicateAppended, logged, nil
	}
	// The log does not hold the earlier event any more, for instance because a
	// newer leader replaced this node's unreplicated tail. That publish cannot
	// complete, so its record must not keep refusing the ID.
	released, err := p.DedupStore.ReleaseIf(messageID, dedup.AppendedAt(offset))
	if err != nil || !released {
		return duplicateInProgress, nil, err
	}
	taken, err := p.DedupStore.IsDuplicate(messageID, dedup.ClaimedOffset)
	if err != nil || taken {
		return duplicateInProgress, nil, err
	}
	return duplicateReleased, nil, nil
}

// finishPriorPublishes completes earlier publishes whose events, logged, are
// already in the partition's log. Nothing is appended: the log is replicated
// as far as the last of them, which covers the others because followers hold a
// prefix of it, and the partition then schedules and accepts them.
func (h *EventServiceHandler) finishPriorPublishes(p *partition.Partition, logged []*types.Event, durable bool) error {
	last := logged[0]
	for _, event := range logged[1:] {
		if event.Offset > last.Offset {
			last = event
		}
	}
	if h.partitionManager.ReplicationRequired() || p.ReplLeader != nil {
		p.ReplicateMu.Lock()
		repl := p.ReplLeader
		var err error
		if repl == nil {
			err = fmt.Errorf("replication leader for partition %d is not ready", p.ID)
		} else if err = repl.Replicate([]*types.Event{last}); err != nil {
			err = fmt.Errorf("replication: %w", err)
		}
		p.ReplicateMu.Unlock()
		if err != nil {
			return err
		}
	}
	if durable {
		if err := p.Wal.Flush(); err != nil {
			return fmt.Errorf("durable fsync: %w", err)
		}
	}
	return p.AcceptAppended(logged)
}

// acceptEarlierPublishes is called by a publish that has just been replicated
// as required, with the offset of its first event. Whatever the partition
// holds below that offset is therefore replicated too.
func acceptEarlierPublishes(p *partition.Partition, firstOffset int64) {
	if !p.HasUnaccepted() {
		return
	}
	if err := p.AcceptThrough(firstOffset - 1); err != nil {
		slog.Warn("accepting earlier publishes failed", "partition", p.ID, "through", firstOffset-1, "error", err)
	}
}

// Publish handles a single-event publish request: validate, authorize, dedup,
// append to the partition WAL, and optionally wait for durable ack.
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

	defer partitionInternal.EndPublish(partitionInternal.BeginPublish())
	if err := h.ensurePublishAllowed(partitionID); err != nil {
		return nil, err
	}

	// Check if duplicate (unless explicitly allowed). dedupMgr is kept in scope so
	// the claim can be rolled back if the durable WAL append below fails.
	var dedupMgr DedupManager
	if !req.AllowDuplicate {
		dedupMgr = h.dedupManagerForPartition(partitionID)
		if dedupMgr == nil {
			return nil, status.Error(codes.Unavailable, "dedup manager not initialized on this node")
		}

		isDuplicate, err := dedupMgr.IsDuplicate(event.GetMessageId(), -1) // offset will be assigned
		if err != nil {
			return &types.PublishResponse{
				Success: false,
				Error:   fmt.Sprintf("check duplicate: %v", err),
			}, nil
		}
		if isDuplicate {
			kind, logged, err := classifyDuplicate(partitionInternal, event.GetMessageId())
			switch {
			case err != nil:
				return &types.PublishResponse{
					Success: false,
					Error:   fmt.Sprintf("check duplicate: %v", err),
				}, nil
			case kind == duplicateAccepted:
				return &types.PublishResponse{
					Success: false,
					Error:   "duplicate message_id",
				}, nil
			case kind == duplicateInProgress:
				return &types.PublishResponse{
					Success: false,
					Error:   "duplicate message_id: an earlier publish with this ID is still in progress",
				}, nil
			case kind == duplicateAppended:
				// The earlier attempt reached the log and then failed. Finish
				// it; appending again would deliver the event twice.
				if err := h.finishPriorPublishes(partitionInternal, []*types.Event{logged}, durableAckEnabled(event)); err != nil {
					return &types.PublishResponse{
						Success: false,
						Error:   err.Error(),
					}, nil
				}
				metrics.AddEventsAccepted(partitionInternal.ID, 1)
				return &types.PublishResponse{
					Success:     true,
					Offset:      logged.Offset,
					PartitionId: partitionInternal.ID,
					ScheduleTs:  logged.GetScheduleTs(),
				}, nil
			}
			// duplicateReleased: this publish holds the ID and proceeds as new.
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
	// fsync). rollbackDedup undoes the dedup claim when the event never reached
	// the log, so a client retry of this message ID is not dropped as a
	// duplicate. A failure after the append leaves the event in the log; the
	// partition then holds it unaccepted, and a retry finishes this publish
	// instead of appending the event again.
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
	if h.partitionManager.ReplicationRequired() || partitionInternal.ReplLeader != nil {
		partitionInternal.ReplicateMu.Lock()
		repl := partitionInternal.ReplLeader
		if repl == nil {
			partitionInternal.ReplicateMu.Unlock()
			rollbackDedup()
			return &types.PublishResponse{
				Success: false,
				Error:   fmt.Sprintf("replication leader for partition %d is not ready", partitionID),
			}, nil
		}
		if err := partitionInternal.Wal.AppendEvent(event); err != nil {
			partitionInternal.ReplicateMu.Unlock()
			rollbackDedup()
			return &types.PublishResponse{
				Success: false,
				Error:   fmt.Sprintf("append to WAL: %v", err),
			}, nil
		}
		replErr := repl.Replicate([]*types.Event{event})
		if replErr != nil {
			// In the local log but not on the required in-sync replicas.
			partitionInternal.HoldUnaccepted([]*types.Event{event}, dedupMgr != nil)
		}
		partitionInternal.ReplicateMu.Unlock()
		if replErr != nil {
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
			partitionInternal.HoldUnaccepted([]*types.Event{event}, dedupMgr != nil)
			return &types.PublishResponse{
				Success: false,
				Error:   fmt.Sprintf("durable fsync: %v", err),
			}, nil
		}
	}
	acceptEarlierPublishes(partitionInternal, event.Offset)

	// Schedule the event in timing wheel
	if err := partitionInternal.Scheduler.Schedule(event); err != nil {
		partitionInternal.HoldUnaccepted([]*types.Event{event}, dedupMgr != nil)
		return &types.PublishResponse{
			Success: false,
			Error:   fmt.Sprintf("schedule event: %v", err),
		}, nil
	}

	if !req.AllowDuplicate {
		if err := partitionInternal.DedupStore.Put(event.MessageId, event.Offset, event.CreatedTs); err != nil {
			return nil, status.Errorf(codes.Internal, "record publish completion: %v", err)
		}
	}
	metrics.AddEventsAccepted(partitionInternal.ID, 1)
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
	// priorPublishes holds, per partition, the logged events of earlier publishes
	// that this request retries and must finish rather than append again.
	priorPublishes := make(map[int32][]*types.Event)
	if !req.AllowDuplicate {
		for pid, evts := range partitionEvents {
			messageIDs := make([]string, len(evts))
			offsets := make([]int64, len(evts))
			for i, e := range evts {
				messageIDs[i] = e.GetMessageId()
				offsets[i] = -1
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
				if !duplicates[i] {
					kept = append(kept, e)
					continue
				}
				// A recorded ID alone does not prove the previous publish was
				// replicated or scheduled. Only an accepted one is a duplicate
				// that needs nothing more.
				kind, logged, err := classifyDuplicate(partitionInternals[pid], e.MessageId)
				if kind == duplicateReleased {
					kept = append(kept, e)
					continue
				}
				atomic.AddInt32(&duplicateCount, 1)
				metrics.AddEventsDuplicate(pid, 1)
				switch {
				case err != nil:
					setError(1, fmt.Sprintf("duplicate message_id: previous outcome unknown: %v", err))
				case kind == duplicateInProgress:
					setError(1, "duplicate message_id: an earlier publish with this ID is still in progress")
				case kind == duplicateAppended:
					priorPublishes[pid] = append(priorPublishes[pid], logged)
				}
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

	// Finish the earlier publishes this request retries. Their events are in
	// the log already, so this replicates and schedules without appending.
	durableRetry := len(priorPublishes) > 0 && anyDurableAck(req.Events)
	for partitionID, logged := range priorPublishes {
		wg.Add(1)
		go func(pid int32, logged []*types.Event) {
			defer wg.Done()
			partitionInternal := partitionInternals[pid]
			defer partitionInternal.EndPublish(partitionInternal.BeginPublish())
			err := h.ensurePublishAllowed(pid)
			if err == nil {
				err = h.finishPriorPublishes(partitionInternal, logged, durableRetry)
			}
			if err != nil {
				setError(int32(len(logged)), fmt.Sprintf("finishing earlier publish for partition %d: %v", pid, err))
			}
		}(partitionID, logged)
	}

	for partitionID, events := range partitionEvents {
		wg.Add(1)
		go func(pid int32, evts []*types.Event) {
			defer wg.Done()

			partitionInternal := partitionInternals[pid]
			if partitionInternal == nil {
				setError(int32(len(evts)), fmt.Sprintf("partition %d was not materialized", pid))
				return
			}
			defer partitionInternal.EndPublish(partitionInternal.BeginPublish())

			// rollbackDedup undoes the dedup claims for this batch when it never
			// reached the log, so client retries are not dropped as spurious
			// duplicates. Only meaningful when dedup actually claimed them
			// (i.e. !req.AllowDuplicate). Once the batch is in the log a failure
			// leaves it held by the partition instead, for a retry to finish.
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

			// Announced above; this is the check a leadership handoff relies on.
			if err := h.ensurePublishAllowed(pid); err != nil {
				rollbackDedup()
				setError(int32(len(evts)), err.Error())
				return
			}

			// Batch append to WAL (single syscall for all events). When this
			// partition is a replication leader, the append and the replication to
			// followers must occur together in strict offset order — Leader.Replicate
			// requires contiguous offsets and is not safe to call concurrently out of
			// order. ReplicateMu serializes the two for replicated partitions only;
			// RF=1 / non-leader partitions (ReplLeader == nil) keep the fully
			// pipelined fast path with no extra locking.
			if h.partitionManager.ReplicationRequired() || partitionInternal.ReplLeader != nil {
				partitionInternal.ReplicateMu.Lock()
				repl := partitionInternal.ReplLeader
				if repl == nil {
					partitionInternal.ReplicateMu.Unlock()
					rollbackDedup()
					setError(int32(len(evts)), fmt.Sprintf("replication leader for partition %d is not ready", pid))
					return
				}
				if err := partitionInternal.Wal.AppendBatch(evts); err != nil {
					partitionInternal.ReplicateMu.Unlock()
					rollbackDedup()
					setError(int32(len(evts)), fmt.Sprintf("WAL append for partition %d: %v", pid, err))
					return
				}
				replErr := repl.Replicate(evts)
				if replErr != nil {
					// The batch is in the local WAL but did not reach the required
					// in-sync replicas. It stays held, unscheduled, until it has;
					// the client's retry finishes this publish.
					partitionInternal.HoldUnaccepted(evts, !req.AllowDuplicate)
				}
				partitionInternal.ReplicateMu.Unlock()
				if replErr != nil {
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
					partitionInternal.HoldUnaccepted(evts, !req.AllowDuplicate)
					setError(int32(len(evts)), fmt.Sprintf("durable fsync for partition %d: %v", pid, err))
					return
				}
			}
			acceptEarlierPublishes(partitionInternal, evts[0].Offset)

			// Batch schedule all events (single lock acquisition)
			if err := partitionInternal.Scheduler.ScheduleBatch(evts); err != nil {
				// The events are in the WAL, but the publish is not complete until
				// they are scheduled. Hold them so a retry schedules them, and
				// surface the failure instead of silently reporting success.
				slog.Warn("batch schedule partially failed", "partition", pid, "count", len(evts), "error", err)
				partitionInternal.HoldUnaccepted(evts, !req.AllowDuplicate)
				setError(int32(len(evts)), fmt.Sprintf("schedule batch for partition %d: %v", pid, err))
				return
			}

			if !req.AllowDuplicate {
				ids := make([]string, len(evts))
				offsets := make([]int64, len(evts))
				createdTS := make([]int64, len(evts))
				for i, event := range evts {
					ids[i], offsets[i], createdTS[i] = event.MessageId, event.Offset, event.CreatedTs
				}
				if err := partitionInternal.DedupStore.PutBatch(ids, offsets, createdTS); err != nil {
					setError(int32(len(evts)), fmt.Sprintf("record publish completion: %v", err))
					return
				}
			}
			// Update stats
			localPublished := int32(len(evts))
			atomic.AddInt32(&publishedCount, localPublished)
			metrics.AddEventsAccepted(partitionInternal.ID, len(evts))

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

	if req.GetTopic() == "" {
		return status.Error(codes.InvalidArgument, "topic is required")
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

	if err := h.ensureClusterPartitionLed(partitionID); err != nil {
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

	cm := partitionInternal.ConsumerGroup
	// Get consumer group offset
	startOffset, err := cm.GetCommittedOffset(req.GetConsumerGroup(), partitionID)
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
		NextOffset:    max(0, startOffset),
		Topic:         req.GetTopic(),
		Subject:       principal(ctx),
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
	if _, err := cm.Subscribe(req); err != nil {
		return fmt.Errorf("create consumer group: %w", err)
	}
	defer func() {
		if err := cm.LeaveGroup(req.GetConsumerGroup(), req.GetSubscriptionId()); err != nil {
			slog.Warn("Failed to leave consumer group", "group", req.GetConsumerGroup(), "member", req.GetSubscriptionId(), "error", err)
		}
	}()

	origin := max(int64(0), startOffset)
	if req.GetStartOffset() >= 0 {
		origin = max(origin, req.GetStartOffset())
	}
	return h.redriveRetained(stream.Context(), partitionInternal, req.GetConsumerGroup(), req.GetTopic(), origin)
}

const (
	// redriveBatchEvents is how many WAL records one redrive step offers.
	redriveBatchEvents = 512
	// redriveBatchBytes is how many bytes of them one step may read: events are
	// megabytes each, and 512 of them at once is gigabytes.
	redriveBatchBytes = 4 << 20
	// redriveBlockedWait bounds the wait for credits when no ack arrives.
	redriveBlockedWait = 250 * time.Millisecond
	// redriveSweepInterval and redriveSweepEvents pace the background sweep.
	redriveSweepInterval = 500 * time.Millisecond
	redriveSweepEvents   = 256
	// servedCheckInterval is how often an open subscription checks that this
	// node still leads its partition.
	servedCheckInterval = 250 * time.Millisecond
)

// redriveRetained delivers retained WAL records to one subscription's consumer
// group until ctx ends. Live events normally arrive through the scheduler; this
// covers everything that path could not hand over. It has three sources, in
// priority order:
//
//  1. ranges the dispatcher queued for redrive (flow control, a full worker
//     queue, an abandoned or failed delivery), re-read as soon as they are
//     reported;
//  2. the backlog that existed when the subscription started, read once at
//     the pace the group's credits allow (later events are the live path's);
//  3. a slow sweep from the group's first incomplete offset, as a safety net
//     for anything the first two did not cover.
//
// Only due records of the subscribed topic are eligible, and the dispatcher
// skips records already completed or in flight for the group.
//
// Nothing is read beyond what may be delivered (Partition.DeliverableThrough):
// the end of the log can hold entries that are not on enough replicas yet, or
// never will be. What is left of a range waits until they are.
func (h *EventServiceHandler) redriveRetained(ctx context.Context, p *partition.Partition, group, topic string, origin int64) error {
	requests := p.Dispatcher.RegisterRedrive(group)
	defer p.Dispatcher.UnregisterRedrive(requests)

	// offer hands the group [from, to], as far as one batch of the log reads
	// it: next is where the range goes on from. blocked means flow control held
	// some of it back and the same range must be offered again.
	var cached []*types.Event
	cachedFrom, cachedTo, cachedNext := int64(-1), int64(-1), int64(-1)
	offer := func(from, to int64) (next int64, blocked bool, err error) {
		if from != cachedFrom || to != cachedTo {
			events, err := p.Wal.ReadEventsWithin(from, to, redriveBatchBytes)
			if err != nil {
				return from, false, status.Errorf(codes.Internal, "replay retained events: %v", err)
			}
			cached, cachedFrom, cachedTo, cachedNext = events, from, to, to+1
			if len(events) > 0 {
				cachedNext = events[len(events)-1].Offset + 1
			}
		}
		now := time.Now().UnixMilli()
		due := make([]*types.Event, 0, len(cached))
		for _, event := range cached {
			if event.Topic == topic && event.ScheduleTs <= now {
				due = append(due, event)
			}
		}
		_, blocked = p.Dispatcher.RedriveGroup(group, due)
		return cachedNext, blocked, nil
	}

	backlog := origin                   // next offset of the one-time backlog pass
	backlogEnd := p.Wal.GetLastOffset() // registered above, so later events are pushed or queued
	sweep := origin                     // next offset of the background sweep
	nextSweep := time.Now().Add(redriveSweepInterval)
	retryLow, retryHigh := int64(0), int64(-1) // queued redrive range; empty when low > high
	nextServedCheck := time.Now()

	for {
		if now := time.Now(); !now.Before(nextServedCheck) {
			if err := h.ensureSubscriptionServed(p); err != nil {
				return err
			}
			nextServedCheck = now.Add(servedCheckInterval)
		}
		if low, high, ok := requests.Take(); ok {
			low = max(low, origin)
			// A range wholly inside the unread backlog is covered by that pass.
			if covered := low >= backlog && high <= backlogEnd; low <= high && !covered {
				if retryLow > retryHigh {
					retryLow, retryHigh = low, high
				} else {
					retryLow, retryHigh = min(retryLow, low), max(retryHigh, high)
				}
			}
		}

		deliverable := p.DeliverableThrough()
		end := min(p.Wal.GetLastOffset(), deliverable)
		retryTo := min(retryLow+redriveBatchEvents-1, retryHigh, deliverable)
		backlogTo := min(backlog+redriveBatchEvents-1, backlogEnd, deliverable)
		var wait time.Duration
		switch {
		case retryLow <= retryTo:
			next, blocked, err := offer(retryLow, retryTo)
			if err != nil {
				return err
			}
			if blocked {
				wait = redriveBlockedWait
			} else {
				retryLow = next
			}
		case backlog <= backlogTo:
			next, blocked, err := offer(backlog, backlogTo)
			if err != nil {
				return err
			}
			if blocked {
				wait = redriveBlockedWait
			} else {
				backlog = next
			}
		default:
			if now := time.Now(); now.Before(nextSweep) {
				wait = nextSweep.Sub(now)
			} else {
				nextSweep = now.Add(redriveSweepInterval)
				floor := origin
				if committed, err := p.ConsumerGroup.GetCommittedOffset(group, p.ID); err == nil {
					floor = max(floor, committed)
				}
				if sweep < floor || sweep > end {
					sweep = floor
				}
				if sweep <= end {
					to := min(sweep+redriveSweepEvents-1, end)
					next, blocked, err := offer(sweep, to)
					if err != nil {
						return err
					}
					if !blocked {
						sweep = next
					}
				}
				wait = redriveSweepInterval
			}
			if retryLow <= retryHigh || backlog <= backlogEnd {
				// The rest of these is not replicated yet; look again soon.
				wait = min(wait, redriveBlockedWait)
			}
		}

		if wait == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
			continue
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-requests.Wake():
			timer.Stop()
		case <-timer.C:
		}
	}
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

	// Acks are read ahead of processing. Recording a completion is a durable
	// write, so everything that queues up behind one write is committed
	// together by the next instead of paying for an fsync each.
	incoming := make(chan *types.AckRequest, maxAckBatch)
	recvErr := make(chan error, 1)
	go func() {
		defer close(incoming)
		for {
			req, err := stream.Recv()
			if err != nil {
				recvErr <- err
				return
			}
			select {
			case incoming <- req:
			case <-stream.Context().Done():
				recvErr <- stream.Context().Err()
				return
			}
		}
	}()

	batch := make([]*types.AckRequest, 0, maxAckBatch)
	for {
		first, ok := <-incoming
		if !ok {
			return <-recvErr
		}
		batch = append(batch[:0], first)
	fill:
		for len(batch) < maxAckBatch {
			select {
			case req, ok := <-incoming:
				if !ok {
					break fill
				}
				batch = append(batch, req)
			default:
				break fill
			}
		}
		for _, resp := range h.processAcks(ctx, batch) {
			if err := stream.Send(resp); err != nil {
				return err
			}
		}
	}
}

// maxAckBatch bounds how many acks of one stream share a durable commit.
const maxAckBatch = 256

// processAcks validates a run of acks from one stream, records the successful
// ones with one durable write per partition, and returns a response for each
// ack in order.
func (h *EventServiceHandler) processAcks(ctx context.Context, acks []*types.AckRequest) []*types.AckResponse {
	type ackState struct {
		partition *partition.Partition
		group     string
		events    []*types.Event
		err       error
	}
	states := make([]ackState, len(acks))
	successes := make(map[int32][]int) // partition ID -> indexes of acks to record

	subject := principal(ctx)
	for i, req := range acks {
		state := &states[i]
		parts := strings.SplitN(req.GetDeliveryId(), ":", 3)
		if len(parts) != 3 {
			state.err = fmt.Errorf("invalid delivery ID")
			continue
		}
		pid, err := strconv.ParseInt(parts[1], 10, 32)
		if err != nil {
			state.err = err
			continue
		}
		p, err := h.partitionManager.GetInternalPartition(int32(pid))
		if err != nil {
			state.err = err
			continue
		}
		group, topic, events, err := p.Dispatcher.ValidateAck(req.DeliveryId, subject, req.NextOffset, req.Success)
		if err == nil {
			err = h.checkTopicAuth(ctx, topic, "subscribe")
		}
		if err != nil {
			state.err = err
			continue
		}
		state.partition, state.group, state.events = p, group, events
		if req.Success {
			successes[p.ID] = append(successes[p.ID], i)
		}
	}

	for pid, indexes := range successes {
		commits := make([]consumer.DeliveryCommit, len(indexes))
		for j, i := range indexes {
			commits[j] = consumer.DeliveryCommit{GroupID: states[i].group, Events: states[i].events}
		}
		for j, err := range states[indexes[0]].partition.ConsumerGroup.CommitDeliveries(pid, commits) {
			states[indexes[j]].err = err
		}
	}

	responses := make([]*types.AckResponse, len(acks))
	for i, req := range acks {
		state := &states[i]
		if state.err == nil {
			state.err = state.partition.Dispatcher.HandleAck(req.DeliveryId, req.Success, req.NextOffset)
		}
		if state.err != nil {
			responses[i] = &types.AckResponse{Success: false, Error: state.err.Error()}
			continue
		}
		committed, _ := state.partition.ConsumerGroup.GetCommittedOffset(state.group, state.partition.ID)
		responses[i] = &types.AckResponse{Success: true, CommittedOffset: committed}
	}
	return responses
}

// Replay handles replay requests
func (h *EventServiceHandler) Replay(req *types.ReplayRequest, stream types.EventService_ReplayServer) error {
	if req.GetTopic() == "" {
		return status.Error(codes.InvalidArgument, "topic is required")
	}
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

func principal(ctx context.Context) string {
	if claims, ok := auth.ClaimsFromContext(ctx); ok {
		return claims.Subject
	}
	return ""
}

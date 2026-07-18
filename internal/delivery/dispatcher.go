// Package delivery implements sharded event dispatch to gRPC subscribers.
//
// The Dispatcher fans out ready events with per-subscription flow-control
// credits, ack timeouts, exponential retries via a min-heap, optional circuit
// breakers, and a durable dead-letter queue for exhausted deliveries. A Worker
// bridges the scheduler ready queue into DispatchBatch for high throughput.
package delivery

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/internal/metrics"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
	"github.com/jatin711-debug/cronos_db_golang/pkg/utils"
)

// dispatchPools recycle short-lived maps and slices used on the hot dispatch
// path. Maps are cleared before reuse so the pooled instance does not retain
// stale references.
var dispatchPools = struct {
	partitionEvents sync.Pool // map[int32][]*types.Event
	groupSubs       sync.Pool // map[string][]*Subscription
	batchesBySub    sync.Pool // map[*Subscription][]*types.Event
	intMap          sync.Pool // map[string]int
	stringSlice     sync.Pool // []string
}{
	partitionEvents: sync.Pool{New: func() interface{} { return make(map[int32][]*types.Event) }},
	groupSubs:       sync.Pool{New: func() interface{} { return make(map[string][]*Subscription) }},
	batchesBySub:    sync.Pool{New: func() interface{} { return make(map[*Subscription][]*types.Event) }},
	intMap:          sync.Pool{New: func() interface{} { return make(map[string]int) }},
	stringSlice:     sync.Pool{New: func() interface{} { return make([]string, 0, 16) }},
}

func acquirePartitionEventMap() map[int32][]*types.Event {
	return dispatchPools.partitionEvents.Get().(map[int32][]*types.Event)
}

func releasePartitionEventMap(m map[int32][]*types.Event) {
	clear(m)
	dispatchPools.partitionEvents.Put(m)
}

func acquireGroupSubsMap() map[string][]*Subscription {
	return dispatchPools.groupSubs.Get().(map[string][]*Subscription)
}

func releaseGroupSubsMap(m map[string][]*Subscription) {
	clear(m)
	dispatchPools.groupSubs.Put(m)
}

func acquireBatchesBySubMap() map[*Subscription][]*types.Event {
	return dispatchPools.batchesBySub.Get().(map[*Subscription][]*types.Event)
}

func releaseBatchesBySubMap(m map[*Subscription][]*types.Event) {
	clear(m)
	dispatchPools.batchesBySub.Put(m)
}

func acquireIntMap() map[string]int {
	return dispatchPools.intMap.Get().(map[string]int)
}

func releaseIntMap(m map[string]int) {
	clear(m)
	dispatchPools.intMap.Put(m)
}

func acquireStringSlice() []string {
	return dispatchPools.stringSlice.Get().([]string)[:0]
}

func releaseStringSlice(s []string) {
	if cap(s) <= 4096 {
		dispatchPools.stringSlice.Put(s[:0])
	}
}

// Dispatcher manages event delivery to subscribers.
// Subscriptions are sharded by ID to reduce lock contention; partition-to-
// subscriber indexes enable O(1) fan-out. Credits, in-flight caps, retries,
// and circuit breakers protect slow or failing consumers.
type Dispatcher struct {
	shards     []*DispatcherShard
	shardCount int
	config     *Config
	dlq        *DeadLetterQueue
	quit       chan struct{}
	quitOnce   sync.Once
	wg         sync.WaitGroup

	// Partition-to-subscribers index for O(1) lookup instead of scanning all shards.
	partitionsMu  sync.RWMutex
	partitionSubs map[int32][]*Subscription

	// Per-consumer-group round-robin cursor that persists across batches so
	// events are evenly distributed across subscribers. snapshotGroupCursors
	// (called once per batch) reads the current positions into a lock-free
	// local map used for the duration of the batch; advanceGroupCursors
	// writes the final positions back. This avoids acquiring the mutex once
	// per event on the hot path.
	groupCursorMu sync.Mutex
	groupCursor   map[string]int

	// In-flight limiter to protect memory under slow consumers.
	inFlightCount atomic.Int64

	// Retry heap for non-blocking delayed retries.
	retryHeap *RetryHeap

	// OnDeliveryComplete is called when a delivery reaches final disposition
	// (successful ack or DLQ). The tenantID is extracted from Event.Meta.
	OnDeliveryComplete func(tenantID string)
}

// DispatcherShard holds a subset of subscriptions and their in-flight deliveries.
type DispatcherShard struct {
	mu               sync.RWMutex
	subscriptions    map[string]*Subscription
	activeDeliveries map[string]*ActiveDelivery
	// expiry indexes activeDeliveries by ack deadline so the timeout loop pops
	// only the entries that are actually due instead of scanning every active
	// delivery. Guarded by mu, in lockstep with activeDeliveries.
	expiry *deliveryExpiry
	dlq    *DeadLetterQueue
}

// Subscription represents a single consumer connection for a partition.
type Subscription struct {
	// ID uniquely identifies this subscription across the dispatcher.
	ID string
	// ConsumerGroup is the logical group used for round-robin fan-out.
	ConsumerGroup string
	// Partition is the partition this subscriber consumes from.
	Partition *types.Partition
	// NextOffset is the next expected offset after the last successful ack.
	NextOffset int64
	// Credits is the current flow-control credit balance (atomically updated).
	Credits int32
	// MaxCredits is the credit ceiling for this subscription.
	MaxCredits int32
	// Stream is the transport used to send deliveries and receive control messages.
	Stream Stream
	// CreatedTS is the subscription creation time in Unix milliseconds.
	CreatedTS int64

	// circuitBreaker protects against repeatedly sending to failed consumers.
	circuitBreaker *CircuitBreaker
}

// Stream is the bidirectional transport used to deliver events and receive acks/credits.
type Stream interface {
	// Send delivers a message (or batch) to the subscriber.
	Send(delivery *DeliveryMessage) error
	// Recv receives the next control message (ack or credit top-up).
	Recv() (*Control, error)
	// Context returns the stream's cancellation context.
	Context() context.Context
}

// DeliveryMessage is a unit of work sent to a subscriber.
type DeliveryMessage struct {
	// Event is the single-event payload; set for non-batch deliveries (and often
	// for singleton batches for convenience).
	Event *types.Event
	// DeliveryID uniquely identifies this delivery for ack correlation.
	DeliveryID string
	// Attempt is the 1-based delivery attempt number.
	Attempt int32
	// AckTimeout is the ack deadline in milliseconds from send time.
	AckTimeout int32
	// Batch holds multiple events for batched delivery; nil for single-event sends.
	Batch []*types.Event
}

// Control is a control-plane message from a subscriber (ack and/or credit).
type Control struct {
	// Ack is a delivery acknowledgment, if present.
	Ack *AckMessage
	// Credit is a flow-control credit top-up, if present.
	Credit *CreditMessage
}

// AckMessage acknowledges (or nacks) a prior delivery.
type AckMessage struct {
	// DeliveryID is the delivery being acknowledged.
	DeliveryID string
	// Success is true when the subscriber processed the delivery successfully.
	Success bool
	// Error is an optional error description when Success is false.
	Error string
	// NextOffset is the subscriber's next expected offset after this ack.
	NextOffset int64
}

// CreditMessage tops up flow-control credits for a subscription.
type CreditMessage struct {
	// Credits is the number of credits to add.
	Credits int32
}

// ActiveDelivery tracks an in-flight delivery awaiting ack or timeout.
type ActiveDelivery struct {
	// Delivery is the message that was sent to the subscriber.
	Delivery *DeliveryMessage
	// Subscription is the recipient of the delivery.
	Subscription *Subscription
	// Attempt is the current attempt number (mirrors Delivery.Attempt).
	Attempt int32
	// CreatedTS is when this attempt was tracked (Unix ms).
	CreatedTS int64
	// AckDeadline is when the delivery times out if not acked.
	AckDeadline time.Time
	// CreditsConsumed is how many credits were taken for this delivery (batch size or 1).
	CreditsConsumed int32
}

// Config holds dispatcher tuning parameters.
type Config struct {
	// MaxRetries is the maximum delivery attempts before DLQ.
	MaxRetries int32
	// DefaultAckTimeout is how long to wait for an ack before retrying.
	DefaultAckTimeout time.Duration
	// MaxDeliveryCredits is the default credit ceiling for new subscriptions.
	MaxDeliveryCredits int32
	// RetryBackoff is the base delay multiplied by attempt number between retries.
	RetryBackoff time.Duration
	// MaxInFlightEvents is the global cap on in-flight deliveries (0 = unlimited).
	MaxInFlightEvents int64

	// CircuitBreakerFailureThreshold is the failure rate (0.0–1.0) that trips the breaker.
	// A value of 1.0 effectively disables tripping under normal ratios.
	CircuitBreakerFailureThreshold float64
	// CircuitBreakerOpenDurationMs is how long the breaker stays open before half-open.
	CircuitBreakerOpenDurationMs int64
	// CircuitBreakerMinAttempts is the minimum samples before the failure rate is evaluated.
	CircuitBreakerMinAttempts int64
}

// DefaultConfig returns a Config with production-oriented defaults.
func DefaultConfig() *Config {
	return &Config{
		MaxRetries:                     5,
		DefaultAckTimeout:              30 * time.Second,
		MaxDeliveryCredits:             1000,
		RetryBackoff:                   1 * time.Second,
		MaxInFlightEvents:              100000, // Default 100K in-flight events cap
		CircuitBreakerFailureThreshold: 0.5,
		CircuitBreakerOpenDurationMs:   30000,
		CircuitBreakerMinAttempts:      10,
	}
}

const defaultShardCount = 32

// NewDispatcher creates a sharded dispatcher and starts the timeout/retry loop.
func NewDispatcher(config *Config) *Dispatcher {
	if config == nil {
		config = DefaultConfig()
	}

	shardCount := defaultShardCount
	shards := make([]*DispatcherShard, shardCount)
	for i := 0; i < shardCount; i++ {
		shards[i] = &DispatcherShard{
			subscriptions:    make(map[string]*Subscription),
			activeDeliveries: make(map[string]*ActiveDelivery),
			expiry:           newDeliveryExpiry(),
		}
	}

	d := &Dispatcher{
		shards:        shards,
		shardCount:    shardCount,
		config:        config,
		dlq:           nil,
		quit:          make(chan struct{}),
		partitionSubs: make(map[int32][]*Subscription),
		groupCursor:   make(map[string]int),
		retryHeap:     NewRetryHeap(),
	}

	d.wg.Add(1)
	utils.GoSafe("dispatcher-timeout", d.timeoutLoop)

	return d
}

// NewDispatcherWithDLQ creates a new dispatcher with a dead-letter queue.
func NewDispatcherWithDLQ(config *Config, dlq *DeadLetterQueue) *Dispatcher {
	d := NewDispatcher(config)
	d.SetDLQ(dlq)
	return d
}

// SetDLQ sets the dead-letter queue.
func (d *Dispatcher) SetDLQ(dlq *DeadLetterQueue) {
	d.dlq = dlq
	for _, shard := range d.shards {
		shard.dlq = dlq
	}
}

// getShard returns the shard for a given key.
func (d *Dispatcher) getShard(key string) *DispatcherShard {
	var h uint32
	for i := 0; i < len(key); i++ {
		h = 31*h + uint32(key[i])
	}
	return d.shards[h%uint32(d.shardCount)]
}

// tryConsumeCredit decrements one credit for a subscription if available.
func (d *Dispatcher) tryConsumeCredit(sub *Subscription) bool {
	for {
		curr := atomic.LoadInt32(&sub.Credits)
		if curr <= 0 {
			return false
		}
		if atomic.CompareAndSwapInt32(&sub.Credits, curr, curr-1) {
			return true
		}
	}
}

// releaseCredits returns n credits to a subscription (capped at MaxCredits).
func (d *Dispatcher) releaseCredits(sub *Subscription, n int32) {
	if sub == nil || n <= 0 {
		return
	}

	for {
		curr := atomic.LoadInt32(&sub.Credits)
		next := curr + n
		if next > sub.MaxCredits {
			next = sub.MaxCredits
		}
		if atomic.CompareAndSwapInt32(&sub.Credits, curr, next) {
			return
		}
	}
}

// releaseCredit returns one credit to a subscription.
func (d *Dispatcher) releaseCredit(sub *Subscription) {
	d.releaseCredits(sub, 1)
}

// Subscribe registers a subscription, initializes credits and circuit breaker,
// and indexes it under its partition for O(1) dispatch lookups.
func (d *Dispatcher) Subscribe(sub *Subscription) error {
	shard := d.getShard(sub.ID)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	if _, exists := shard.subscriptions[sub.ID]; exists {
		return fmt.Errorf("subscription %s already exists", sub.ID)
	}

	if sub.MaxCredits == 0 {
		sub.MaxCredits = d.config.MaxDeliveryCredits
	}
	atomic.StoreInt32(&sub.Credits, sub.MaxCredits)

	// Initialize circuit breaker if enabled
	if sub.circuitBreaker == nil {
		sub.circuitBreaker = NewCircuitBreaker()
	}

	shard.subscriptions[sub.ID] = sub

	d.partitionsMu.Lock()
	d.partitionSubs[sub.Partition.ID] = append(d.partitionSubs[sub.Partition.ID], sub)
	d.partitionsMu.Unlock()

	return nil
}

// Unsubscribe removes a subscription and abandons its in-flight deliveries.
func (d *Dispatcher) Unsubscribe(subscriptionID string) error {
	shard := d.getShard(subscriptionID)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	sub, exists := shard.subscriptions[subscriptionID]
	if !exists {
		return fmt.Errorf("subscription %s not found", subscriptionID)
	}

	partitionID := sub.Partition.ID

	for deliveryID, active := range shard.activeDeliveries {
		if active.Subscription != nil && active.Subscription.ID == subscriptionID {
			if err := d.decInFlight(1); err != nil {
				log.Printf("[DISPATCHER] in-flight underflow: %v", err)
			}
			delete(shard.activeDeliveries, deliveryID)
			shard.expiry.remove(deliveryID)
		}
	}

	delete(shard.subscriptions, subscriptionID)

	d.partitionsMu.Lock()
	if subs := d.partitionSubs[partitionID]; subs != nil {
		newSubs := make([]*Subscription, 0, len(subs))
		for _, s := range subs {
			if s.ID != subscriptionID {
				newSubs = append(newSubs, s)
			}
		}
		if len(newSubs) == 0 {
			delete(d.partitionSubs, partitionID)
		} else {
			d.partitionSubs[partitionID] = newSubs
		}
	}
	d.partitionsMu.Unlock()

	return nil
}

// nextGroupStart returns the next subscriber index for a consumer group and
// advances the persistent cursor by one. Used by the single-event Dispatch
// path; the batch path uses snapshotGroupCursors/advanceGroupCursors to avoid
// a mutex acquire per event.
func (d *Dispatcher) nextGroupStart(groupID string, numSubs int) int {
	d.groupCursorMu.Lock()
	defer d.groupCursorMu.Unlock()

	start := d.groupCursor[groupID]
	d.groupCursor[groupID] = (start + 1) % numSubs
	return start
}

// snapshotGroupCursors returns a snapshot of the current cursor position for
// each group ID in groupIDs. The caller mutates the returned map locally
// (per-event increment) without any locking, then commits the final positions
// back via advanceGroupCursors.
func (d *Dispatcher) snapshotGroupCursors(groupIDs []string) map[string]int {
	d.groupCursorMu.Lock()
	defer d.groupCursorMu.Unlock()
	snapshot := make(map[string]int, len(groupIDs))
	for _, gid := range groupIDs {
		snapshot[gid] = d.groupCursor[gid]
	}
	return snapshot
}

// advanceGroupCursors commits the (locally incremented) cursor positions back
// into the persistent store. Called once per batch, not per event.
func (d *Dispatcher) advanceGroupCursors(snapshots map[string]int, numSubsByGroup map[string]int) {
	if len(snapshots) == 0 {
		return
	}
	d.groupCursorMu.Lock()
	defer d.groupCursorMu.Unlock()
	for gid, pos := range snapshots {
		if numSubs, ok := numSubsByGroup[gid]; ok && numSubs > 0 {
			d.groupCursor[gid] = pos % numSubs
		} else {
			d.groupCursor[gid] = pos
		}
	}
}

func groupSubscriptionsByConsumer(allSubs []*Subscription) map[string][]*Subscription {
	consumerGroupSubs := acquireGroupSubsMap()

	// Fast path: if all subs share the same consumer group, avoid appending.
	if len(allSubs) > 0 {
		firstGroup := allSubs[0].ConsumerGroup
		allSame := true
		for i := 1; i < len(allSubs); i++ {
			if allSubs[i].ConsumerGroup != firstGroup {
				allSame = false
				break
			}
		}
		if allSame {
			consumerGroupSubs[firstGroup] = allSubs
			return consumerGroupSubs
		}
	}

	for _, sub := range allSubs {
		consumerGroupSubs[sub.ConsumerGroup] = append(consumerGroupSubs[sub.ConsumerGroup], sub)
	}
	return consumerGroupSubs
}

func (d *Dispatcher) pickSubscriber(groupSubs []*Subscription, startIdx int) *Subscription {
	for i := 0; i < len(groupSubs); i++ {
		idx := (startIdx + i) % len(groupSubs)
		candidate := groupSubs[idx]
		// Skip subscribers with open circuit breakers
		if candidate.circuitBreaker != nil && !candidate.circuitBreaker.CanTry() {
			continue
		}
		if d.tryConsumeCredit(candidate) {
			return candidate
		}
	}
	return nil
}

// Dispatch fans out a single event to one subscriber per consumer group on the
// event's partition, subject to credits, circuit breakers, and in-flight limits.
func (d *Dispatcher) Dispatch(event *types.Event) error {
	start := time.Now()
	defer func() {
		metrics.ObserveDispatch(strconv.FormatInt(int64(event.GetPartitionId()), 10), time.Since(start))
	}()

	d.partitionsMu.RLock()
	allSubs := d.partitionSubs[event.GetPartitionId()]
	d.partitionsMu.RUnlock()

	if len(allSubs) == 0 {
		return nil
	}

	// Fast path: single subscriber (very common case)
	// Avoids map allocation from groupSubscriptionsByConsumer entirely.
	if len(allSubs) == 1 {
		return d.dispatchToSub(allSubs[0], event)
	}

	consumerGroupSubs := groupSubscriptionsByConsumer(allSubs)

	for groupID, groupSubs := range consumerGroupSubs {
		startIdx := d.nextGroupStart(groupID, len(groupSubs))
		selectedSub := d.pickSubscriber(groupSubs, startIdx)
		if selectedSub == nil {
			log.Printf("[DISPATCHER] No subscriber with credits in group %s for event %s",
				groupID, event.GetMessageId())
			continue
		}

		delivery := deliveryMessagePool.Get().(*DeliveryMessage)
		delivery.Event = event
		delivery.DeliveryID = makeDeliveryID(selectedSub.ID, event.Offset)
		delivery.Attempt = 1
		delivery.AckTimeout = int32(d.config.DefaultAckTimeout / time.Millisecond)
		delivery.Batch = nil

		if !d.tryReserveInFlight(1) {
			deliveryMessagePool.Put(delivery)
			d.releaseCredit(selectedSub)
			metrics.IncDispatcherBackpressureSkip(strconv.FormatInt(int64(event.GetPartitionId()), 10), "in_flight_cap", 1)
			return fmt.Errorf("in-flight limit exceeded")
		}

		if err := selectedSub.Stream.Send(delivery); err != nil {
			if err := d.decInFlight(1); err != nil {
				log.Printf("[DISPATCHER] in-flight underflow: %v", err)
			}
			d.releaseCredit(selectedSub)
			deliveryMessagePool.Put(delivery)
			log.Printf("[DISPATCHER] Failed to send to subscriber %s: %v", selectedSub.ID, err)
			continue
		}

		d.trackDelivery(delivery, selectedSub)
	}

	return nil
}

// dispatchToSub handles the fast path for a single subscriber.
// No map allocation, no round-robin selection needed.
func (d *Dispatcher) dispatchToSub(sub *Subscription, event *types.Event) error {
	// Circuit breaker check: skip open circuits without consuming credits
	if sub.circuitBreaker != nil && !sub.circuitBreaker.CanTry() {
		return nil
	}

	if !d.tryConsumeCredit(sub) {
		return nil // No credits available
	}

	delivery := deliveryMessagePool.Get().(*DeliveryMessage)
	delivery.Event = event
	delivery.DeliveryID = makeDeliveryID(sub.ID, event.Offset)
	delivery.Attempt = 1
	delivery.AckTimeout = int32(d.config.DefaultAckTimeout / time.Millisecond)
	delivery.Batch = nil

	if !d.tryReserveInFlight(1) {
		deliveryMessagePool.Put(delivery)
		d.releaseCredit(sub)
		return fmt.Errorf("in-flight limit exceeded")
	}

	if err := sub.Stream.Send(delivery); err != nil {
		if err := d.decInFlight(1); err != nil {
			log.Printf("[DISPATCHER] in-flight underflow: %v", err)
		}
		d.releaseCredit(sub)
		deliveryMessagePool.Put(delivery)
		if sub.circuitBreaker != nil {
			sub.circuitBreaker.RecordFailure(
				d.config.CircuitBreakerFailureThreshold,
				d.config.CircuitBreakerMinAttempts,
				d.config.CircuitBreakerOpenDurationMs,
			)
		}
		log.Printf("[DISPATCHER] Failed to send to subscriber %s: %v", sub.ID, err)
		return nil
	}

	if sub.circuitBreaker != nil {
		sub.circuitBreaker.RecordSuccess()
	}
	d.trackDelivery(delivery, sub)
	return nil
}

// DispatchBatch fans out a batch of events, grouping by partition and building
// per-subscriber batches for efficient stream sends.
func (d *Dispatcher) DispatchBatch(events []*types.Event) error {
	if len(events) == 0 {
		return nil
	}

	// Support mixed-partition batches without additional caller assumptions.
	byPartition := acquirePartitionEventMap()
	defer releasePartitionEventMap(byPartition)
	for _, ev := range events {
		byPartition[ev.GetPartitionId()] = append(byPartition[ev.GetPartitionId()], ev)
	}

	for partitionID, partitionEvents := range byPartition {
		if err := d.dispatchPartitionBatch(partitionID, partitionEvents); err != nil {
			return err
		}
	}

	return nil
}

func (d *Dispatcher) dispatchPartitionBatch(partitionID int32, events []*types.Event) error {
	start := time.Now()
	defer func() {
		metrics.ObserveDispatch(strconv.FormatInt(int64(partitionID), 10), time.Since(start))
	}()

	d.partitionsMu.RLock()
	allSubs := d.partitionSubs[partitionID]
	d.partitionsMu.RUnlock()

	if len(allSubs) == 0 {
		return nil
	}

	consumerGroupSubs := groupSubscriptionsByConsumer(allSubs)
	defer releaseGroupSubsMap(consumerGroupSubs)

	// Snapshot the persistent per-group cursors ONCE for the whole batch and
	// rotate them in a lock-free local map. This reduces mutex acquisitions
	// from O(numEvents × numGroups) per batch down to 2 (snapshot + commit).
	// Cross-batch fairness is preserved because we commit the final positions
	// back to the persistent store after the loop.
	groupIDs := acquireStringSlice()
	defer releaseStringSlice(groupIDs)
	numSubsByGroup := acquireIntMap()
	defer releaseIntMap(numSubsByGroup)
	for gid, subs := range consumerGroupSubs {
		groupIDs = append(groupIDs, gid)
		numSubsByGroup[gid] = len(subs)
	}
	localCursors := d.snapshotGroupCursors(groupIDs)
	defer releaseIntMap(localCursors)

	batchesBySub := acquireBatchesBySubMap()
	defer releaseBatchesBySubMap(batchesBySub)

	for _, event := range events {
		for groupID, groupSubs := range consumerGroupSubs {
			if len(groupSubs) == 0 {
				continue
			}

			numSubs := numSubsByGroup[groupID]
			start := localCursors[groupID] % numSubs
			localCursors[groupID] = start + 1

			selectedSub := d.pickSubscriber(groupSubs, start)
			if selectedSub == nil {
				// No subscriber in this group currently has credits. The event is
				// not delivered on this pass; surface it as backpressure rather than
				// dropping silently. It remains durable in the WAL and is re-driven
				// when the consumer reconnects from its committed offset.
				metrics.IncDispatcherBackpressureSkip(strconv.FormatInt(int64(partitionID), 10), "no_credits", 1)
				continue
			}

			batchesBySub[selectedSub] = append(batchesBySub[selectedSub], event)
		}
	}

	// Commit the locally advanced cursor positions back to the persistent store
	// so the next batch continues round-robin from where this one left off.
	d.advanceGroupCursors(localCursors, numSubsByGroup)

	for sub, batchEvents := range batchesBySub {
		if len(batchEvents) == 0 {
			continue
		}

		delivery := deliveryMessagePool.Get().(*DeliveryMessage)
		delivery.Event = nil
		delivery.Batch = batchEvents
		if len(batchEvents) == 1 {
			// Singleton batches are common; expose the event directly so
			// single-event consumers see it in Delivery.Event.
			delivery.Event = batchEvents[0]
			delivery.Batch = nil
		}
		delivery.DeliveryID = makeDeliveryIDBatch(sub.ID, batchEvents[0].Offset, len(batchEvents))
		delivery.Attempt = 1
		delivery.AckTimeout = int32(d.config.DefaultAckTimeout / time.Millisecond)

		if !d.tryReserveInFlight(1) {
			deliveryMessagePool.Put(delivery)
			// Credits were consumed per event while assigning.
			d.releaseCredits(sub, int32(len(batchEvents)))
			metrics.IncDispatcherBackpressureSkip(strconv.FormatInt(int64(partitionID), 10), "in_flight_cap", len(batchEvents))
			return fmt.Errorf("in-flight limit exceeded")
		}

		if err := sub.Stream.Send(delivery); err != nil {
			if err := d.decInFlight(1); err != nil {
				log.Printf("[DISPATCHER] in-flight underflow: %v", err)
			}
			d.releaseCredits(sub, int32(len(batchEvents)))
			deliveryMessagePool.Put(delivery)
			if sub.circuitBreaker != nil {
				sub.circuitBreaker.RecordFailure(
					d.config.CircuitBreakerFailureThreshold,
					d.config.CircuitBreakerMinAttempts,
					d.config.CircuitBreakerOpenDurationMs,
				)
			}
			log.Printf("[DISPATCHER] Failed to send batch to %s: %v", sub.ID, err)
			continue
		}

		if sub.circuitBreaker != nil {
			sub.circuitBreaker.RecordSuccess()
		}
		d.trackDelivery(delivery, sub)
	}

	return nil
}

// trackDelivery tracks an active delivery.
func (d *Dispatcher) trackDelivery(delivery *DeliveryMessage, sub *Subscription) {
	creditsConsumed := int32(1)
	if len(delivery.Batch) > 0 {
		creditsConsumed = int32(len(delivery.Batch))
	}

	// Single time.Now() call instead of two separate calls
	now := time.Now()

	ackDeadline := now.Add(d.config.DefaultAckTimeout)

	shard := d.getShard(sub.ID)
	shard.mu.Lock()
	shard.activeDeliveries[delivery.DeliveryID] = &ActiveDelivery{
		Delivery:        delivery,
		Subscription:    sub,
		Attempt:         delivery.Attempt,
		CreatedTS:       now.UnixMilli(),
		AckDeadline:     ackDeadline,
		CreditsConsumed: creditsConsumed,
	}
	// Index by deadline for O(k log n) expiry. A retry re-tracks the same
	// delivery id, which updates the existing deadline in place.
	shard.expiry.add(delivery.DeliveryID, ackDeadline.UnixNano())
	shard.mu.Unlock()
}

// deliveryMessagePool pools DeliveryMessage structs to eliminate per-dispatch allocations.
var deliveryMessagePool = sync.Pool{
	New: func() interface{} {
		return &DeliveryMessage{}
	},
}

// maxDeliveryIDBuf is the on-stack buffer size used to build delivery IDs
// before copying them into a real string. It is intentionally not a
// zero-allocation path: the returned string escapes (it is stored as a map key
// in activeDeliveries and passed to the DLQ), so returning a view into a stack
// buffer would be a use-after-free.
const maxDeliveryIDBuf = 128

// makeDeliveryID creates a delivery ID of the form "<subID>-<offset>".
func makeDeliveryID(subID string, offset int64) string {
	need := len(subID) + 1 + 20 // subID + '-' + max int64 digits
	if need <= maxDeliveryIDBuf {
		var buf [maxDeliveryIDBuf]byte
		n := copy(buf[:], subID)
		buf[n] = '-'
		n++
		b := strconv.AppendInt(buf[n:n], offset, 10)
		n += len(b)
		return string(buf[:n]) // one alloc; the previous unsafe.String here was unsound
	}
	b := make([]byte, 0, need)
	b = append(b, subID...)
	b = append(b, '-')
	b = strconv.AppendInt(b, offset, 10)
	return string(b)
}

// makeDeliveryIDBatch creates a batch delivery ID of the form
// "<subID>-batch-<offset>-<count>".
func makeDeliveryIDBatch(subID string, offset int64, count int) string {
	need := len(subID) + 7 + 20 + 1 + 10 // subID + "-batch-" + offset + '-' + count
	if need <= maxDeliveryIDBuf {
		var buf [maxDeliveryIDBuf]byte
		n := copy(buf[:], subID)
		n += copy(buf[n:], "-batch-")
		b := strconv.AppendInt(buf[n:n], offset, 10)
		n += len(b)
		buf[n] = '-'
		n++
		b2 := strconv.AppendInt(buf[n:n], int64(count), 10)
		n += len(b2)
		return string(buf[:n]) // one alloc; the previous unsafe.String here was unsound
	}
	b := make([]byte, 0, need)
	b = append(b, subID...)
	b = append(b, "-batch-"...)
	b = strconv.AppendInt(b, offset, 10)
	b = append(b, '-')
	b = strconv.AppendInt(b, int64(count), 10)
	return string(b)
}

func (d *Dispatcher) timeoutLoop() {
	defer d.wg.Done()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			now := time.Now()
			d.scanExpiredDeliveries(now)
			d.processRetries(now)
		case <-d.quit:
			return
		}
	}
}

func (d *Dispatcher) scanExpiredDeliveries(now time.Time) {
	nowNano := now.UnixNano()
	for _, shard := range d.shards {
		shard.mu.Lock()
		// Pop only the deliveries whose deadline has passed; the heap yields them
		// in deadline order and stops at the first entry still in the future.
		for {
			deliveryID, ok := shard.expiry.popExpired(nowNano)
			if !ok {
				break
			}
			active, exists := shard.activeDeliveries[deliveryID]
			if !exists {
				// Already acked/removed; the expiry entry was stale. (Removal on
				// ack normally prevents this, but stay defensive.)
				continue
			}
			delete(shard.activeDeliveries, deliveryID)
			if err := d.decInFlight(1); err != nil {
				log.Printf("[DISPATCHER] in-flight underflow: %v", err)
			}

			if active.Attempt < d.config.MaxRetries {
				// Non-blocking: push to retry heap instead of sleeping inline
				backoff := time.Duration(active.Attempt) * d.config.RetryBackoff
				d.retryHeap.PushEntry(NewRetryEntry(active, backoff))
			} else {
				d.releaseCredits(active.Subscription, active.CreditsConsumed)
				d.sendToDLQ(active, "delivery timeout after max retries")
			}
		}
		shard.mu.Unlock()
	}
}

// processRetries dispatches any retry entries whose backoff has elapsed.
func (d *Dispatcher) processRetries(now time.Time) {
	entries := d.retryHeap.Due(now.UnixMilli())
	if len(entries) == 0 {
		return
	}

	for _, entry := range entries {
		active := entry.active
		if active == nil || active.Subscription == nil {
			continue
		}

		// Check circuit breaker before retrying
		if active.Subscription.circuitBreaker != nil && !active.Subscription.circuitBreaker.CanTry() {
			// Circuit still open — re-queue with a meaningful backoff (5s) to avoid busy-loop
			d.retryHeap.PushEntry(NewRetryEntry(active, 5*time.Second))
			continue
		}

		// Attempt redelivery without sleep
		retryDelivery := deliveryMessagePool.Get().(*DeliveryMessage)
		retryDelivery.Event = active.Delivery.Event
		retryDelivery.DeliveryID = active.Delivery.DeliveryID
		retryDelivery.Attempt = active.Attempt + 1
		retryDelivery.AckTimeout = active.Delivery.AckTimeout
		retryDelivery.Batch = active.Delivery.Batch

		if !d.tryReserveInFlight(1) {
			// Re-queue with short backoff to try later
			deliveryMessagePool.Put(retryDelivery)
			d.retryHeap.PushEntry(NewRetryEntry(active, time.Second))
			continue
		}

		if err := active.Subscription.Stream.Send(retryDelivery); err != nil {
			if err := d.decInFlight(1); err != nil {
				log.Printf("[DISPATCHER] in-flight underflow: %v", err)
			}
			deliveryMessagePool.Put(retryDelivery)
			if active.Subscription.circuitBreaker != nil {
				active.Subscription.circuitBreaker.RecordFailure(
					d.config.CircuitBreakerFailureThreshold,
					d.config.CircuitBreakerMinAttempts,
					d.config.CircuitBreakerOpenDurationMs,
				)
			}

			if retryDelivery.Attempt < d.config.MaxRetries {
				// Re-queue with backoff
				backoff := time.Duration(retryDelivery.Attempt) * d.config.RetryBackoff
				d.retryHeap.PushEntry(NewRetryEntry(active, backoff))
			} else {
				d.releaseCredits(active.Subscription, active.CreditsConsumed)
				d.sendToDLQ(active, fmt.Sprintf("retry failed after max retries: %v", err))
			}
			continue
		}

		if active.Subscription.circuitBreaker != nil {
			active.Subscription.circuitBreaker.RecordSuccess()
		}
		d.trackDelivery(retryDelivery, active.Subscription)
	}
}

// tryReserveInFlight atomically reserves capacity in the global in-flight budget.
func (d *Dispatcher) tryReserveInFlight(n int64) bool {
	for {
		current := d.inFlightCount.Load()
		if d.config.MaxInFlightEvents > 0 && current+n > d.config.MaxInFlightEvents {
			return false
		}
		if d.inFlightCount.CompareAndSwap(current, current+n) {
			return true
		}
	}
}

func (d *Dispatcher) decInFlight(n int64) error {
	for {
		current := d.inFlightCount.Load()
		next := current - n
		if next < 0 {
			return fmt.Errorf("in-flight underflow: attempted to decrement %d by %d", current, n)
		}
		if d.inFlightCount.CompareAndSwap(current, next) {
			return nil
		}
	}
}

func (d *Dispatcher) getInFlight() int64 {
	return d.inFlightCount.Load()
}

func parseSubIDFromDeliveryID(deliveryID string) (string, error) {
	if deliveryID == "" {
		return "", fmt.Errorf("invalid delivery ID format")
	}

	// Batch format: "<subID>-batch-<offset>-<count>"
	if batchIdx := strings.LastIndex(deliveryID, "-batch-"); batchIdx != -1 {
		suffix := deliveryID[batchIdx+len("-batch-"):]
		sep := strings.LastIndexByte(suffix, '-')
		if sep <= 0 || sep == len(suffix)-1 {
			return "", fmt.Errorf("invalid delivery ID format")
		}
		if _, err := strconv.ParseInt(suffix[:sep], 10, 64); err != nil {
			return "", fmt.Errorf("invalid delivery ID format")
		}
		if _, err := strconv.ParseInt(suffix[sep+1:], 10, 64); err != nil {
			return "", fmt.Errorf("invalid delivery ID format")
		}
		subID := deliveryID[:batchIdx]
		if subID == "" {
			return "", fmt.Errorf("invalid delivery ID format")
		}
		return subID, nil
	}

	// Single format: "<subID>-<offset>"
	idx := strings.LastIndexByte(deliveryID, '-')
	if idx <= 0 || idx == len(deliveryID)-1 {
		return "", fmt.Errorf("invalid delivery ID format")
	}
	if _, err := strconv.ParseInt(deliveryID[idx+1:], 10, 64); err != nil {
		return "", fmt.Errorf("invalid delivery ID format")
	}
	return deliveryID[:idx], nil
}

// HandleAck processes a subscriber acknowledgment. Success releases credits and
// updates NextOffset; failure schedules a retry or sends the delivery to the DLQ.
func (d *Dispatcher) HandleAck(deliveryID string, success bool, nextOffset int64) error {
	subID, err := parseSubIDFromDeliveryID(deliveryID)
	if err != nil {
		return err
	}

	shard := d.getShard(subID)
	shard.mu.Lock()
	active, exists := shard.activeDeliveries[deliveryID]
	if !exists {
		shard.mu.Unlock()
		return nil
	}
	delete(shard.activeDeliveries, deliveryID)
	shard.expiry.remove(deliveryID)
	shard.mu.Unlock()

	if err := d.decInFlight(1); err != nil {
		log.Printf("[DISPATCHER] in-flight underflow: %v", err)
	}

	if success {
		active.Subscription.NextOffset = nextOffset
		d.releaseCredits(active.Subscription, active.CreditsConsumed)
		if active.Subscription.circuitBreaker != nil {
			active.Subscription.circuitBreaker.RecordSuccess()
		}
		d.notifyDeliveryComplete(active)
		return nil
	}

	if active.Subscription.circuitBreaker != nil {
		active.Subscription.circuitBreaker.RecordFailure(
			d.config.CircuitBreakerFailureThreshold,
			d.config.CircuitBreakerMinAttempts,
			d.config.CircuitBreakerOpenDurationMs,
		)
	}

	if active.Attempt < d.config.MaxRetries {
		if retryErr := d.retryDelivery(active); retryErr != nil {
			d.releaseCredits(active.Subscription, active.CreditsConsumed)
			d.sendToDLQ(active, fmt.Sprintf("ack failure; retry failed: %v", retryErr))
			return retryErr
		}
		return nil
	}

	d.releaseCredits(active.Subscription, active.CreditsConsumed)
	d.sendToDLQ(active, "ack failure after max retries")
	return nil
}

// notifyDeliveryComplete invokes the OnDeliveryComplete callback with the tenant ID.
func (d *Dispatcher) notifyDeliveryComplete(active *ActiveDelivery) {
	if d.OnDeliveryComplete == nil || active == nil || active.Delivery == nil || active.Delivery.Event == nil {
		return
	}
	if tenantID, ok := active.Delivery.Event.Meta["tenant_id"]; ok && tenantID != "" {
		d.OnDeliveryComplete(tenantID)
	}
}

// sendToDLQ sends a failed delivery to the dead-letter queue.
func (d *Dispatcher) sendToDLQ(active *ActiveDelivery, reason string) {
	d.notifyDeliveryComplete(active)
	if d.dlq == nil {
		log.Printf("[DISPATCHER] DLQ not configured, dropping failed delivery %s: %s",
			active.Delivery.DeliveryID, reason)
		return
	}

	subscriberID := ""
	if active.Subscription != nil {
		subscriberID = active.Subscription.ID
	}

	if err := d.dlq.Add(
		active.Delivery.Event,
		active.Delivery.DeliveryID,
		active.Attempt,
		reason,
		subscriberID,
	); err != nil {
		log.Printf("[DISPATCHER] Failed to add to DLQ: %v", err)
	} else {
		log.Printf("[DISPATCHER] Added to DLQ: delivery=%s, reason=%s",
			active.Delivery.DeliveryID, reason)
	}
}

// retryDelivery retries a failed delivery.
func (d *Dispatcher) retryDelivery(active *ActiveDelivery) error {
	retryDelivery := deliveryMessagePool.Get().(*DeliveryMessage)
	retryDelivery.Event = active.Delivery.Event
	retryDelivery.DeliveryID = active.Delivery.DeliveryID
	retryDelivery.Attempt = active.Attempt + 1
	retryDelivery.AckTimeout = active.Delivery.AckTimeout
	retryDelivery.Batch = active.Delivery.Batch

	if !d.tryReserveInFlight(1) {
		deliveryMessagePool.Put(retryDelivery)
		return fmt.Errorf("in-flight limit exceeded on retry")
	}

	backoff := time.Duration(active.Attempt) * d.config.RetryBackoff
	if backoff > 0 {
		timer := time.NewTimer(backoff)
		select {
		case <-timer.C:
			timer.Stop()
		case <-d.quit:
			timer.Stop()
			if err := d.decInFlight(1); err != nil {
				log.Printf("[DISPATCHER] in-flight underflow: %v", err)
			}
			deliveryMessagePool.Put(retryDelivery)
			return fmt.Errorf("dispatcher closing")
		}
	}

	if err := active.Subscription.Stream.Send(retryDelivery); err != nil {
		if err := d.decInFlight(1); err != nil {
			log.Printf("[DISPATCHER] in-flight underflow: %v", err)
		}
		deliveryMessagePool.Put(retryDelivery)
		return fmt.Errorf("resend failed: %w", err)
	}

	d.trackDelivery(retryDelivery, active.Subscription)
	return nil
}

// GetStats returns dispatcher statistics.
func (d *Dispatcher) GetStats() *DispatcherStats {
	stats := &DispatcherStats{}
	for _, shard := range d.shards {
		shard.mu.RLock()
		stats.ActiveSubscriptions += int64(len(shard.subscriptions))
		stats.ActiveDeliveries += int64(len(shard.activeDeliveries))

		for _, sub := range shard.subscriptions {
			credits := atomic.LoadInt32(&sub.Credits)
			stats.CreditsInUse += int64(sub.MaxCredits - credits)
			stats.CreditsAvailable += int64(credits)
			if sub.circuitBreaker != nil && !sub.circuitBreaker.CanTry() {
				stats.OpenCircuitBreakers++
			}
		}
		shard.mu.RUnlock()
	}
	stats.RetryQueueDepth = int64(d.retryHeap.Len())
	if d.dlq != nil {
		stats.DLQSize = int64(d.dlq.Count())
	}
	return stats
}

// DispatcherStats is a point-in-time snapshot of dispatcher load.
type DispatcherStats struct {
	// ActiveSubscriptions is the number of registered subscriptions.
	ActiveSubscriptions int64
	// ActiveDeliveries is the number of in-flight deliveries awaiting ack.
	ActiveDeliveries int64
	// CreditsInUse is MaxCredits minus currently available credits, summed across subs.
	CreditsInUse int64
	// CreditsAvailable is the sum of remaining credits across all subscriptions.
	CreditsAvailable int64
	// RetryQueueDepth is the number of entries waiting in the retry heap.
	RetryQueueDepth int64
	// DLQSize is the number of entries in the dead-letter queue (0 if unset).
	DLQSize int64
	// OpenCircuitBreakers counts subscriptions whose breaker is open or half-open (CanTry false).
	OpenCircuitBreakers int64
}

// Drain waits for all active deliveries to complete or the timeout to expire.
func (d *Dispatcher) Drain(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		stats := d.GetStats()
		if stats.ActiveDeliveries == 0 {
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return fmt.Errorf("drain timeout: %d deliveries still active", d.GetStats().ActiveDeliveries)
}

// Close closes the dispatcher and cleans up all resources. Safe to call multiple times.
func (d *Dispatcher) Close() {
	d.quitOnce.Do(func() { close(d.quit) })
	d.wg.Wait()

	for _, shard := range d.shards {
		shard.mu.Lock()
		for deliveryID := range shard.activeDeliveries {
			if err := d.decInFlight(1); err != nil {
				log.Printf("[DISPATCHER] in-flight underflow: %v", err)
			}
			delete(shard.activeDeliveries, deliveryID)
		}
		shard.expiry = newDeliveryExpiry()
		shard.subscriptions = make(map[string]*Subscription)
		shard.mu.Unlock()
	}
}

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

// Dispatcher manages event delivery to subscribers.
// It is optimized with sharding for reduced lock contention.
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
	// single-event batches (batchSize=1) still rotate across subscribers.
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

// DispatcherShard represents a shard of the dispatcher.
type DispatcherShard struct {
	mu               sync.RWMutex
	subscriptions    map[string]*Subscription
	activeDeliveries map[string]*ActiveDelivery
	dlq              *DeadLetterQueue
}

// Subscription represents a subscriber.
type Subscription struct {
	ID            string
	ConsumerGroup string
	Partition     *types.Partition
	NextOffset    int64
	Credits       int32
	MaxCredits    int32
	Stream        Stream
	CreatedTS     int64

	// Circuit breaker protects against repeatedly sending to failed consumers.
	circuitBreaker *CircuitBreaker
}

// Stream represents gRPC stream.
type Stream interface {
	Send(delivery *DeliveryMessage) error
	Recv() (*Control, error)
	Context() context.Context
}

// DeliveryMessage represents a delivery to subscriber.
type DeliveryMessage struct {
	Event      *types.Event
	DeliveryID string
	Attempt    int32
	AckTimeout int32
	Batch      []*types.Event // For batched delivery
}

// DeliveryControl represents control message from subscriber.
type Control struct {
	Ack    *AckMessage
	Credit *CreditMessage
}

// AckMessage represents acknowledgment.
type AckMessage struct {
	DeliveryID string
	Success    bool
	Error      string
	NextOffset int64
}

// CreditMessage represents flow control credit.
type CreditMessage struct {
	Credits int32
}

// ActiveDelivery represents an active delivery.
type ActiveDelivery struct {
	Delivery        *DeliveryMessage
	Subscription    *Subscription
	Attempt         int32
	CreatedTS       int64
	AckDeadline     time.Time
	CreditsConsumed int32
}

// Config represents dispatcher configuration.
type Config struct {
	MaxRetries         int32
	DefaultAckTimeout  time.Duration
	MaxDeliveryCredits int32
	RetryBackoff       time.Duration
	MaxInFlightEvents  int64 // Global cap on in-flight deliveries across all subscriptions

	// Circuit breaker configuration
	CircuitBreakerFailureThreshold float64 // 0.0-1.0, 1.0 = disabled
	CircuitBreakerOpenDurationMs   int64
	CircuitBreakerMinAttempts      int64
}

// DefaultConfig returns default config.
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

// NewDispatcher creates a new dispatcher with sharding.
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

// Subscribe adds a subscription.
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

// Unsubscribe removes a subscription.
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

func (d *Dispatcher) nextGroupStart(groupID string, numSubs int) int {
	d.groupCursorMu.Lock()
	defer d.groupCursorMu.Unlock()

	start := d.groupCursor[groupID]
	d.groupCursor[groupID] = (start + 1) % numSubs
	return start
}

func groupSubscriptionsByConsumer(allSubs []*Subscription) map[string][]*Subscription {
	// Fast path: if all subs share the same consumer group, skip map allocation
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
			return map[string][]*Subscription{firstGroup: allSubs}
		}
	}

	consumerGroupSubs := make(map[string][]*Subscription)
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

// Dispatch dispatches an event to subscribers.
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

// DispatchBatch dispatches a batch of events.
func (d *Dispatcher) DispatchBatch(events []*types.Event) error {
	if len(events) == 0 {
		return nil
	}

	// Support mixed-partition batches without additional caller assumptions.
	byPartition := make(map[int32][]*types.Event)
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

	batchesBySub := make(map[*Subscription][]*types.Event)

	for _, event := range events {
		for groupID, groupSubs := range consumerGroupSubs {
			if len(groupSubs) == 0 {
				continue
			}

			start := d.nextGroupStart(groupID, len(groupSubs))

			selectedSub := d.pickSubscriber(groupSubs, start)
			if selectedSub == nil {
				continue
			}

			batchesBySub[selectedSub] = append(batchesBySub[selectedSub], event)
		}
	}

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

	shard := d.getShard(sub.ID)
	shard.mu.Lock()
	shard.activeDeliveries[delivery.DeliveryID] = &ActiveDelivery{
		Delivery:        delivery,
		Subscription:    sub,
		Attempt:         delivery.Attempt,
		CreatedTS:       now.UnixMilli(),
		AckDeadline:     now.Add(d.config.DefaultAckTimeout),
		CreditsConsumed: creditsConsumed,
	}
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
	for _, shard := range d.shards {
		shard.mu.Lock()
		for deliveryID, active := range shard.activeDeliveries {
			if now.After(active.AckDeadline) {
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

// HandleAck handles acknowledgment from subscriber.
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

// DispatcherStats represents dispatcher statistics.
type DispatcherStats struct {
	ActiveSubscriptions int64
	ActiveDeliveries    int64
	CreditsInUse        int64
	CreditsAvailable    int64
	RetryQueueDepth     int64
	DLQSize             int64
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
		shard.subscriptions = make(map[string]*Subscription)
		shard.mu.Unlock()
	}
}

package delivery

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"cronos_db/pkg/types"
)

// Dispatcher manages event delivery to subscribers
// Optimized with sharding for reduced lock contention
type Dispatcher struct {
	shards    []*DispatcherShard
	shardCount int
	config    *Config
	dlq       *DeadLetterQueue
	quit      chan struct{}
}

// DispatcherShard represents a shard of the dispatcher
type DispatcherShard struct {
	mu               sync.RWMutex
	subscriptions    map[string]*Subscription
	activeDeliveries map[string]*ActiveDelivery
	dlq              *DeadLetterQueue
}

// Subscription represents a subscriber
type Subscription struct {
	ID            string
	ConsumerGroup string
	Partition     *types.Partition
	NextOffset    int64
	Credits       int32
	MaxCredits    int32
	Stream        Stream
	CreatedTS     int64
}

// Stream represents gRPC stream
type Stream interface {
	Send(delivery *DeliveryMessage) error
	Recv() (*Control, error)
	Context() context.Context
}

// DeliveryMessage represents a delivery to subscriber
type DeliveryMessage struct {
	Event      *types.Event
	DeliveryID string
	Attempt    int32
	AckTimeout int32
	Batch      []*types.Event // For batched delivery
}

// DeliveryControl represents control message from subscriber
type Control struct {
	Ack    *AckMessage
	Credit *CreditMessage
}

// AckMessage represents acknowledgment
type AckMessage struct {
	DeliveryID string
	Success    bool
	Error      string
	NextOffset int64
}

// CreditMessage represents flow control credit
type CreditMessage struct {
	Credits int32
}

// ActiveDelivery represents an active delivery
type ActiveDelivery struct {
	Delivery     *DeliveryMessage
	Subscription *Subscription
	Attempt      int32
	CreatedTS    int64
	AckDeadline  time.Time
	Timer        *time.Timer
	quit         chan struct{} // Quit channel for timeout goroutine
}

// Config represents dispatcher configuration
type Config struct {
	MaxRetries         int32
	DefaultAckTimeout  time.Duration
	MaxDeliveryCredits int32
	RetryBackoff       time.Duration
}

// DefaultConfig returns default config
func DefaultConfig() *Config {
	return &Config{
		MaxRetries:         5,
		DefaultAckTimeout:  30 * time.Second,
		MaxDeliveryCredits: 1000,
		RetryBackoff:       1 * time.Second,
	}
}

// NewDispatcher creates a new dispatcher with sharding
func NewDispatcher(config *Config) *Dispatcher {
	shardCount := 32 // Default shard count
	shards := make([]*DispatcherShard, shardCount)
	for i := 0; i < shardCount; i++ {
		shards[i] = &DispatcherShard{
			subscriptions:    make(map[string]*Subscription),
			activeDeliveries: make(map[string]*ActiveDelivery),
		}
	}

	return &Dispatcher{
		shards:     shards,
		shardCount: shardCount,
		config:     config,
		dlq:        nil,
		quit:       make(chan struct{}),
	}
}

// NewDispatcherWithDLQ creates a new dispatcher with a dead-letter queue
func NewDispatcherWithDLQ(config *Config, dlq *DeadLetterQueue) *Dispatcher {
	d := NewDispatcher(config)
	d.SetDLQ(dlq)
	return d
}

// SetDLQ sets the dead-letter queue
func (d *Dispatcher) SetDLQ(dlq *DeadLetterQueue) {
	d.dlq = dlq
	for _, shard := range d.shards {
		shard.dlq = dlq
	}
}

// getShard returns the shard for a given key
func (d *Dispatcher) getShard(key string) *DispatcherShard {
	// Simple hash
	var h uint32
	for i := 0; i < len(key); i++ {
		h = 31*h + uint32(key[i])
	}
	return d.shards[h%uint32(d.shardCount)]
}

// Subscribe adds a subscription
func (d *Dispatcher) Subscribe(sub *Subscription) error {
	// Shard by subscription ID
	shard := d.getShard(sub.ID)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	log.Printf("[DISPATCHER] Subscribe: Registering subscription %s (partition=%d, maxCredits=%d)",
		sub.ID, sub.Partition.ID, sub.MaxCredits)

	// Check if subscription already exists
	if _, exists := shard.subscriptions[sub.ID]; exists {
		return fmt.Errorf("subscription %s already exists", sub.ID)
	}

	// Set initial credits
	if sub.MaxCredits == 0 {
		sub.MaxCredits = d.config.MaxDeliveryCredits
	}
	sub.Credits = sub.MaxCredits

	shard.subscriptions[sub.ID] = sub
	log.Printf("[DISPATCHER] Subscribe: Successfully registered subscription %s", sub.ID)
	return nil
}

// Unsubscribe removes a subscription
func (d *Dispatcher) Unsubscribe(subscriptionID string) error {
	shard := d.getShard(subscriptionID)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	_, exists := shard.subscriptions[subscriptionID]
	if !exists {
		return fmt.Errorf("subscription %s not found", subscriptionID)
	}

	// Cancel active deliveries
	for deliveryID, active := range shard.activeDeliveries {
		if active.Subscription.ID == subscriptionID {
			if active.Timer != nil {
				active.Timer.Stop()
			}
			delete(shard.activeDeliveries, deliveryID)
		}
	}

	delete(shard.subscriptions, subscriptionID)
	return nil
}

// Dispatch dispatches an event to subscribers
func (d *Dispatcher) Dispatch(event *types.Event) error {
	// Iterate all shards to find subscriptions for this partition
	// Optimization: Partition to Shard mapping?
	// For now, we iterate, which is slightly less efficient but safer for "find all subscriptions"
	// However, usually one Dispatcher per partition in the architecture?
	// If Dispatcher is shared, we must lock shards.

	// Better: We are dispatching ONE event. We need to find matching subscriptions.
	// Subscriptions are scattered across shards.

	var allSubs []*Subscription

	for _, shard := range d.shards {
		shard.mu.RLock()
		for _, sub := range shard.subscriptions {
			if sub.Partition.ID == event.GetPartitionId() {
				allSubs = append(allSubs, sub)
			}
		}
		shard.mu.RUnlock()
	}

	if len(allSubs) == 0 {
		return nil
	}

	// Group subscriptions by consumer group
	consumerGroupSubs := make(map[string][]*Subscription)
	for _, sub := range allSubs {
		consumerGroupSubs[sub.ConsumerGroup] = append(consumerGroupSubs[sub.ConsumerGroup], sub)
	}

	// For each consumer group, pick ONE subscriber
	for groupID, groupSubs := range consumerGroupSubs {
		var selectedSub *Subscription
		startIdx := int(event.Offset % int64(len(groupSubs)))

		for i := 0; i < len(groupSubs); i++ {
			idx := (startIdx + i) % len(groupSubs)
			// Lock shard to check credits safely
			sSub := groupSubs[idx]
			sShard := d.getShard(sSub.ID)

			// We need to lock to check and decrement credits.
			// To avoid deadlock, we tryLock or just lock briefly.
			sShard.mu.Lock()
			if sSub.Credits > 0 {
				selectedSub = sSub
				sSub.Credits-- // Decrement immediately
				sShard.mu.Unlock()
				break
			}
			sShard.mu.Unlock()
		}

		if selectedSub == nil {
			log.Printf("[DISPATCHER] No subscriber with credits in group %s for event %s",
				groupID, event.GetMessageId())
			continue
		}

		// Create delivery
		delivery := &DeliveryMessage{
			Event:      event,
			DeliveryID: fmt.Sprintf("%s-%d", selectedSub.ID, event.Offset),
			Attempt:    1,
			AckTimeout: int32(d.config.DefaultAckTimeout / time.Millisecond),
		}

		// Send to subscriber
		if err := selectedSub.Stream.Send(delivery); err != nil {
			log.Printf("[DISPATCHER] Failed to send to subscriber %s: %v", selectedSub.ID, err)
			continue
		}

		// Track active delivery in the subscriber's shard
		d.trackDelivery(delivery, selectedSub)
	}

	return nil
}

// DispatchBatch dispatches a batch of events
func (d *Dispatcher) DispatchBatch(events []*types.Event) error {
	if len(events) == 0 {
		return nil
	}

	// Optimization: Group events by topic/partition to minimize subscription lookups
	// For simplicity, we process grouping by consumer group

	// Map[ConsumerGroup] -> Map[Subscriber] -> Batch
	batches := make(map[string]map[string][]*types.Event)

	// We need to look up subscriptions for each event or the whole batch if they belong to same partition
	// Assumption: Batch belongs to same partition usually.
	partitionID := events[0].GetPartitionId()

	var allSubs []*Subscription
	for _, shard := range d.shards {
		shard.mu.RLock()
		for _, sub := range shard.subscriptions {
			if sub.Partition.ID == partitionID {
				allSubs = append(allSubs, sub)
			}
		}
		shard.mu.RUnlock()
	}

	if len(allSubs) == 0 {
		return nil
	}

	// Group subs by consumer group
	consumerGroupSubs := make(map[string][]*Subscription)
	for _, sub := range allSubs {
		consumerGroupSubs[sub.ConsumerGroup] = append(consumerGroupSubs[sub.ConsumerGroup], sub)
	}

	// Assign events to subscribers
	for _, event := range events {
		for _, groupSubs := range consumerGroupSubs {
			// Round robin
			startIdx := int(event.Offset % int64(len(groupSubs)))
			var selectedSub *Subscription

			for i := 0; i < len(groupSubs); i++ {
				idx := (startIdx + i) % len(groupSubs)
				sSub := groupSubs[idx]
				sShard := d.getShard(sSub.ID)

				sShard.mu.Lock()
				if sSub.Credits > 0 {
					selectedSub = sSub
					sSub.Credits--
					sShard.mu.Unlock()
					break
				}
				sShard.mu.Unlock()
			}

			if selectedSub != nil {
				if batches[selectedSub.ConsumerGroup] == nil {
					batches[selectedSub.ConsumerGroup] = make(map[string][]*types.Event)
				}
				batches[selectedSub.ConsumerGroup][selectedSub.ID] = append(batches[selectedSub.ConsumerGroup][selectedSub.ID], event)
			}
		}
	}

	// Send batches
	for _, subBatches := range batches {
		for subID, batchEvents := range subBatches {
			// We need the subscriber object. We can find it again or store it.
			// Finding it is safer.
			shard := d.getShard(subID)
			shard.mu.RLock()
			sub, exists := shard.subscriptions[subID]
			shard.mu.RUnlock()

			if !exists {
				continue
			}

			// Create batch delivery
			// We use the new 'Batch' field in proto, or just send individual messages if proto not updated?
			// We updated proto.

			delivery := &DeliveryMessage{
				Event: nil, // Batch mode
				// Use ID of first event for tracking?
				DeliveryID: fmt.Sprintf("%s-batch-%d-%d", sub.ID, batchEvents[0].Offset, len(batchEvents)),
				Attempt:    1,
				AckTimeout: int32(d.config.DefaultAckTimeout / time.Millisecond),
				Batch:      batchEvents,
			}

			if err := sub.Stream.Send(delivery); err != nil {
				log.Printf("[DISPATCHER] Failed to send batch to %s: %v", sub.ID, err)
				continue
			}

			d.trackDelivery(delivery, sub)
		}
	}

	return nil
}

// trackDelivery tracks an active delivery
func (d *Dispatcher) trackDelivery(delivery *DeliveryMessage, sub *Subscription) {
	shard := d.getShard(sub.ID)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	active := &ActiveDelivery{
		Delivery:     delivery,
		Subscription: sub,
		Attempt:      delivery.Attempt,
		CreatedTS:    time.Now().UnixMilli(),
		AckDeadline:  time.Now().Add(d.config.DefaultAckTimeout),
		Timer:        time.NewTimer(d.config.DefaultAckTimeout),
		quit:         make(chan struct{}),
	}

	shard.activeDeliveries[delivery.DeliveryID] = active

	// Wait for ack or timeout
	go func(deliveryID string, timer *time.Timer, quit chan struct{}, s *DispatcherShard) {
		select {
		case <-timer.C:
			d.handleDeliveryTimeout(deliveryID, s)
		case <-quit:
			timer.Stop()
			return
		}
	}(delivery.DeliveryID, active.Timer, active.quit, shard)
}

// HandleAck handles acknowledgment from subscriber
func (d *Dispatcher) HandleAck(deliveryID string, success bool, nextOffset int64) error {
	// We don't know the shard from deliveryID easily unless we parse it or store a map.
	// deliveryID format: "subID-offset". We can extract subID.
	var subID string
	// Find the last index of "-"
	for i := len(deliveryID) - 1; i >= 0; i-- {
		if deliveryID[i] == '-' {
			subID = deliveryID[:i]
			break
		}
	}

	if subID == "" {
		// Fallback: search all shards (expensive) or just fail
		// Let's assume standard format
		return fmt.Errorf("invalid delivery ID format")
	}

	shard := d.getShard(subID)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	active, exists := shard.activeDeliveries[deliveryID]
	if !exists {
		return nil
	}

	// Stop timer
	if active.Timer != nil {
		active.Timer.Stop()
	}
	close(active.quit)

	if success {
		active.Subscription.NextOffset = nextOffset
	}

	active.Subscription.Credits++
	delete(shard.activeDeliveries, deliveryID)

	if !success {
		if active.Attempt < d.config.MaxRetries {
			// Unlock shard before retry to avoid holding lock during sleep/send
			shard.mu.Unlock()
			err := d.retryDelivery(active)
			shard.mu.Lock() // Re-lock
			return err
		}
		d.sendToDLQ(active, "max retries exceeded", shard)
	}

	return nil
}

// handleDeliveryTimeout handles delivery timeout
func (d *Dispatcher) handleDeliveryTimeout(deliveryID string, shard *DispatcherShard) {
	shard.mu.Lock()
	defer shard.mu.Unlock()

	active, exists := shard.activeDeliveries[deliveryID]
	if !exists {
		return
	}

	delete(shard.activeDeliveries, deliveryID)

	if active.Subscription != nil {
		active.Subscription.Credits++
	}

	if active.Attempt < d.config.MaxRetries {
		shard.mu.Unlock()
		d.retryDelivery(active)
		shard.mu.Lock()
	} else {
		d.sendToDLQ(active, "delivery timeout after max retries", shard)
	}
}

// sendToDLQ sends a failed delivery to the dead-letter queue
func (d *Dispatcher) sendToDLQ(active *ActiveDelivery, reason string, shard *DispatcherShard) {
	if shard.dlq == nil {
		log.Printf("[DISPATCHER] DLQ not configured, dropping failed delivery %s: %s",
			active.Delivery.DeliveryID, reason)
		return
	}

	subscriberID := ""
	if active.Subscription != nil {
		subscriberID = active.Subscription.ID
	}

	if err := shard.dlq.Add(
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

// retryDelivery retries a failed delivery
func (d *Dispatcher) retryDelivery(active *ActiveDelivery) error {
	retryDelivery := &DeliveryMessage{
		Event:      active.Delivery.Event,
		DeliveryID: active.Delivery.DeliveryID,
		Attempt:    active.Attempt + 1,
		AckTimeout: active.Delivery.AckTimeout,
	}

	backoff := time.Duration(active.Attempt) * d.config.RetryBackoff
	time.Sleep(backoff)

	if err := active.Subscription.Stream.Send(retryDelivery); err != nil {
		return fmt.Errorf("resend failed: %w", err)
	}

	// We need to re-track this delivery.
	// NOTE: This requires locking the shard again.
	// Since retryDelivery is called when lock is NOT held (in HandleAck/Timeout logic above),
	// we call trackDelivery which takes the lock.

	d.trackDelivery(retryDelivery, active.Subscription)

	return nil
}

// GetStats returns dispatcher statistics
func (d *Dispatcher) GetStats() *DispatcherStats {
	stats := &DispatcherStats{}
	for _, shard := range d.shards {
		shard.mu.RLock()
		stats.ActiveSubscriptions += int64(len(shard.subscriptions))
		stats.ActiveDeliveries += int64(len(shard.activeDeliveries))

		for _, sub := range shard.subscriptions {
			stats.CreditsInUse += int64(sub.MaxCredits - sub.Credits)
			stats.CreditsAvailable += int64(sub.Credits)
		}
		shard.mu.RUnlock()
	}
	return stats
}

// DispatcherStats represents dispatcher statistics
type DispatcherStats struct {
	ActiveSubscriptions int64
	ActiveDeliveries    int64
	CreditsInUse        int64
	CreditsAvailable    int64
}

// Close closes the dispatcher and cleans up all resources
func (d *Dispatcher) Close() {
	close(d.quit)
	for _, shard := range d.shards {
		shard.mu.Lock()
		for deliveryID, active := range shard.activeDeliveries {
			if active.Timer != nil {
				active.Timer.Stop()
			}
			if active.quit != nil {
				select {
				case <-active.quit:
				default:
					close(active.quit)
				}
			}
			delete(shard.activeDeliveries, deliveryID)
		}
		shard.subscriptions = make(map[string]*Subscription)
		shard.mu.Unlock()
	}
}

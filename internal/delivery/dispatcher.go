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
type Dispatcher struct {
	mu             sync.RWMutex
	subscriptions  map[string]*Subscription
	activeDeliveries map[string]*ActiveDelivery
	config         *Config
	quit           chan struct{}
}

// Subscription represents a subscriber
type Subscription struct {
	ID             string
	ConsumerGroup  string
	Partition      *types.Partition
	NextOffset     int64
	Credits        int32
	MaxCredits     int32
	Stream         Stream
	CreatedTS      int64
}

// Stream represents gRPC stream
type Stream interface {
	Send(delivery *DeliveryMessage) error
	Recv() (*Control, error)
	Context() context.Context
}

// DeliveryMessage represents a delivery to subscriber
type DeliveryMessage struct {
	Event       *types.Event
	DeliveryID  string
	Attempt     int32
	AckTimeout  int32
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
	Delivery      *DeliveryMessage
	Subscription  *Subscription
	Attempt       int32
	CreatedTS     int64
	AckDeadline   time.Time
	Timer         *time.Timer
	quit          chan struct{} // Quit channel for timeout goroutine
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

// NewDispatcher creates a new dispatcher
func NewDispatcher(config *Config) *Dispatcher {
	return &Dispatcher{
		subscriptions:      make(map[string]*Subscription),
		activeDeliveries:  make(map[string]*ActiveDelivery),
		config:            config,
		quit:              make(chan struct{}),
	}
}

// Subscribe adds a subscription
func (d *Dispatcher) Subscribe(sub *Subscription) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	log.Printf("[DISPATCHER] Subscribe: Registering subscription %s (partition=%d, maxCredits=%d)",
		sub.ID, sub.Partition.ID, sub.MaxCredits)

	// Check if subscription already exists
	if _, exists := d.subscriptions[sub.ID]; exists {
		return fmt.Errorf("subscription %s already exists", sub.ID)
	}

	// Set initial credits
	if sub.MaxCredits == 0 {
		sub.MaxCredits = d.config.MaxDeliveryCredits
	}
	sub.Credits = sub.MaxCredits

	d.subscriptions[sub.ID] = sub
	log.Printf("[DISPATCHER] Subscribe: Successfully registered subscription %s (total subscriptions=%d)",
		sub.ID, len(d.subscriptions))
	return nil
}

// Unsubscribe removes a subscription
func (d *Dispatcher) Unsubscribe(subscriptionID string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	_, exists := d.subscriptions[subscriptionID]
	if !exists {
		return fmt.Errorf("subscription %s not found", subscriptionID)
	}

	// Cancel active deliveries
	for deliveryID, active := range d.activeDeliveries {
		if active.Subscription.ID == subscriptionID {
			if active.Timer != nil {
				active.Timer.Stop()
			}
			delete(d.activeDeliveries, deliveryID)
		}
	}

	delete(d.subscriptions, subscriptionID)
	return nil
}

// Dispatch dispatches an event to subscribers
func (d *Dispatcher) Dispatch(event *types.Event) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	log.Printf("[DISPATCHER] Dispatching event %s (partition=%d, offset=%d)",
		event.GetMessageId(), event.GetPartitionId(), event.Offset)

	// Find all subscriptions for this event's topic/partition
	subs := d.findSubscriptions(event)

	log.Printf("[DISPATCHER] Found %d subscriptions for event %s", len(subs), event.GetMessageId())

	// Dispatch to each subscription with credits
	for _, sub := range subs {
		log.Printf("[DISPATCHER] Processing subscription %s (credits=%d)", sub.ID, sub.Credits)
		if sub.Credits <= 0 {
			// No credits, skip
			log.Printf("[DISPATCHER] Skipping subscription %s: no credits", sub.ID)
			continue
		}

		// Create delivery
		delivery := &DeliveryMessage{
			Event:       event,
			DeliveryID:  fmt.Sprintf("%s-%d", sub.ID, event.Offset),
			Attempt:     1,
			AckTimeout:  int32(d.config.DefaultAckTimeout / time.Millisecond),
		}

		log.Printf("[DISPATCHER] Sending event %s to subscriber %s", event.GetMessageId(), sub.ID)

		// Send to subscriber
		if err := sub.Stream.Send(delivery); err != nil {
			log.Printf("[DISPATCHER] Failed to send to subscriber %s: %v", sub.ID, err)
			// TODO: Clean up dead subscription
			continue
		}

		log.Printf("[DISPATCHER] Successfully sent event %s to subscriber %s", event.GetMessageId(), sub.ID)

		// Track active delivery
		d.trackDelivery(delivery, sub)

		// Decrement credits
		sub.Credits--
	}

	return nil
}

// findSubscriptions finds subscriptions for an event
func (d *Dispatcher) findSubscriptions(event *types.Event) []*Subscription {
	var subs []*Subscription

	log.Printf("[DISPATCHER] findSubscriptions: eventPartition=%d, totalSubs=%d",
		event.GetPartitionId(), len(d.subscriptions))

	for subID, sub := range d.subscriptions {
		// Check if subscription matches event's partition/topic
		// This is simplified - real implementation would check consumer group assignments
		log.Printf("[DISPATCHER] Checking subscription %s: subPartition=%d, eventPartition=%d",
			subID, sub.Partition.ID, event.GetPartitionId())
		if sub.Partition.ID == event.GetPartitionId() {
			log.Printf("[DISPATCHER] Subscription %s MATCHES!", subID)
			subs = append(subs, sub)
		} else {
			log.Printf("[DISPATCHER] Subscription %s NO MATCH", subID)
		}
	}

	return subs
}

// trackDelivery tracks an active delivery
func (d *Dispatcher) trackDelivery(delivery *DeliveryMessage, sub *Subscription) {
	active := &ActiveDelivery{
		Delivery:     delivery,
		Subscription: sub,
		Attempt:      delivery.Attempt,
		CreatedTS:    time.Now().UnixMilli(),
		AckDeadline:  time.Now().Add(d.config.DefaultAckTimeout),
		Timer:        time.NewTimer(d.config.DefaultAckTimeout),
		quit:         make(chan struct{}),
	}

	d.activeDeliveries[delivery.DeliveryID] = active

	// Wait for ack or timeout
	go func() {
		select {
		case <-active.Timer.C:
			d.handleDeliveryTimeout(delivery.DeliveryID)
		case <-active.quit:
			return // Clean exit on delivery completion
		}
	}()
}

// HandleAck handles acknowledgment from subscriber
func (d *Dispatcher) HandleAck(deliveryID string, success bool, nextOffset int64) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	active, exists := d.activeDeliveries[deliveryID]
	if !exists {
		// Delivery not found, may have been acked already
		return nil
	}

	// Stop timer
	if active.Timer != nil {
		active.Timer.Stop()
	}

	// Close quit channel to stop timeout goroutine
	close(active.quit)

	// Update subscription offset if successful
	if success {
		active.Subscription.NextOffset = nextOffset
	}

	// Return credits to subscriber
	active.Subscription.Credits++

	// Remove from active deliveries
	delete(d.activeDeliveries, deliveryID)

	// Retry if failed and under max retries
	if !success && active.Attempt < d.config.MaxRetries {
		return d.retryDelivery(active)
	}

	return nil
}

// handleDeliveryTimeout handles delivery timeout
func (d *Dispatcher) handleDeliveryTimeout(deliveryID string) {
	d.mu.Lock()
	defer d.mu.Unlock()

	active, exists := d.activeDeliveries[deliveryID]
	if !exists {
		return
	}

	// Retry if under max retries
	if active.Attempt < d.config.MaxRetries {
		d.retryDelivery(active)
	}
}

// retryDelivery retries a failed delivery
func (d *Dispatcher) retryDelivery(active *ActiveDelivery) error {
	// Create retry delivery
	retryDelivery := &DeliveryMessage{
		Event:       active.Delivery.Event,
		DeliveryID:  active.Delivery.DeliveryID,
		Attempt:     active.Attempt + 1,
		AckTimeout:  active.Delivery.AckTimeout,
	}

	// Calculate backoff
	backoff := time.Duration(active.Attempt) * d.config.RetryBackoff
	time.Sleep(backoff)

	// Resend to subscriber
	if err := active.Subscription.Stream.Send(retryDelivery); err != nil {
		// Log error, give up
		return fmt.Errorf("resend failed: %w", err)
	}

	// Update active delivery
	active.Attempt++
	active.AckDeadline = time.Now().Add(d.config.DefaultAckTimeout)
	active.Timer.Reset(d.config.DefaultAckTimeout)

	return nil
}

// GetStats returns dispatcher statistics
func (d *Dispatcher) GetStats() *DispatcherStats {
	d.mu.RLock()
	defer d.mu.RUnlock()

	return &DispatcherStats{
		ActiveSubscriptions: int64(len(d.subscriptions)),
		ActiveDeliveries:   int64(len(d.activeDeliveries)),
		CreditsInUse:       d.calculateCreditsInUse(),
		CreditsAvailable:   d.calculateCreditsAvailable(),
	}
}

// calculateCreditsInUse calculates total credits in use
func (d *Dispatcher) calculateCreditsInUse() int64 {
	var total int64
	for _, sub := range d.subscriptions {
		total += int64(sub.MaxCredits - sub.Credits)
	}
	return total
}

// calculateCreditsAvailable calculates total available credits
func (d *Dispatcher) calculateCreditsAvailable() int64 {
	var total int64
	for _, sub := range d.subscriptions {
		total += int64(sub.Credits)
	}
	return total
}

// DispatcherStats represents dispatcher statistics
type DispatcherStats struct {
	ActiveSubscriptions int64
	ActiveDeliveries   int64
	CreditsInUse       int64
	CreditsAvailable   int64
}

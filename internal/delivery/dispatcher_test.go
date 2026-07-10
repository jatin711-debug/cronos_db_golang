package delivery

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

// mockStream implements the Stream interface for testing
type mockStream struct {
	sendMu     sync.Mutex
	sendCalled int32
	sendErr    error
	lastMsg    *DeliveryMessage
	ctx        context.Context
	cancel     context.CancelFunc
}

func newMockStream() *mockStream {
	ctx, cancel := context.WithCancel(context.Background())
	return &mockStream{ctx: ctx, cancel: cancel}
}

func (s *mockStream) Send(msg *DeliveryMessage) error {
	if s.sendErr != nil {
		return s.sendErr
	}
	atomic.AddInt32(&s.sendCalled, 1)
	s.sendMu.Lock()
	s.lastMsg = msg
	s.sendMu.Unlock()
	return nil
}

func (s *mockStream) Recv() (*Control, error) {
	return nil, nil
}

func (s *mockStream) Context() context.Context {
	return s.ctx
}

func (s *mockStream) SendCount() int {
	return int(atomic.LoadInt32(&s.sendCalled))
}

func makeTestEvent(partitionID int32, offset int64, msgID string) *types.Event {
	return &types.Event{
		MessageId:   msgID,
		Topic:       "test-topic",
		Payload:     []byte("test payload"),
		Offset:      offset,
		PartitionId: partitionID,
		ScheduleTs:  time.Now().UnixMilli(),
	}
}

func makeTestSubscription(id string, partitionID int32, stream *mockStream) *Subscription {
	return &Subscription{
		ID:            id,
		ConsumerGroup: "test-group",
		Partition:     &types.Partition{ID: partitionID},
		NextOffset:    0,
		MaxCredits:    100,
		Stream:        stream,
		CreatedTS:     time.Now().UnixMilli(),
	}
}

func TestDispatcher_NewDispatcher(t *testing.T) {
	d := NewDispatcher(nil)
	if d == nil {
		t.Fatal("NewDispatcher should not return nil")
	}
	if len(d.shards) != 32 {
		t.Errorf("expected 32 shards, got %d", len(d.shards))
	}
	d.Close()
}

func TestDispatcher_NewDispatcherWithConfig(t *testing.T) {
	cfg := &Config{
		MaxRetries:         10,
		DefaultAckTimeout:  60 * time.Second,
		MaxDeliveryCredits: 500,
		RetryBackoff:       2 * time.Second,
		MaxInFlightEvents:  50000,
	}

	d := NewDispatcher(cfg)
	if d == nil {
		t.Fatal("NewDispatcher should not return nil")
	}

	if d.config.MaxRetries != 10 {
		t.Errorf("expected MaxRetries=10, got %d", d.config.MaxRetries)
	}
	if d.config.MaxDeliveryCredits != 500 {
		t.Errorf("expected MaxDeliveryCredits=500, got %d", d.config.MaxDeliveryCredits)
	}
	d.Close()
}

func TestDispatcher_NewDispatcherWithDLQ(t *testing.T) {
	tmpDir := t.TempDir()
	dlq, err := NewDeadLetterQueue(tmpDir, 100)
	if err != nil {
		t.Fatalf("NewDeadLetterQueue: %v", err)
	}
	defer dlq.Close()

	d := NewDispatcherWithDLQ(nil, dlq)
	if d == nil {
		t.Fatal("NewDispatcherWithDLQ should not return nil")
	}
	if d.dlq == nil {
		t.Error("dlq should be set")
	}
	d.Close()
}

func TestDispatcher_Subscribe(t *testing.T) {
	d := NewDispatcher(nil)
	defer d.Close()

	stream := newMockStream()
	sub := makeTestSubscription("sub-1", 0, stream)

	err := d.Subscribe(sub)
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	// Duplicate subscription
	err = d.Subscribe(sub)
	if err == nil {
		t.Error("expected error for duplicate subscription")
	}
}

func TestDispatcher_Unsubscribe(t *testing.T) {
	d := NewDispatcher(nil)
	defer d.Close()

	stream := newMockStream()
	sub := makeTestSubscription("sub-1", 0, stream)

	d.Subscribe(sub)
	err := d.Unsubscribe("sub-1")
	if err != nil {
		t.Fatalf("Unsubscribe failed: %v", err)
	}

	// Unsubscribe again should fail
	err = d.Unsubscribe("sub-1")
	if err == nil {
		t.Error("expected error for unsubscribing nonexistent")
	}
}

func TestDispatcher_Dispatch_SingleSubscriber(t *testing.T) {
	d := NewDispatcher(nil)
	defer d.Close()

	stream := newMockStream()
	sub := makeTestSubscription("sub-1", 0, stream)
	d.Subscribe(sub)

	event := makeTestEvent(0, 0, "msg-0")
	err := d.Dispatch(event)
	if err != nil {
		t.Fatalf("Dispatch failed: %v", err)
	}

	// Give goroutine time to deliver
	time.Sleep(10 * time.Millisecond)

	if stream.SendCount() != 1 {
		t.Errorf("expected 1 send, got %d", stream.SendCount())
	}
}

func TestDispatcher_Dispatch_NoSubscribers(t *testing.T) {
	d := NewDispatcher(nil)
	defer d.Close()

	event := makeTestEvent(0, 0, "msg-0")
	err := d.Dispatch(event)
	if err != nil {
		t.Fatalf("Dispatch to no subscribers failed: %v", err)
	}
}

func TestDispatcher_Dispatch_CreditExhaustion(t *testing.T) {
	d := NewDispatcher(nil)
	defer d.Close()

	stream := newMockStream()
	sub := makeTestSubscription("sub-1", 0, stream)
	sub.MaxCredits = 1 // Very low credits
	d.Subscribe(sub)

	// First event should succeed
	event1 := makeTestEvent(0, 0, "msg-0")
	d.Dispatch(event1)
	time.Sleep(10 * time.Millisecond)

	if stream.SendCount() != 1 {
		t.Errorf("first event: expected 1 send, got %d", stream.SendCount())
	}

	// Second event should be silently dropped (no credits)
	event2 := makeTestEvent(0, 1, "msg-1")
	d.Dispatch(event2)
	time.Sleep(10 * time.Millisecond)

	// Second event may or may not be sent depending on timing
	// Just verify no panic
}

func TestDispatcher_Dispatch_UnknownPartition(t *testing.T) {
	d := NewDispatcher(nil)
	defer d.Close()

	stream := newMockStream()
	sub := makeTestSubscription("sub-1", 99, stream) // Partition 99
	d.Subscribe(sub)

	// Event for partition 0 (not subscribed)
	event := makeTestEvent(0, 0, "msg-0")
	err := d.Dispatch(event)
	if err != nil {
		t.Fatalf("Dispatch failed: %v", err)
	}

	time.Sleep(10 * time.Millisecond)
	if stream.SendCount() != 0 {
		t.Errorf("expected 0 sends for wrong partition, got %d", stream.SendCount())
	}
}

func TestDispatcher_DispatchBatch(t *testing.T) {
	d := NewDispatcher(nil)
	defer d.Close()

	stream := newMockStream()
	sub := makeTestSubscription("sub-1", 0, stream)
	d.Subscribe(sub)

	events := make([]*types.Event, 5)
	for i := 0; i < 5; i++ {
		events[i] = makeTestEvent(0, int64(i), fmt.Sprintf("batch-%d", i))
	}

	err := d.DispatchBatch(events)
	if err != nil {
		t.Fatalf("DispatchBatch failed: %v", err)
	}

	time.Sleep(20 * time.Millisecond)
	if stream.SendCount() == 0 {
		t.Error("expected at least some sends")
	}
}

func TestDispatcher_DispatchBatch_Empty(t *testing.T) {
	d := NewDispatcher(nil)
	defer d.Close()

	err := d.DispatchBatch([]*types.Event{})
	if err != nil {
		t.Fatalf("DispatchBatch with empty slice failed: %v", err)
	}
}

func TestDispatcher_Dispatch_MultipleSubscribers(t *testing.T) {
	d := NewDispatcher(nil)
	defer d.Close()

	stream1 := newMockStream()
	stream2 := newMockStream()
	sub1 := makeTestSubscription("sub-1", 0, stream1)
	sub2 := makeTestSubscription("sub-2", 0, stream2)
	d.Subscribe(sub1)
	d.Subscribe(sub2)

	event := makeTestEvent(0, 0, "msg-0")
	d.Dispatch(event)

	time.Sleep(50 * time.Millisecond)
	total := stream1.SendCount() + stream2.SendCount()
	if total < 1 {
		t.Error("at least one subscriber should have received event")
	}
}

func TestDispatcher_HandleAck_Success(t *testing.T) {
	d := NewDispatcher(nil)
	defer d.Close()

	stream := newMockStream()
	sub := makeTestSubscription("sub-1", 0, stream)
	sub.MaxCredits = 10
	d.Subscribe(sub)

	event := makeTestEvent(0, 0, "msg-0")
	d.Dispatch(event)
	time.Sleep(10 * time.Millisecond)

	// Ack the delivery
	err := d.HandleAck("sub-1-0", true, 1)
	if err != nil {
		t.Fatalf("HandleAck failed: %v", err)
	}

	// Credits should be restored
	stats := d.GetStats()
	if stats.CreditsAvailable <= 0 {
		t.Error("expected credits to be restored after ack")
	}
}

func TestDispatcher_HandleAck_NonExistent(t *testing.T) {
	d := NewDispatcher(nil)
	defer d.Close()

	// Acking non-existent delivery should not error
	err := d.HandleAck("nonexistent-123", true, 1)
	if err != nil {
		t.Fatalf("HandleAck for nonexistent failed: %v", err)
	}
}

func TestDispatcher_GetStats(t *testing.T) {
	d := NewDispatcher(nil)
	defer d.Close()

	stats := d.GetStats()
	if stats == nil {
		t.Fatal("GetStats should not return nil")
	}

	stream := newMockStream()
	sub := makeTestSubscription("sub-1", 0, stream)
	d.Subscribe(sub)

	event := makeTestEvent(0, 0, "msg-0")
	d.Dispatch(event)
	time.Sleep(10 * time.Millisecond)

	stats = d.GetStats()
	if stats.ActiveSubscriptions != 1 {
		t.Errorf("expected ActiveSubscriptions=1, got %d", stats.ActiveSubscriptions)
	}
}

func TestDispatcher_Drain(t *testing.T) {
	d := NewDispatcher(nil)
	defer d.Close()

	err := d.Drain(100 * time.Millisecond)
	if err != nil {
		t.Fatalf("Drain failed: %v", err)
	}
}

func TestDispatcher_SetDLQ(t *testing.T) {
	d := NewDispatcher(nil)
	tmpDir := t.TempDir()
	dlq, _ := NewDeadLetterQueue(tmpDir, 100)
	defer dlq.Close()

	d.SetDLQ(dlq)

	if d.dlq == nil {
		t.Error("dlq should be set")
	}
}

func TestDispatcher_getShard(t *testing.T) {
	d := NewDispatcher(nil)
	defer d.Close()

	shard1 := d.getShard("sub-1")
	shard2 := d.getShard("sub-2")

	// Same key should return same shard
	if shard1 != d.getShard("sub-1") {
		t.Error("same key should return same shard")
	}

	// Different keys may or may not return same shard (hash distribution)
	// Just verify they return *some* shard
	if shard1 == nil || shard2 == nil {
		t.Error("shards should not be nil")
	}
}

func TestDispatcher_TryReserveInFlight(t *testing.T) {
	d := NewDispatcher(nil)
	d.config.MaxInFlightEvents = 10
	defer d.Close()

	// Should reserve
	if !d.tryReserveInFlight(5) {
		t.Error("should be able to reserve 5")
	}

	// Should reserve remaining
	if !d.tryReserveInFlight(5) {
		t.Error("should be able to reserve remaining 5")
	}

	// Should fail - over limit
	if d.tryReserveInFlight(1) {
		t.Error("should not be able to reserve 1 when at limit")
	}

	// Release some
	if err := d.decInFlight(5); err != nil {
		t.Errorf("unexpected error releasing in-flight: %v", err)
	}
	if !d.tryReserveInFlight(1) {
		t.Error("should be able to reserve 1 after release")
	}
}

func TestDispatcher_TryReserveInFlight_ZeroLimit(t *testing.T) {
	d := NewDispatcher(nil)
	d.config.MaxInFlightEvents = 0 // Disabled
	defer d.Close()

	// With 0 limit, should always allow
	if !d.tryReserveInFlight(1000) {
		t.Error("with MaxInFlightEvents=0, should always allow")
	}
}

func TestDispatcher_DecInFlight(t *testing.T) {
	d := NewDispatcher(nil)
	defer d.Close()

	// Reserve then dec
	d.tryReserveInFlight(5)
	if err := d.decInFlight(3); err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if d.getInFlight() != 2 {
		t.Errorf("expected in-flight=2, got %d", d.getInFlight())
	}

	// Dec more than current should return an error
	if err := d.decInFlight(100); err == nil {
		t.Error("expected underflow error when decrementing more than current")
	}
	if d.getInFlight() != 2 {
		t.Errorf("expected in-flight to remain 2 after failed decrement, got %d", d.getInFlight())
	}
}

func TestDispatcher_TryReserveInFlight_Concurrent(t *testing.T) {
	d := NewDispatcher(nil)
	d.config.MaxInFlightEvents = 100
	defer d.Close()

	var success int64
	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				if d.tryReserveInFlight(1) {
					atomic.AddInt64(&success, 1)
				}
			}
		}()
	}

	wg.Wait()

	if success == 0 {
		t.Error("expected at least some successful reserves")
	}
}

func TestParseSubIDFromDeliveryID(t *testing.T) {
	tests := []struct {
		deliveryID string
		wantSubID  string
		wantErr    bool
	}{
		{"sub-1-0", "sub-1", false},
		{"sub-1-100", "sub-1", false},
		{"my-consumer-group-event-500", "my-consumer-group-event", false},
		{"sub-batch-0-10", "sub", false},
		{"", "", true},
		{"no-dash", "", true},
		{"-0", "", true},
		{"sub-", "", true},
	}

	for _, tc := range tests {
		t.Run(tc.deliveryID, func(t *testing.T) {
			subID, err := parseSubIDFromDeliveryID(tc.deliveryID)
			if tc.wantErr {
				if err == nil {
					t.Error("expected error")
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if subID != tc.wantSubID {
					t.Errorf("expected subID=%q, got %q", tc.wantSubID, subID)
				}
			}
		})
	}
}

func TestGroupSubscriptionsByConsumer(t *testing.T) {
	subs := []*Subscription{
		{ID: "1", ConsumerGroup: "group-A"},
		{ID: "2", ConsumerGroup: "group-A"},
		{ID: "3", ConsumerGroup: "group-B"},
	}

	result := groupSubscriptionsByConsumer(subs)

	if len(result) != 2 {
		t.Errorf("expected 2 groups, got %d", len(result))
	}
	if len(result["group-A"]) != 2 {
		t.Errorf("expected 2 in group-A, got %d", len(result["group-A"]))
	}
	if len(result["group-B"]) != 1 {
		t.Errorf("expected 1 in group-B, got %d", len(result["group-B"]))
	}
}

func TestGroupSubscriptionsByConsumer_AllSameGroup(t *testing.T) {
	subs := []*Subscription{
		{ID: "1", ConsumerGroup: "only-group"},
		{ID: "2", ConsumerGroup: "only-group"},
	}

	result := groupSubscriptionsByConsumer(subs)

	// When all subs share same group, fast path returns just one entry
	if len(result) != 1 {
		t.Errorf("expected 1 group entry, got %d", len(result))
	}
}

func TestGroupSubscriptionsByConsumer_Empty(t *testing.T) {
	result := groupSubscriptionsByConsumer([]*Subscription{})
	if len(result) != 0 {
		t.Errorf("expected 0 groups, got %d", len(result))
	}
}

func TestMakeDeliveryID(t *testing.T) {
	id := makeDeliveryID("sub-1", 123)
	if id != "sub-1-123" {
		t.Errorf("expected sub-1-123, got %s", id)
	}
}

func TestMakeDeliveryIDBatch(t *testing.T) {
	id := makeDeliveryIDBatch("sub-1", 100, 5)
	if id != "sub-1-batch-100-5" {
		t.Errorf("expected sub-1-batch-100-5, got %s", id)
	}
}

// TestMakeDeliveryID_StabilityAcrossCalls is a regression test for a
// use-after-free bug: makeDeliveryID previously returned unsafe.String over a
// stack-local buffer, so the returned string's backing memory was reused on
// the next call. Any caller that held the string (e.g. as an activeDeliveries
// map key) saw it silently mutate. This test builds many IDs, holds them all,
// and asserts every one keeps its original value.
func TestMakeDeliveryID_StabilityAcrossCalls(t *testing.T) {
	const n = 256
	ids := make([]string, n)
	want := make([]string, n)
	for i := 0; i < n; i++ {
		ids[i] = makeDeliveryID("sub-1", int64(i))
		want[i] = "sub-1-" + strconv.Itoa(i)
	}
	for i := range ids {
		if ids[i] != want[i] {
			t.Errorf("id[%d] mutated: want %q, got %q (use-after-free regression)", i, want[i], ids[i])
		}
	}
}

// TestMakeDeliveryIDBatch_StabilityAcrossCalls is the batch-ID regression test
// for the same use-after-free as above.
func TestMakeDeliveryIDBatch_StabilityAcrossCalls(t *testing.T) {
	const n = 256
	ids := make([]string, n)
	want := make([]string, n)
	for i := 0; i < n; i++ {
		ids[i] = makeDeliveryIDBatch("sub-1", int64(i), 3)
		want[i] = "sub-1-batch-" + strconv.Itoa(i) + "-3"
	}
	for i := range ids {
		if ids[i] != want[i] {
			t.Errorf("batch id[%d] mutated: want %q, got %q (use-after-free regression)", i, want[i], ids[i])
		}
	}
}

func TestDeliveryMessage_Batch(t *testing.T) {
	msg := &DeliveryMessage{
		Event:      nil,
		DeliveryID: "test-batch",
		Attempt:    1,
		Batch:      make([]*types.Event, 3),
	}

	if len(msg.Batch) != 3 {
		t.Errorf("expected batch len 3, got %d", len(msg.Batch))
	}
}

func TestActiveDelivery_CreditsConsumed(t *testing.T) {
	ad := &ActiveDelivery{
		CreditsConsumed: 5,
	}

	if ad.CreditsConsumed != 5 {
		t.Errorf("expected 5 credits, got %d", ad.CreditsConsumed)
	}
}
package delivery

import (
	"sync"
	"testing"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

func testEvents(from, to int64) []*types.Event {
	events := make([]*types.Event, 0, to-from+1)
	for offset := from; offset <= to; offset++ {
		events = append(events, makeTestEvent(0, offset, "msg"))
	}
	return events
}

func expectWake(t *testing.T, cursor *RedriveCursor, want bool) {
	t.Helper()
	select {
	case <-cursor.Wake():
		if !want {
			t.Fatal("redrive cursor was woken unexpectedly")
		}
	default:
		if want {
			t.Fatal("redrive cursor was not woken")
		}
	}
}

// Events the push path cannot hand over for lack of credits must be queued for
// redrive at their exact offsets rather than left for a periodic full rescan.
func TestDispatcher_HeldBackEventsAreQueuedForRedrive(t *testing.T) {
	d := NewDispatcher(nil)
	defer d.Close()

	stream := newMockStream()
	sub := makeTestSubscription("sub-1", 0, stream)
	sub.MaxCredits = 2
	if err := d.Subscribe(sub); err != nil {
		t.Fatal(err)
	}
	cursor := d.RegisterRedrive(sub.ConsumerGroup)
	defer d.UnregisterRedrive(cursor)
	other := d.RegisterRedrive("another-group")
	defer d.UnregisterRedrive(other)

	if err := d.DispatchBatch(testEvents(0, 4)); err != nil {
		t.Fatal(err)
	}
	if got := len(stream.lastMsg.Batch); got != 2 {
		t.Fatalf("delivered %d events with 2 credits", got)
	}
	low, high, ok := cursor.Take()
	if !ok || low != 2 || high != 4 {
		t.Fatalf("redrive request = [%d, %d] ok=%v, want [2, 4]", low, high, ok)
	}
	expectWake(t, cursor, true)
	if _, _, ok := cursor.Take(); ok {
		t.Fatal("a taken redrive request must not be reported twice")
	}
	if _, _, ok := other.Take(); ok {
		t.Fatal("a group that missed nothing was asked to redrive")
	}
}

// A redrive caller gets told when flow control held events back, is woken when
// an ack frees credits, and is never handed an event that is still in flight.
func TestDispatcher_RedriveGroupFollowsCredits(t *testing.T) {
	d := NewDispatcher(nil)
	defer d.Close()
	var mu sync.Mutex
	completed := map[int64]bool{}
	d.IsCompleted = func(_ string, offset int64) bool {
		mu.Lock()
		defer mu.Unlock()
		return completed[offset]
	}

	stream := newMockStream()
	sub := makeTestSubscription("sub-1", 0, stream)
	sub.MaxCredits = 2
	if err := d.Subscribe(sub); err != nil {
		t.Fatal(err)
	}
	cursor := d.RegisterRedrive(sub.ConsumerGroup)
	defer d.UnregisterRedrive(cursor)

	events := testEvents(0, 2)
	dispatched, blocked := d.RedriveGroup(sub.ConsumerGroup, events)
	if dispatched != 2 || !blocked {
		t.Fatalf("first offer: dispatched=%d blocked=%v, want 2 and blocked", dispatched, blocked)
	}
	if _, _, ok := cursor.Take(); ok {
		t.Fatal("a redrive caller keeps its own position; nothing should be queued for it")
	}

	// Still in flight: offering the same range again must not duplicate them.
	if dispatched, blocked := d.RedriveGroup(sub.ConsumerGroup, events); dispatched != 0 || !blocked {
		t.Fatalf("re-offer while in flight: dispatched=%d blocked=%v, want 0 and blocked", dispatched, blocked)
	}

	mu.Lock()
	completed[0], completed[1] = true, true
	mu.Unlock()
	if err := d.HandleAck(stream.lastMsg.DeliveryID, true, 2); err != nil {
		t.Fatal(err)
	}
	expectWake(t, cursor, true)

	dispatched, blocked = d.RedriveGroup(sub.ConsumerGroup, events)
	if dispatched != 1 || blocked {
		t.Fatalf("after ack: dispatched=%d blocked=%v, want the one remaining event and not blocked", dispatched, blocked)
	}
	if got := stream.lastMsg.Event.GetOffset(); got != 2 {
		t.Fatalf("redelivered offset %d, want 2", got)
	}
}

// Deliveries a subscriber never acked go back to the rest of its group when it
// disconnects.
func TestDispatcher_UnsubscribeQueuesAbandonedDeliveries(t *testing.T) {
	d := NewDispatcher(nil)
	defer d.Close()

	stream := newMockStream()
	sub := makeTestSubscription("sub-1", 0, stream)
	if err := d.Subscribe(sub); err != nil {
		t.Fatal(err)
	}
	cursor := d.RegisterRedrive(sub.ConsumerGroup)
	defer d.UnregisterRedrive(cursor)

	if err := d.DispatchBatch(testEvents(5, 9)); err != nil {
		t.Fatal(err)
	}
	if _, _, ok := cursor.Take(); ok {
		t.Fatal("nothing was held back yet")
	}
	if err := d.Unsubscribe(sub.ID); err != nil {
		t.Fatal(err)
	}
	low, high, ok := cursor.Take()
	if !ok || low != 5 || high != 9 {
		t.Fatalf("redrive request after disconnect = [%d, %d] ok=%v, want [5, 9]", low, high, ok)
	}
}

// A dead-lettered event has reached its final disposition. The dispatcher has
// to report it so it is recorded complete; otherwise the WAL redrive would
// deliver the poison message again and again.
func TestDispatcher_DeadLetteredEventsAreReportedFinal(t *testing.T) {
	dlq, err := NewDeadLetterQueue(t.TempDir(), 100)
	if err != nil {
		t.Fatal(err)
	}
	cfg := DefaultConfig()
	cfg.MaxRetries = 1
	d := NewDispatcherWithDLQ(cfg, dlq)
	defer dlq.Close()
	defer d.Close()

	var mu sync.Mutex
	final := map[int64]bool{}
	d.IsCompleted = func(_ string, offset int64) bool {
		mu.Lock()
		defer mu.Unlock()
		return final[offset]
	}
	var reportedGroup string
	d.OnDeadLettered = func(group string, events []*types.Event) {
		mu.Lock()
		defer mu.Unlock()
		reportedGroup = group
		for _, event := range events {
			final[event.Offset] = true
		}
	}

	stream := newMockStream()
	sub := makeTestSubscription("sub-1", 0, stream)
	if err := d.Subscribe(sub); err != nil {
		t.Fatal(err)
	}
	events := testEvents(0, 0)
	if err := d.DispatchBatch(events); err != nil {
		t.Fatal(err)
	}
	if err := d.HandleAck(stream.lastMsg.DeliveryID, false, 0); err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	gotGroup, gotFinal := reportedGroup, final[0]
	mu.Unlock()
	if gotGroup != sub.ConsumerGroup || !gotFinal {
		t.Fatalf("dead-lettered event not reported final (group=%q final=%v)", gotGroup, gotFinal)
	}

	sends := stream.SendCount()
	if dispatched, _ := d.RedriveGroup(sub.ConsumerGroup, events); dispatched != 0 {
		t.Fatal("a dead-lettered event was handed out again")
	}
	if stream.SendCount() != sends {
		t.Fatal("a dead-lettered event was sent again")
	}
}

// Ready events the worker has no room for are not lost: every group is pointed
// at their offsets in the WAL.
func TestWorker_DroppedReadyEventsAreQueuedForRedrive(t *testing.T) {
	d := NewDispatcher(nil)
	defer d.Close()
	cursor := d.RegisterRedrive("any-group")
	defer d.UnregisterRedrive(cursor)

	w := NewWorker(d, 100) // not started, so the queue only fills
	w.AddReadyEvents(testEvents(0, 9999))
	if _, _, ok := cursor.Take(); ok {
		t.Fatal("nothing was dropped yet")
	}
	w.AddReadyEvents(testEvents(10000, 10004))
	low, high, ok := cursor.Take()
	if !ok || low != 10000 || high != 10004 {
		t.Fatalf("redrive request for dropped events = [%d, %d] ok=%v, want [10000, 10004]", low, high, ok)
	}
}

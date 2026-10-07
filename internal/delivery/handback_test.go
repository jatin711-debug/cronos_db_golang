package delivery

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

// breakingStream takes deliveries until it is broken, and fails every send
// after that, as the stream of a consumer that has gone away does.
type breakingStream struct {
	*mockStream
	broken atomic.Bool
}

func (s *breakingStream) Send(msg *DeliveryMessage) error {
	if s.broken.Load() {
		return errors.New("transport is closing")
	}
	return s.mockStream.Send(msg)
}

// quickRetryConfig makes a delivery time out after 30 ms. retryAfter is how
// long it then waits before it is sent again.
func quickRetryConfig(retryAfter time.Duration) *Config {
	config := DefaultConfig()
	config.MaxRetries = 3
	config.DefaultAckTimeout = 30 * time.Millisecond
	config.RetryBackoff = retryAfter
	return config
}

// deadLetters makes the test fail if the dispatcher gives an event up, and
// returns what it would have recorded as finished.
func deadLetters(t *testing.T, d *Dispatcher) *atomic.Int32 {
	t.Helper()
	dlq, err := NewDeadLetterQueue(t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = dlq.Close() })
	d.SetDLQ(dlq)
	finished := &atomic.Int32{}
	d.OnDeadLettered = func(string, []*types.Event) { finished.Add(1) }
	return finished
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// A delivery that is not acknowledged in time waits to be sent again to the
// consumer it went to. If that consumer leaves while it waits, nothing gave
// the event back: it stayed marked as on its way, the repeats went to a stream
// nobody read, and after the last of them the event was dead-lettered and
// recorded as finished, with the rest of the group ready to take it.
func TestDispatcher_UnacknowledgedDeliveryOfAConsumerThatLeftGoesBackToTheGroup(t *testing.T) {
	d := NewDispatcher(quickRetryConfig(time.Hour))
	defer d.Close()
	finished := deadLetters(t, d)

	gone := newMockStream()
	first := makeTestSubscription("sub-gone", 0, gone)
	if err := d.Subscribe(first); err != nil {
		t.Fatal(err)
	}
	cursor := d.RegisterRedrive(first.ConsumerGroup)
	defer d.UnregisterRedrive(cursor)

	events := testEvents(7, 7)
	if err := d.DispatchBatch(events); err != nil {
		t.Fatal(err)
	}
	if gone.SendCount() != 1 {
		t.Fatalf("the event was sent %d times, want 1", gone.SendCount())
	}
	// Not acknowledged: it leaves the deliveries in flight and waits its turn
	// to be sent again.
	eventually(t, "the delivery to time out", func() bool { return d.GetStats().RetryQueueDepth == 1 })
	if !d.isPending(first.ConsumerGroup, 7) {
		t.Fatal("an event waiting to be sent again is not marked as on its way")
	}
	cursor.Take()

	if err := d.Unsubscribe(first.ID); err != nil {
		t.Fatal(err)
	}
	if d.isPending(first.ConsumerGroup, 7) {
		t.Fatal("the event is still marked as on its way to a consumer that has left")
	}
	if depth := d.GetStats().RetryQueueDepth; depth != 0 {
		t.Fatalf("%d deliveries still wait to be sent again to a consumer that has left", depth)
	}
	if low, high, ok := cursor.Take(); !ok || low != 7 || high != 7 {
		t.Fatalf("redrive request = [%d, %d] ok=%v, want [7, 7]", low, high, ok)
	}

	stream := newMockStream()
	second := makeTestSubscription("sub-stays", 0, stream)
	if err := d.Subscribe(second); err != nil {
		t.Fatal(err)
	}
	if dispatched, _ := d.RedriveGroup(second.ConsumerGroup, events); dispatched != 1 {
		t.Fatalf("%d events were handed to the consumer that stayed, want 1", dispatched)
	}
	if finished.Load() != 0 {
		t.Fatal("the event was dead-lettered")
	}
}

// Sending a delivery again can fail: the consumer's stream broke and its
// subscription has not ended yet. That says nothing against the event. It
// used to count as an attempt all the same, and after the last one the event
// was dead-lettered and recorded as finished.
func TestDispatcher_DeliveryThatCannotBeSentAgainGoesBackToTheGroup(t *testing.T) {
	d := NewDispatcher(quickRetryConfig(time.Millisecond))
	defer d.Close()
	finished := deadLetters(t, d)

	stream := &breakingStream{mockStream: newMockStream()}
	sub := makeTestSubscription("sub-1", 0, stream.mockStream)
	sub.Stream = stream
	if err := d.Subscribe(sub); err != nil {
		t.Fatal(err)
	}
	cursor := d.RegisterRedrive(sub.ConsumerGroup)
	defer d.UnregisterRedrive(cursor)

	if err := d.DispatchBatch(testEvents(7, 7)); err != nil {
		t.Fatal(err)
	}
	cursor.Take()
	stream.broken.Store(true)

	eventually(t, "the event to be given back to the group", func() bool {
		return !d.isPending(sub.ConsumerGroup, 7)
	})
	if finished.Load() != 0 {
		t.Fatal("the event was dead-lettered because it could not be sent")
	}
	if low, high, ok := cursor.Take(); !ok || low != 7 || high != 7 {
		t.Fatalf("redrive request = [%d, %d] ok=%v, want [7, 7]", low, high, ok)
	}
	if credits := atomic.LoadInt32(&sub.Credits); credits != sub.MaxCredits {
		t.Fatalf("the consumer has %d of %d credits after its delivery was given back", credits, sub.MaxCredits)
	}
	// Long enough for every further attempt there would have been.
	time.Sleep(300 * time.Millisecond)
	if finished.Load() != 0 {
		t.Fatal("the event was dead-lettered because it could not be sent")
	}
}

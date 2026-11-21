package delivery

import (
	"sync"
	"sync/atomic"
	"testing"
)

// A consumer that has acknowledged a delivery gets its credits back before
// the completion is recorded, so its next window can be on its way meanwhile.
// The delivery stays on its way until the completion is recorded: its events
// are not handed out again, and its credits come back once, not twice.
func TestDispatcher_AckedCreditsComeBackBeforeTheCompletionIsRecorded(t *testing.T) {
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
	credits := func() int32 { return atomic.LoadInt32(&sub.Credits) }

	events := testEvents(0, 3) // offsets 0 to 3
	if dispatched, blocked := d.RedriveGroup(sub.ConsumerGroup, events); dispatched != 2 || !blocked {
		t.Fatalf("first window: dispatched=%d blocked=%v, want 2 and blocked", dispatched, blocked)
	}
	first := stream.lastMsg.DeliveryID
	if got := credits(); got != 0 {
		t.Fatalf("%d credits with a full window on its way, want 0", got)
	}

	// The consumer acknowledges the first window; recording that takes a
	// disk sync, which has not happened yet.
	d.ReleaseAckedCredits(first)
	if got := credits(); got != 2 {
		t.Fatalf("%d credits after the acknowledgement arrived, want 2", got)
	}
	expectWake(t, cursor, true)

	// The next window goes out, and it is the next two events, not the two
	// whose completion is still being recorded.
	if dispatched, blocked := d.RedriveGroup(sub.ConsumerGroup, events); dispatched != 2 || blocked {
		t.Fatalf("second window: dispatched=%d blocked=%v, want 2 and not blocked", dispatched, blocked)
	}
	second := stream.lastMsg.DeliveryID
	if second == first {
		t.Fatal("no second delivery")
	}
	if got := stream.lastMsg.Batch; len(got) != 2 || got[0].GetOffset() != 2 || got[1].GetOffset() != 3 {
		t.Fatalf("second window carries %v, want offsets 2 and 3", got)
	}

	// An acknowledgement that arrives twice gives nothing back twice.
	d.ReleaseAckedCredits(first)
	if got := credits(); got != 0 {
		t.Fatalf("%d credits after the same acknowledgement again, want 0", got)
	}
	expectWake(t, cursor, false)

	// The first window's completion is recorded. Its credits went back
	// already; the second window still holds the two there are.
	mu.Lock()
	completed[0], completed[1] = true, true
	mu.Unlock()
	if err := d.HandleAck(first, true, 2); err != nil {
		t.Fatal(err)
	}
	if got := credits(); got != 0 {
		t.Fatalf("%d credits once the first window is recorded, want 0: the second is on its way", got)
	}

	// The second window, the way it went before: recorded, then given back.
	mu.Lock()
	completed[2], completed[3] = true, true
	mu.Unlock()
	if err := d.HandleAck(second, true, 4); err != nil {
		t.Fatal(err)
	}
	if got := credits(); got != 2 {
		t.Fatalf("%d credits with nothing on its way, want 2", got)
	}

	// A delivery nobody knows, or one that is over, changes nothing.
	d.ReleaseAckedCredits("sub-1-99")
	d.ReleaseAckedCredits(second)
	d.ReleaseAckedCredits("not a delivery ID")
	if got := credits(); got != 2 {
		t.Fatalf("%d credits, want 2", got)
	}
}

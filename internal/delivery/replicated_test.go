package delivery

import (
	"sync/atomic"
	"testing"
)

// A partition's leader holds entries for a while that are not on enough
// replicas yet, and for good if it has lost its followers: those are removed
// when it takes the log of the leader that replaced it. An entry like that
// must not reach a consumer. What a consumer acknowledges is recorded by
// offset, and the record outlived the entry: the event that the new leader
// wrote at the same offset counted as finished on this node and was never
// delivered once the node led again.
func TestDispatcher_HoldsBackEntriesThatAreNotReplicated(t *testing.T) {
	d := NewDispatcher(nil)
	defer d.Close()
	var replicated atomic.Int64
	replicated.Store(1)
	d.DeliverableThrough = replicated.Load

	stream := newMockStream()
	sub := makeTestSubscription("sub-1", 0, stream)
	if err := d.Subscribe(sub); err != nil {
		t.Fatal(err)
	}
	cursor := d.RegisterRedrive(sub.ConsumerGroup)
	defer d.UnregisterRedrive(cursor)

	if err := d.DispatchBatch(testEvents(0, 3)); err != nil {
		t.Fatal(err)
	}
	if got := len(stream.lastMsg.Batch); got != 2 {
		t.Fatalf("%d events were delivered with offsets 0 and 1 replicated, want 2", got)
	}
	for offset := int64(2); offset <= 3; offset++ {
		if d.isPending(sub.ConsumerGroup, offset) {
			t.Fatalf("offset %d, which is not replicated, is marked as on its way", offset)
		}
	}
	// The scheduler has let go of them; the log is read again for them.
	if low, high, ok := cursor.Take(); !ok || low != 2 || high != 3 {
		t.Fatalf("redrive request = [%d, %d] ok=%v, want [2, 3]", low, high, ok)
	}

	if dispatched, blocked := d.RedriveGroup(sub.ConsumerGroup, testEvents(2, 3)); dispatched != 0 || !blocked {
		t.Fatalf("redrive of entries that are not replicated: dispatched=%d blocked=%v, want 0 and true", dispatched, blocked)
	}
	replicated.Store(3)
	if dispatched, blocked := d.RedriveGroup(sub.ConsumerGroup, testEvents(2, 3)); dispatched != 2 || blocked {
		t.Fatalf("redrive once they are replicated: dispatched=%d blocked=%v, want 2 and false", dispatched, blocked)
	}
}

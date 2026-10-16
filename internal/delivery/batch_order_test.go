package delivery

import (
	"fmt"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

// A batch goes out in offset order, whatever order its events were handed
// over in, so that an acknowledgement for the offset after its last event is
// taken. Out of order, every acknowledgement of the batch was refused, its
// redeliveries' too, and the subscription's credits stayed taken.
func TestDispatcher_BatchIsSentInOffsetOrder(t *testing.T) {
	d := NewDispatcher(nil)
	defer d.Close()
	stream := newMockStream()
	d.Subscribe(makeTestSubscription("sub-1", 0, stream))

	var events []*types.Event
	for _, offset := range []int64{4, 2, 3, 0, 1} {
		events = append(events, makeTestEvent(0, offset, fmt.Sprintf("m-%d", offset)))
	}
	if err := d.DispatchBatch(events); err != nil {
		t.Fatal(err)
	}
	var msg *DeliveryMessage
	for deadline := time.Now().Add(2 * time.Second); msg == nil && time.Now().Before(deadline); time.Sleep(time.Millisecond) {
		stream.sendMu.Lock()
		msg = stream.lastMsg
		stream.sendMu.Unlock()
	}
	if msg == nil || len(msg.Batch) != 5 {
		t.Fatalf("delivery: %+v", msg)
	}
	for i, event := range msg.Batch {
		if event.Offset != int64(i) {
			t.Fatalf("batch sent in order %v", offsets(msg.Batch))
		}
	}

	// A client acknowledges the offset after the batch's last event.
	next := msg.Batch[len(msg.Batch)-1].Offset + 1
	if _, _, _, err := d.ValidateAck(msg.DeliveryID, "", next, true); err != nil {
		t.Fatalf("acknowledgement refused: %v", err)
	}
}

func offsets(events []*types.Event) []int64 {
	out := make([]int64, len(events))
	for i, event := range events {
		out[i] = event.Offset
	}
	return out
}

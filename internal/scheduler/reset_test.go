package scheduler

import (
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

// Reset leaves nothing scheduled, wherever it was kept: in the wheel, in the
// wheels above it, among the events that are ready, or among the references
// to events far ahead. After it the log can be scheduled again from its
// start, and an offset whose entry has changed gets the event that is there
// now.
func TestReset_ForgetsEverythingAndTakesTheLogAgain(t *testing.T) {
	// A one-second wheel (10 ms ticks, 100 slots) and a one-minute hot window.
	s, err := NewScheduler(t.TempDir(), 0, 10, 100, 1, auditReader{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Stop()

	now := time.Now()
	at := func(offset int64, id string, due time.Time) *types.Event {
		return &types.Event{MessageId: id, Offset: offset, ScheduleTs: due.UnixMilli()}
	}
	events := []*types.Event{
		at(0, "due", now.Add(-time.Second)),
		at(1, "soon", now.Add(500*time.Millisecond)), // the wheel
		at(2, "later", now.Add(30*time.Second)),      // a wheel above it
		at(3, "far", now.Add(time.Hour)),             // beyond the hot window
	}
	if err := s.ScheduleBatch(events); err != nil {
		t.Fatal(err)
	}
	if ready, timers, cold := s.GetReadyQueueDepth(), s.GetTimingWheelDepth(), s.GetColdStoreCount(); ready != 1 || timers != 2 || cold != 1 {
		t.Fatalf("scheduled: %d ready, %d timers, %d far ahead; want 1, 2, 1", ready, timers, cold)
	}
	// A second timer for an offset is refused, which is why what is there has
	// to go before the log is scheduled again.
	if err := s.Schedule(at(1, "another event at the same offset", now.Add(700*time.Millisecond))); err == nil {
		t.Fatal("a second timer for the same offset was accepted")
	}

	if err := s.Reset(); err != nil {
		t.Fatal(err)
	}
	if ready, timers, cold := s.GetReadyQueueDepth(), s.GetTimingWheelDepth(), s.GetColdStoreCount(); ready != 0 || timers != 0 || cold != 0 {
		t.Fatalf("after Reset: %d ready, %d timers, %d far ahead; want none", ready, timers, cold)
	}
	if offsets, err := s.coldStore.ScanRange(0, now.Add(2*time.Hour).UnixMilli()); err != nil || len(offsets) != 0 {
		t.Fatalf("after Reset the store of far events still holds offsets %v (%v)", offsets, err)
	}

	// The log again: offset 1 holds another event now.
	replaced := at(1, "the event that is at offset 1 now", now.Add(300*time.Millisecond))
	if err := s.ScheduleBatch([]*types.Event{events[0], replaced, events[2], events[3]}); err != nil {
		t.Fatalf("scheduling the log again: %v", err)
	}
	if ready, timers, cold := s.GetReadyQueueDepth(), s.GetTimingWheelDepth(), s.GetColdStoreCount(); ready != 1 || timers != 2 || cold != 1 {
		t.Fatalf("scheduled again: %d ready, %d timers, %d far ahead; want 1, 2, 1", ready, timers, cold)
	}

	s.Start()
	fired := map[string]bool{}
	deadline := time.Now().Add(5 * time.Second)
	for !fired[replaced.MessageId] {
		if time.Now().After(deadline) {
			t.Fatalf("the event at offset 1 did not fire; fired: %v", fired)
		}
		for _, event := range s.GetReadyEvents() {
			fired[event.MessageId] = true
		}
		time.Sleep(10 * time.Millisecond)
	}
	if fired["soon"] {
		t.Fatal("the event that was at offset 1 before Reset fired")
	}
}

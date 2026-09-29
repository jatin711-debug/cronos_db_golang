package scheduler

import (
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
	"testing"
	"time"
)

type auditReader struct{ event *types.Event }

func TestAuditReadyBatchKeepsOwnershipAfterNextSchedule(t *testing.T) {
	s, err := NewScheduler(t.TempDir(), 0, 10, 100, 0, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Stop()
	first := &types.Event{MessageId: "first", Offset: 1, ScheduleTs: 1}
	second := &types.Event{MessageId: "second", Offset: 2, ScheduleTs: 1}
	if err := s.Schedule(first); err != nil {
		t.Fatal(err)
	}
	batch := s.GetReadyEvents()
	if err := s.Schedule(second); err != nil {
		t.Fatal(err)
	}
	if len(batch) != 1 || batch[0] != first {
		t.Fatal("next schedule overwrote previously drained ready event")
	}
	next := s.GetReadyEvents()
	if len(next) != 1 || next[0] != second {
		t.Fatal("next scheduled event missing")
	}
}

func (r auditReader) ReadEvent(int64) (*types.Event, error) { return r.event, nil }
func TestAuditHydratorRecoversMissedWindow(t *testing.T) {
	ev := &types.Event{MessageId: "cold", Payload: []byte("payload"), Offset: 1, ScheduleTs: time.Now().Add(30 * time.Minute).UnixMilli()}
	s, err := NewScheduler(t.TempDir(), 0, 10, 100, 60, auditReader{ev}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Stop()
	// Simulate a persisted cold event now inside the hot window after a pause/restart.
	if err := s.coldStore.Store(ev.Offset, ev.ScheduleTs); err != nil {
		t.Fatal(err)
	}
	if n := s.hydrate(); n != 1 {
		t.Fatalf("cold event inside hot window stranded: hydrated %d, want 1", n)
	}
}

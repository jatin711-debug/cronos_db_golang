package scheduler

import (
	"sync"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

type budgetReader map[int64]*types.Event

func (r budgetReader) ReadEvent(offset int64) (*types.Event, error) { return r[offset], nil }

func newBudgetScheduler(t *testing.T, b *MemoryBudget, r budgetReader, id int32) *Scheduler {
	t.Helper()
	s, err := NewScheduler(t.TempDir(), id, 10, 60, 60, r, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SetMemoryBudget(b, 2, nil); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Stop)
	return s
}

func TestSchedulerSharedBudgetConcurrentOverflowAndDrain(t *testing.T) {
	b := NewMemoryBudget(32 << 10)
	r := budgetReader{}
	for i := int64(0); i < 400; i++ {
		r[i] = &types.Event{Offset: i, ScheduleTs: time.Now().UnixMilli() - 1, Payload: make([]byte, 4096), Meta: map[string]string{"key": "value"}}
	}
	schedulers := []*Scheduler{newBudgetScheduler(t, b, r, 0), newBudgetScheduler(t, b, r, 1)}
	var wg sync.WaitGroup
	for i := int64(0); i < 400; i++ {
		wg.Add(1)
		go func(i int64) {
			defer wg.Done()
			if err := schedulers[i%2].Schedule(r[i]); err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()
	if used := b.Used(); used <= 0 || used > b.limit {
		t.Fatalf("retained %d > budget %d", used, b.limit)
	}
	seen := map[int64]bool{}
	for pass := 0; pass < 400 && len(seen) < 400; pass++ {
		for _, s := range schedulers {
			for _, e := range s.GetReadyEvents() {
				if seen[e.Offset] {
					t.Fatalf("duplicate offset %d", e.Offset)
				}
				seen[e.Offset] = true
			}
			s.hydrate()
			if b.Used() > b.limit {
				t.Fatal("hydration exceeded shared budget")
			}
		}
	}
	if len(seen) != 400 {
		t.Fatalf("delivered %d of 400", len(seen))
	}
	if b.Used() != 0 {
		t.Fatalf("leaked %d reserved bytes", b.Used())
	}
}

func TestSchedulerBudgetPrioritizesEarlierColdEvent(t *testing.T) {
	r := budgetReader{
		0: {Offset: 0, ScheduleTs: time.Now().Add(time.Minute).UnixMilli(), Payload: make([]byte, 4096)},
		1: {Offset: 1, ScheduleTs: time.Now().Add(-time.Second).UnixMilli(), Payload: make([]byte, 4096)},
	}
	b := NewMemoryBudget(6000)
	s := newBudgetScheduler(t, b, r, 0)
	if err := s.Schedule(r[0]); err != nil {
		t.Fatal(err)
	}
	if err := s.Schedule(r[1]); err != nil {
		t.Fatal(err)
	}
	if n := s.hydrate(); n != 1 {
		t.Fatalf("hydrated %d, want earlier event", n)
	}
	ready := s.GetReadyEvents()
	if len(ready) != 1 || ready[0].Offset != 1 {
		t.Fatalf("earlier event was starved: %v", ready)
	}
	refs, err := s.coldStore.ScanRange(0, time.Now().Add(time.Hour).UnixMilli())
	if err != nil || len(refs) != 1 || refs[0] != 0 {
		t.Fatalf("later timer not retained on disk: %v %v", refs, err)
	}
	if err := s.Reset(); err != nil {
		t.Fatal(err)
	}
	if b.Used() != 0 {
		t.Fatal("reset leaked reservations")
	}
}

func TestColdScanIsPaged(t *testing.T) {
	b := NewMemoryBudget(6000)
	s := newBudgetScheduler(t, b, budgetReader{}, 0)
	for i := int64(0); i < 1500; i++ {
		if err := s.coldStore.Store(i, 1); err != nil {
			t.Fatal(err)
		}
	}
	refs, err := s.coldStore.ScanRangeLimit(0, 2, 128)
	if err != nil || len(refs) != 128 || refs[127] != 127 {
		t.Fatalf("page %v, err %v", refs, err)
	}
}

func TestBudgetWithDisabledHotWindowKeepsFutureTimer(t *testing.T) {
	e := &types.Event{Offset: 1, ScheduleTs: time.Now().Add(time.Minute).UnixMilli()}
	s, err := NewScheduler(t.TempDir(), 0, 10, 60, 0, budgetReader{1: e}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Stop()
	if err := s.SetMemoryBudget(NewMemoryBudget(1<<20), 1, nil); err != nil {
		t.Fatal(err)
	}
	if err := s.Schedule(e); err != nil {
		t.Fatal(err)
	}
	if s.GetTimingWheelDepth() != 1 || s.GetColdStoreCount() != 0 {
		t.Fatal("reserved future event was stranded as a cold reference")
	}
}

package scheduler

import (
	"math/rand"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

// The heap always offers the event due last, through any mix of additions
// and removals.
func TestRetainedHeapOffersTheLatest(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	var h retainedHeap
	items := map[int64]*retainedItem{}
	for step := 0; step < 20000; step++ {
		if len(items) == 0 || rng.Intn(3) > 0 {
			offset := int64(step)
			item := &retainedItem{event: &types.Event{Offset: offset, ScheduleTs: int64(rng.Intn(500))}}
			items[offset] = item
			h.push(item)
		} else {
			for offset, item := range items {
				h.remove(item)
				delete(items, offset)
				break
			}
		}
		var want *types.Event
		for _, item := range items {
			e := item.event
			if want == nil || e.ScheduleTs > want.ScheduleTs || e.ScheduleTs == want.ScheduleTs && e.Offset > want.Offset {
				want = e
			}
		}
		if got := h.latest(); got != want {
			t.Fatalf("step %d: latest = %v, want %v", step, got, want)
		}
		if len(h) != len(items) {
			t.Fatalf("step %d: heap holds %d, want %d", step, len(h), len(items))
		}
	}
}

// With the budget full of later timers, a publish of an earlier event takes
// the memory of the latest one, which goes to the cold store. The event used
// to go to the cold store itself and wait for the hydrator to bring it back.
func TestScheduleTakesMemoryFromTheLatestTimer(t *testing.T) {
	now := time.Now().UnixMilli()
	r := budgetReader{}
	s := newSoloBudgetScheduler(t, NewMemoryBudget(3*(512+4096+100)), r)
	for i := int64(0); i < 3; i++ {
		r[i] = &types.Event{Offset: i, ScheduleTs: now + (i+1)*60_000, Payload: make([]byte, 4096)}
		if err := s.Schedule(r[i]); err != nil {
			t.Fatal(err)
		}
	}
	if s.GetColdStoreCount() != 0 {
		t.Fatalf("%d events cold before the budget was full", s.GetColdStoreCount())
	}
	early := &types.Event{Offset: 10, ScheduleTs: now + 1000, Payload: make([]byte, 4096)}
	r[10] = early
	if err := s.Schedule(early); err != nil {
		t.Fatal(err)
	}
	refs, err := s.coldStore.ScanRange(0, now+time.Hour.Milliseconds())
	if err != nil {
		t.Fatal(err)
	}
	if len(refs) != 1 || refs[0] != 2 {
		t.Fatalf("cold store holds %v, want only the latest timer (offset 2)", refs)
	}
	s.budgetMu.Lock()
	_, held := s.reservations[10]
	s.budgetMu.Unlock()
	if !held {
		t.Fatal("the earlier event is not held in memory")
	}
	// A later event than everything held goes to the cold store.
	late := &types.Event{Offset: 11, ScheduleTs: now + 10*60_000, Payload: make([]byte, 4096)}
	if err := s.Schedule(late); err != nil {
		t.Fatal(err)
	}
	if refs, _ := s.coldStore.ScanRange(0, now+time.Hour.Milliseconds()); len(refs) != 2 {
		t.Fatalf("cold store holds %v, want offsets 2 and 11", refs)
	}
}

// newSoloBudgetScheduler gives one scheduler the whole budget.
func newSoloBudgetScheduler(t *testing.T, b *MemoryBudget, r EventReader) *Scheduler {
	t.Helper()
	s, err := NewScheduler(t.TempDir(), 0, 10, 60, 60, r, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Stop)
	if err := s.SetMemoryBudget(b, 1, nil); err != nil {
		t.Fatal(err)
	}
	return s
}

type countingReader struct {
	budgetReader
	reads atomic.Int64
}

func (r *countingReader) ReadEvent(offset int64) (*types.Event, error) {
	r.reads.Add(1)
	return r.budgetReader.ReadEvent(offset)
}

// A pass with room for a few events takes them earliest first, removes their
// cold references, and reads about as many from the log, not the 1024 a pass
// used to scan the cold store for.
func TestHydrationReadsWhatItCanTake(t *testing.T) {
	now := time.Now().UnixMilli()
	r := &countingReader{budgetReader: budgetReader{}}
	s := newSoloBudgetScheduler(t, NewMemoryBudget(5*(512+1024)), r)
	for i := int64(0); i < 2000; i++ {
		r.budgetReader[i] = &types.Event{Offset: i, ScheduleTs: now + 30_000 + i, Payload: make([]byte, 1024)}
		if err := s.coldStore.Store(i, now+30_000+i); err != nil {
			t.Fatal(err)
		}
	}
	took := s.hydrate()
	if took < 4 || took > 5 {
		t.Fatalf("took %d events with room for about 5", took)
	}
	if reads := r.reads.Load(); reads > 2*minHydrateChunk {
		t.Fatalf("read %d events from the log to take %d", reads, took)
	}
	// Taken in order, earliest first, and gone from the cold store.
	if refs, _ := s.coldStore.ScanRangeLimit(0, now+time.Hour.Milliseconds(), 1); len(refs) != 1 || refs[0] != int64(took) {
		t.Fatalf("first cold reference is %v, want offset %d", refs, took)
	}
}

// Making room for an earlier event leaves no cold copy of a timer that has
// already fired, or it would be delivered again from disk.
func TestReturningAFiredTimerLeavesNoColdCopy(t *testing.T) {
	now := time.Now().UnixMilli()
	r := budgetReader{}
	b := NewMemoryBudget(512 + 4096 + 100)
	s := newBudgetScheduler(t, b, r, 0)
	// Held, but not on the wheel: as an event whose timer has fired and
	// that waits in the ready queue.
	fired := &types.Event{Offset: 1, ScheduleTs: now + 60_000, Payload: make([]byte, 4096)}
	s.budgetMu.Lock()
	if !s.reserveEvent(fired) {
		s.budgetMu.Unlock()
		t.Fatal("could not hold the first event")
	}
	earlier := &types.Event{Offset: 2, ScheduleTs: now + 1000, Payload: make([]byte, 4096)}
	made := s.reserveEarlier(earlier)
	s.budgetMu.Unlock()
	if made {
		t.Fatal("room was made by returning a timer that is not on the wheel")
	}
	if n := s.GetColdStoreCount(); n != 0 {
		t.Fatalf("%d cold references left behind", n)
	}
	if ok, _ := s.coldStore.Contains(1, fired.ScheduleTs); ok {
		t.Fatal("the fired event has a cold copy and would be delivered twice")
	}
}

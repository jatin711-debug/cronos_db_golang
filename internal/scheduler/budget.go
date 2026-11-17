package scheduler

import (
	"cmp"
	"fmt"
	"log"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/internal/pebblemem"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

// MemoryBudget is shared by every scheduler on a node. Payload hydration is
// serialized so scans cannot each allocate another large record concurrently.
type MemoryBudget struct {
	limit     int64
	used      atomic.Int64
	hydrateMu sync.Mutex
}

func NewMemoryBudget(bytes int64) *MemoryBudget { return &MemoryBudget{limit: bytes} }
func (b *MemoryBudget) Used() int64             { return b.used.Load() }
func (b *MemoryBudget) reserve(bytes int64) bool {
	for {
		used := b.used.Load()
		if bytes > b.limit-used {
			return false
		}
		if b.used.CompareAndSwap(used, used+bytes) {
			return true
		}
	}
}

// SetMemoryBudget must run before scheduling or starting workers. Each
// partition gets a fair share, with room for one larger record when empty.
func (s *Scheduler) SetMemoryBudget(budget *MemoryBudget, partitions int, mem *pebblemem.Budget) error {
	if budget == nil || budget.limit <= 0 || s.eventReader == nil {
		return fmt.Errorf("scheduler budget requires a positive limit and WAL reader")
	}
	if s.coldStore == nil {
		store, err := NewColdStore(s.dataDir, mem)
		if err != nil {
			return err
		}
		s.coldStore = store
	}
	s.budget = budget
	s.budgetShare = budget.limit / int64(max(partitions, 1))
	s.reservations = make(map[int64]int64)
	s.retainedEvents = make(map[int64]*retainedItem)
	s.retainedOrder = nil
	return nil
}

// retainedBytes includes payload capacity, strings, metadata map overhead, the
// event, timer, and accounting entry. It is deliberately conservative.
func retainedBytes(e *types.Event) int64 {
	n := int64(512 + cap(e.Payload) + len(e.MessageId) + len(e.Topic) + cap(e.ProtoReflect().GetUnknown()))
	for k, v := range e.Meta {
		n += int64(128 + len(k) + len(v))
	}
	return n
}

// reserveEvent is called under budgetMu; true means newly retained.
func (s *Scheduler) reserveEvent(e *types.Event) bool {
	n := retainedBytes(e)
	if s.retained > 0 && n > s.budgetShare-s.retained {
		return false
	}
	if !s.budget.reserve(n) {
		return false
	}
	s.reservations[e.Offset] = n
	item := &retainedItem{event: e}
	s.retainedEvents[e.Offset] = item
	s.retainedOrder.push(item)
	s.retained += n
	return true
}

func (s *Scheduler) releaseEvent(offset int64) {
	if n, exists := s.reservations[offset]; exists {
		delete(s.reservations, offset)
		if item := s.retainedEvents[offset]; item != nil {
			s.retainedOrder.remove(item)
			delete(s.retainedEvents, offset)
		}
		s.retained -= n
		s.budget.used.Add(-n)
	}
}

// reserveEarlier makes room for e by returning later timers to the cold store,
// latest due first, until e fits. It stops at a timer that is not due later
// than e: the earliest work keeps the memory. The caller holds budgetMu.
//
// The timer is stored cold before it comes off the wheel, so it is never in
// neither place. One that is no longer on the wheel has fired, or is firing,
// and is delivered from memory: its cold copy is taken back at once, or the
// event would be delivered again from disk. The copy used to be left behind.
// The hydrator checks a cold entry is still there under budgetMu, which the
// caller holds throughout.
//
// It used to scan every event held to find the latest; the heap answers at
// once, so publishes can make room too (ScheduleBatch).
func (s *Scheduler) reserveEarlier(e *types.Event) bool {
	for {
		if s.reserveEvent(e) {
			return true
		}
		later := s.retainedOrder.latest()
		if later == nil || later.ScheduleTs <= e.ScheduleTs {
			return false
		}
		if err := s.coldStore.Store(later.Offset, later.ScheduleTs); err != nil {
			return false
		}
		if err := s.timingWheel.RemoveTimer(later.Offset); err != nil {
			if delErr := s.coldStore.Delete(later.Offset, later.ScheduleTs); delErr != nil {
				log.Printf("[SCHEDULER] Partition %d: offset %d fired while it was returned to the cold store, and its cold copy could not be removed; it may be delivered twice: %v", s.partitionID, later.Offset, delErr)
			}
			return false
		}
		s.releaseEvent(later.Offset)
	}
}

func (s *Scheduler) Schedule(event *types.Event) error {
	if s.budget == nil {
		return s.scheduleUnbudgeted(event)
	}
	return s.ScheduleBatch([]*types.Event{event})
}

func (s *Scheduler) ScheduleBatch(events []*types.Event) error {
	if s.budget == nil {
		return s.scheduleBatchUnbudgeted(events)
	}
	s.budgetMu.Lock()
	defer s.budgetMu.Unlock()
	hot := make([]*types.Event, 0, len(events))
	cold := make([]struct{ Offset, ScheduleTS int64 }, 0)
	now := time.Now().UnixMilli()
	// Earliest first: an event makes room by returning later timers to the
	// cold store, and must not return one of this batch it comes after,
	// which is not on the wheel yet.
	if len(events) > 1 && !slices.IsSortedFunc(events, byScheduleTime) {
		events = slices.SortedStableFunc(slices.Values(events), byScheduleTime)
	}
	for _, event := range events {
		if _, exists := s.reservations[event.Offset]; exists {
			continue
		}
		// An event due within the hot window takes memory from a timer due
		// later when the budget is full. It used to go to the cold store
		// instead and wake the hydrator, which brought it back by sending
		// the later timer to disk: with the budget full, every publish made
		// that round trip, and the timers went back and forth.
		if (s.hotWindowMs <= 0 || event.ScheduleTs <= now+s.hotWindowMs) && s.reserveEarlier(event) {
			hot = append(hot, event)
		} else {
			cold = append(cold, struct{ Offset, ScheduleTS int64 }{event.Offset, event.ScheduleTs})
		}
	}
	// Persist overflow before making retained events visible to the worker.
	if err := s.coldStore.StoreBatch(cold); err != nil {
		for _, event := range hot {
			s.releaseEvent(event.Offset)
		}
		return err
	}
	// The hydrator takes what comes due within its next window early; what
	// is due later waits for a later pass. Its passes are at least
	// hydratorWakeGap apart however often it is woken.
	s.mu.RLock()
	window := now + s.hotWindowMs + max(s.hydratorInterval.Milliseconds(), 10000)
	s.mu.RUnlock()
	for _, entry := range cold {
		if entry.ScheduleTS <= window {
			s.wakeHydrator()
			break
		}
	}
	if err := s.scheduleBatchUnbudgeted(hot); err != nil {
		// Keep reservations on partial scheduling failure; retry/reset releases
		// them, and capacity must never be reused while a payload is retained.
		return err
	}
	return nil
}

func byScheduleTime(a, b *types.Event) int {
	return cmp.Compare(a.ScheduleTs, b.ScheduleTs)
}

func (s *Scheduler) wakeHydrator() {
	select {
	case s.hydratorWake <- struct{}{}:
	default:
	}
}

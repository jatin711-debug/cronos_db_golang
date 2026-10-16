package scheduler

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
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
func (s *Scheduler) SetMemoryBudget(budget *MemoryBudget, partitions int, cache *pebble.Cache) error {
	if budget == nil || budget.limit <= 0 || s.eventReader == nil {
		return fmt.Errorf("scheduler budget requires a positive limit and WAL reader")
	}
	if s.coldStore == nil {
		store, err := NewColdStore(s.dataDir, cache)
		if err != nil {
			return err
		}
		s.coldStore = store
	}
	s.budget = budget
	s.budgetShare = budget.limit / int64(max(partitions, 1))
	s.reservations = make(map[int64]int64)
	s.retainedEvents = make(map[int64]*types.Event)
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
	s.retainedEvents[e.Offset] = e
	s.retained += n
	return true
}

func (s *Scheduler) releaseEvent(offset int64) {
	if n, exists := s.reservations[offset]; exists {
		delete(s.reservations, offset)
		delete(s.retainedEvents, offset)
		s.retained -= n
		s.budget.used.Add(-n)
	}
}

// Make room for earlier work by returning later timers to disk. This scan runs
// only in the background hydrator under pressure, never on publish admission.
// tickMu prevents an eviction racing the wheel's move to the ready queue.
func (s *Scheduler) reserveEarlier(e *types.Event) bool {
	for {
		if s.reserveEvent(e) {
			return true
		}
		var latest *types.Event
		for _, held := range s.retainedEvents {
			if held.ScheduleTs > e.ScheduleTs && (latest == nil || held.ScheduleTs > latest.ScheduleTs) {
				latest = held
			}
		}
		if latest == nil {
			return false
		}
		if err := s.coldStore.Store(latest.Offset, latest.ScheduleTs); err != nil {
			return false
		}
		if err := s.timingWheel.RemoveTimer(latest.Offset); err != nil {
			return false
		}
		s.releaseEvent(latest.Offset)
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
	for _, event := range events {
		if _, exists := s.reservations[event.Offset]; exists {
			continue
		}
		if (s.hotWindowMs <= 0 || event.ScheduleTs <= now+s.hotWindowMs) && s.reserveEvent(event) {
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
	if len(cold) > 0 {
		s.wakeHydrator()
	}
	if err := s.scheduleBatchUnbudgeted(hot); err != nil {
		// Keep reservations on partial scheduling failure; retry/reset releases
		// them, and capacity must never be reused while a payload is retained.
		return err
	}
	return nil
}

func (s *Scheduler) wakeHydrator() {
	select {
	case s.hydratorWake <- struct{}{}:
	default:
	}
}

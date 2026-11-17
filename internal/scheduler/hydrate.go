package scheduler

import (
	"fmt"
	"log"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

const (
	// maxHydratePerPass bounds how many cold references one pass takes.
	maxHydratePerPass = 1024
	// minHydrateChunk is the smallest number of references a pass reads.
	minHydrateChunk = 16
	// hydratorWakeGap is the least time between passes started by a wake.
	// Held events fire many times a second under load, and each firing
	// wakes the hydrator; it ran pass after pass, and each read the cold store
	// to take one or two events.
	hydratorWakeGap = 100 * time.Millisecond
	// hydrateLogInterval is how often the hydrator logs what it took.
	hydrateLogInterval = time.Minute
)

// hydrate moves cold references whose events come due within the hot window
// (and the lookahead) into the timing wheel, earliest first, as far as the
// memory budget allows. It returns how many it took.
//
// It works in chunks. The first is twice what the last pass took, at least
// 16, and the next is twice as large as long as everything read was taken.
// A pass used to read 1024 references whatever it could take: with the budget
// full it took one or two, and stepped over a thousand entries of the cold
// store, deleted ones among them, to find them.
func (s *Scheduler) hydrate() int {
	s.tickMu.Lock()
	chunk := min(max(2*s.lastHydrated, minHydrateChunk), maxHydratePerPass)
	s.tickMu.Unlock()

	total := 0
	for total < maxHydratePerPass {
		limit := min(chunk, maxHydratePerPass-total)
		took, read := s.hydrateChunk(limit)
		total += took
		if took < limit || read < limit {
			break // the budget stopped it, or the window holds no more
		}
		chunk = min(2*chunk, maxHydratePerPass)
	}

	s.tickMu.Lock()
	s.lastHydrated = total
	s.hydratedSinceLog += total
	if now := time.Now(); s.hydratedSinceLog > 0 && now.Sub(s.lastHydrateLog) >= hydrateLogInterval {
		log.Printf("[SCHEDULER] Hydrated %d events from the cold store into the timing wheel since %s (partition=%d)",
			s.hydratedSinceLog, s.lastHydrateLog.Format(time.TimeOnly), s.partitionID)
		s.hydratedSinceLog, s.lastHydrateLog = 0, now
	}
	s.tickMu.Unlock()
	if total > 0 {
		schedulerHydratedEvents.WithLabelValues(fmt.Sprintf("%d", s.partitionID)).Add(float64(total))
	}
	return total
}

// hydrateChunk takes up to limit cold references due within the window. It
// returns how many it took and how many it read.
//
// The events are read from the log before tickMu is taken: reading is the
// slow part, and while tickMu is held the wheel fires no timers. Under tickMu
// and budgetMu, each reference is checked to be still in the cold store with
// a point lookup, so what was removed or sent elsewhere meanwhile is left
// alone, and its cold entry is deleted as it is taken: a publish that makes
// room can return a timer taken a moment ago to the cold store, and a delete
// at the end of the pass would have removed that copy too.
func (s *Scheduler) hydrateChunk(limit int) (took, read int) {
	s.tickMu.Lock()
	resets := s.resets
	s.tickMu.Unlock()

	s.mu.RLock()
	lookaheadMs := max(s.hydratorInterval.Milliseconds(), 10000)
	s.mu.RUnlock()
	offsets, err := s.coldStore.ScanRangeLimit(0, time.Now().UnixMilli()+s.hotWindowMs+lookaheadMs, limit)
	if err != nil {
		log.Printf("[SCHEDULER] Hydrator scan error (partition=%d): %v", s.partitionID, err)
		return 0, 0
	}
	if len(offsets) == 0 {
		return 0, 0
	}
	events := make([]*types.Event, 0, len(offsets))
	for _, offset := range offsets {
		event, err := s.eventReader.ReadEvent(offset)
		if err != nil {
			log.Printf("[SCHEDULER] Hydrator read error at offset %d: %v", offset, err)
			continue
		}
		if event != nil {
			events = append(events, event)
		}
	}

	s.tickMu.Lock()
	defer s.tickMu.Unlock()
	if s.resets != resets {
		return 0, len(offsets) // the log may have been cut and written again; the next pass reads anew
	}
	if s.budget != nil {
		s.budget.hydrateMu.Lock()
		defer s.budget.hydrateMu.Unlock()
	}
	for _, event := range events {
		taken, keepGoing := s.hydrateOne(event)
		if taken {
			took++
		}
		if !keepGoing {
			break // earliest due first; the rest stays in the cold store
		}
	}
	return took, len(offsets)
}

// hydrateOne moves one cold reference into the wheel, or the ready queue if it
// is due. taken reports that it was moved; keepGoing is false when the budget
// has no room for it, so that nothing due later is taken before it. The caller
// holds tickMu.
func (s *Scheduler) hydrateOne(event *types.Event) (taken, keepGoing bool) {
	s.budgetMu.Lock()
	defer s.budgetMu.Unlock()
	present, err := s.coldStore.Contains(event.Offset, event.ScheduleTs)
	if err != nil {
		log.Printf("[SCHEDULER] Hydrator lookup error at offset %d: %v", event.Offset, err)
		return false, false
	}
	if !present {
		return false, true // removed, or taken, since it was read
	}
	if s.budget != nil {
		if _, held := s.reservations[event.Offset]; held {
			// Already on the wheel: the cold copy is redundant.
			s.deleteCold(event)
			return false, true
		}
		if !s.reserveEarlier(event) {
			return false, false
		}
	}
	now := time.Now().UnixMilli()
	if event.GetScheduleTs() <= now {
		s.readyMu.Lock()
		s.readyQueue = append(s.readyQueue, event)
		s.readyMu.Unlock()
		s.notifyReady()
	} else {
		timer := s.timingWheel.GetTimerFast(event.GetOffset(), event, now)
		if err := s.timingWheel.AddTimer(timer); err != nil {
			s.timingWheel.PutTimer(timer)
			if s.budget != nil {
				s.releaseEvent(event.Offset)
			}
			log.Printf("[SCHEDULER] Hydrator add timer error: %v", err)
			return false, true
		}
	}
	s.deleteCold(event)
	return true, true
}

func (s *Scheduler) deleteCold(event *types.Event) {
	if err := s.coldStore.Delete(event.Offset, event.ScheduleTs); err != nil {
		log.Printf("[SCHEDULER] Hydrator delete error at offset %d: %v", event.Offset, err)
	}
}

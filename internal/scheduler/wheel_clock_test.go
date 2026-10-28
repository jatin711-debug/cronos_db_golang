package scheduler

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

// slowLog is a WAL reader whose reads take a moment, as disk reads do.
type slowLog struct {
	mu     sync.Mutex
	events map[int64]*types.Event
	delay  time.Duration
}

func (l *slowLog) ReadEvent(offset int64) (*types.Event, error) {
	time.Sleep(l.delay)
	l.mu.Lock()
	defer l.mu.Unlock()
	e, ok := l.events[offset]
	if !ok {
		return nil, fmt.Errorf("offset %d not found", offset)
	}
	return e, nil
}

func (l *slowLog) put(e *types.Event) {
	l.mu.Lock()
	l.events[e.Offset] = e
	l.mu.Unlock()
}

// wheelLagMs is how far the wheel's clock is behind the wall clock.
func wheelLagMs(s *Scheduler) int64 {
	tw := s.timingWheel
	return time.Now().UnixMilli() - (tw.startTimeMs + tw.GetCurrentTick()*int64(tw.tickMs))
}

// A stall of the worker longer than a tick used to set the wheel back for
// good: the ticker drops the fires it is late for, and the wheel moved one tick
// per fire. Ten stalls of 100 ms left it about a second behind.
func TestWheelCatchesUpAfterStalls(t *testing.T) {
	s, err := NewScheduler(t.TempDir(), 0, 10, 600, 1, &slowLog{events: map[int64]*types.Event{}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Stop()
	s.Start()
	time.Sleep(300 * time.Millisecond)
	for i := 0; i < 10; i++ {
		s.tickMu.Lock()
		time.Sleep(100 * time.Millisecond)
		s.tickMu.Unlock()
		time.Sleep(50 * time.Millisecond)
	}
	time.Sleep(500 * time.Millisecond)
	if lag := wheelLagMs(s); lag > 100 {
		t.Fatalf("the wheel is %d ms behind the clock after ten 100 ms stalls; it should have caught up", lag)
	}
}

// Hydration holds tickMu while it reads the log. An event scheduled after a
// slow pass still fires when it is due.
func TestEventAfterSlowHydrationFiresOnTime(t *testing.T) {
	log := &slowLog{events: make(map[int64]*types.Event), delay: 2 * time.Millisecond}
	s, err := NewScheduler(t.TempDir(), 0, 10, 600, 1, log, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Stop()
	if err := s.SetMemoryBudget(NewMemoryBudget(64<<20), 1, nil); err != nil {
		t.Fatal(err)
	}
	s.Start()

	// Due beyond the one-minute hot window: they go to the cold store, and the
	// hydrator pulls them in, reading each one from the slow log.
	now := time.Now().UnixMilli()
	far := make([]*types.Event, 0, 1000)
	for i := int64(0); i < 1000; i++ {
		e := &types.Event{Offset: i, ScheduleTs: now + 65_000, Topic: "t", MessageId: fmt.Sprint(i)}
		log.put(e)
		far = append(far, e)
	}
	if err := s.ScheduleBatch(far); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(30 * time.Second)
	for s.GetColdStoreCount() > 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if s.GetColdStoreCount() > 0 {
		t.Fatalf("hydration did not finish: %d events still cold", s.GetColdStoreCount())
	}

	due := time.Now().UnixMilli() + 200
	hot := &types.Event{Offset: 5000, ScheduleTs: due, Topic: "t", MessageId: "hot"}
	log.put(hot)
	if err := s.Schedule(hot); err != nil {
		t.Fatal(err)
	}
	for limit := time.Now().Add(10 * time.Second); time.Now().Before(limit); time.Sleep(5 * time.Millisecond) {
		ready := s.GetReadyEvents()
		if len(ready) == 0 {
			continue
		}
		firedAt := time.Now().UnixMilli()
		if firedAt < due {
			t.Fatalf("the event fired %d ms early", due-firedAt)
		}
		if late := firedAt - due; late > 300 {
			t.Fatalf("an event due in 200 ms fired %d ms late after a slow hydration pass", late)
		}
		return
	}
	t.Fatal("the event did not fire")
}

// Catching up never expires a slot whose end the clock has not passed.
func TestAdvanceWheelDoesNotFireEarly(t *testing.T) {
	s, err := NewScheduler(t.TempDir(), 0, 10, 600, 1, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Stop()
	start := s.timingWheel.startTimeMs
	due := start + 5_000
	if err := s.Schedule(&types.Event{Offset: 1, ScheduleTs: due, Topic: "t", MessageId: "m"}); err != nil {
		t.Fatal(err)
	}
	s.tickMu.Lock()
	s.advanceWheel(due - 1)
	s.tickMu.Unlock()
	if ready := s.GetReadyEvents(); len(ready) != 0 {
		t.Fatalf("the event fired with the clock 1 ms before it was due")
	}
	for i := 0; i < 10 && len(s.GetReadyEvents()) == 0; i++ {
		s.tickMu.Lock()
		s.advanceWheel(due + 20)
		s.tickMu.Unlock()
		if ready := s.GetReadyEvents(); len(ready) == 1 {
			return
		}
	}
	t.Fatal("the event did not fire once the clock had passed it")
}

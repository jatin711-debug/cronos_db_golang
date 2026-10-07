package scheduler

import (
	"math/rand"
	"testing"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

// firedAt ticks the wheel until every timer has expired, or limit ticks have
// passed, and returns the tick at which each timer left the wheel.
func firedAt(tw *TimingWheel, timers int, limit int64) map[int64]int64 {
	fired := make(map[int64]int64, timers)
	for len(fired) < timers && tw.GetCurrentTick() < limit {
		tick := tw.GetCurrentTick()
		tw.Tick()
		for drained := false; !drained; {
			select {
			case batch := <-tw.GetExpiredChannel():
				for _, timer := range batch {
					fired[timer.EventID] = tick
				}
			default:
				drained = true
			}
		}
	}
	return fired
}

// A timer leaves the wheel on the tick of its expiry whichever level it
// started on, and wherever in a rotation the wheel was when it was added.
//
// The second half matters: a server adds its first far timer at an arbitrary
// moment, not at tick 0. An overflow level that counted its time from that
// moment, while being advanced on this wheel's rotations, handed timers down
// too early; they then wrapped around the lower wheel and fired a whole
// rotation before they were due.
func TestTimingWheel_FiresOnTheExpiryTickFromEveryLevel(t *testing.T) {
	const (
		tickMs    = 10
		wheelSize = 10 // level 0 spans 100 ms, level 1 one second, level 2 ten seconds
		startMs   = 1_700_000_000_000
	)
	rng := rand.New(rand.NewSource(1))

	for run := 0; run < 60; run++ {
		tw := NewTimingWheel(tickMs, wheelSize, 5, 0, startMs)
		type want struct{ added, due int64 }
		wants := make(map[int64]want)
		var last int64

		// Timers are added at several moments of the same wheel's life, so
		// overflow levels are both created and reused part-way through a
		// rotation.
		id := int64(0)
		for burst := 0; burst < 4; burst++ {
			for skip := rng.Intn(3 * wheelSize); skip > 0; skip-- {
				tw.Tick()
			}
			for n := 0; n < 5; n++ {
				id++
				delayTicks := int64(1 + rng.Intn(25*wheelSize)) // up to 2.5 s: reaches level 2
				now := tw.GetCurrentTick()
				event := &types.Event{Offset: id, ScheduleTs: startMs + (now+delayTicks)*tickMs}
				if err := tw.AddTimer(NewTimer(id, event)); err != nil {
					t.Fatalf("run %d: add timer %d: %v", run, id, err)
				}
				wants[id] = want{added: now, due: now + delayTicks}
				last = max(last, now+delayTicks)
			}
		}

		// Timers that came due while later bursts were being added have left
		// the wheel already; this test is about the ones still pending.
		for drained := false; !drained; {
			select {
			case batch := <-tw.GetExpiredChannel():
				for _, timer := range batch {
					delete(wants, timer.EventID)
				}
			default:
				drained = true
			}
		}

		fired := firedAt(tw, len(wants), last+3*wheelSize*wheelSize)
		for timerID, w := range wants {
			got, ok := fired[timerID]
			switch {
			case !ok:
				t.Fatalf("run %d: timer added at tick %d and due at tick %d never fired", run, w.added, w.due)
			case got != w.due:
				t.Fatalf("run %d: timer added at tick %d and due at tick %d fired at tick %d (%+d ticks)",
					run, w.added, w.due, got, got-w.due)
			}
		}
	}
}

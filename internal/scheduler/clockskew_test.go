package scheduler

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

// TestScheduler_RecoverNoClockSkew is a regression test for the restart clock-skew
// bug: recover() restored currentTick from the checkpoint while startTimeMs was
// re-sampled at boot, so the wheel's absolute "now" jumped ~uptime into the future
// and every timer with delay < uptime fired immediately (and the inflated tick was
// re-checkpointed, compounding across restarts). After the fix, recovery rebases
// the tick from wall clock so a genuinely-future timer scheduled post-recovery is
// NOT immediately ready.
func TestScheduler_RecoverNoClockSkew(t *testing.T) {
	dir := t.TempDir()
	const tickMs = 100

	// Simulate a prior run that accumulated a large tick count (1 hour of uptime)
	// with a start time one hour ago. This is exactly the state that skewed the
	// clock forward on restart.
	startTimeMs := time.Now().UnixMilli() - int64(time.Hour/time.Millisecond)
	cp := SchedulerCheckpoint{
		PartitionID:      0,
		CurrentTick:      int64(time.Hour/time.Millisecond) / tickMs, // ~1h worth of ticks
		ActiveTimers:     0,
		NextTickMs:       tickMs,
		WheelSize:        60,
		LastCheckpointTS: time.Now().UnixMilli(),
		StartTimeMs:      startTimeMs,
	}
	data, _ := json.Marshal(cp)
	if err := os.WriteFile(filepath.Join(dir, "timer_state.json"), data, 0644); err != nil {
		t.Fatalf("write checkpoint: %v", err)
	}

	sched, err := NewScheduler(dir, 0, tickMs, 60, 0, nil, nil)
	if err != nil {
		t.Fatalf("NewScheduler (recover): %v", err)
	}
	defer sched.Stop()

	// The wheel's absolute time must be ~now, not now + 1h.
	tw := sched.timingWheel
	absNow := tw.startTimeMs + tw.currentTick*int64(tw.tickMs)
	realNow := time.Now().UnixMilli()
	skew := absNow - realNow
	if skew < 0 {
		skew = -skew
	}
	// Allow a few seconds of slack for the tick granularity + test timing.
	if skew > 5000 {
		t.Fatalf("clock skew after recovery: wheel now=%d real now=%d skew=%dms", absNow, realNow, skew)
	}

	// Schedule a timer 30 minutes in the future; it must NOT be ready immediately.
	future := &types.Event{
		MessageId:  "future-1",
		ScheduleTs: realNow + int64(30*time.Minute/time.Millisecond),
		Payload:    []byte("p"),
		Topic:      "t",
	}
	if err := sched.Schedule(future); err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	if ready := sched.GetReadyEvents(); len(ready) != 0 {
		t.Fatalf("future timer fired immediately after recovery (clock skew): %d ready", len(ready))
	}
}

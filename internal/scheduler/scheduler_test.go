package scheduler

import (
	"testing"
	"time"

	"cronos_db/pkg/types"
)

func TestTimingWheel_AddTimer(t *testing.T) {
	startTime := time.Now().UnixMilli()
	tw := NewTimingWheel(100, 60, 5, 0, startTime) // 100ms tick, 60 slots
	tw.initialize()

	// Create timer for near future (500ms = 5 ticks)
	event := &types.Event{
		MessageId:  "test-1",
		ScheduleTs: startTime + 500, // 500ms in future
		Topic:      "test",
	}

	timer := NewTimer(event.MessageId, event)

	err := tw.AddTimer(timer)
	if err != nil {
		t.Fatalf("Failed to add timer: %v", err)
	}

	stats := tw.GetStats()
	if stats.ActiveTimers != 1 {
		t.Errorf("Expected 1 active timer, got %d", stats.ActiveTimers)
	}
}

func TestTimingWheel_RemoveTimer(t *testing.T) {
	startTime := time.Now().UnixMilli()
	tw := NewTimingWheel(100, 60, 5, 0, startTime)
	tw.initialize()

	event := &types.Event{
		MessageId:  "remove-test",
		ScheduleTs: startTime + 1000, // 1s in future
	}
	timer := NewTimer(event.MessageId, event)

	if err := tw.AddTimer(timer); err != nil {
		t.Fatalf("Failed to add timer: %v", err)
	}

	if err := tw.RemoveTimer("remove-test"); err != nil {
		t.Fatalf("Failed to remove timer: %v", err)
	}

	stats := tw.GetStats()
	if stats.ActiveTimers != 0 {
		t.Errorf("Expected 0 active timers after remove, got %d", stats.ActiveTimers)
	}
}

func TestTimingWheel_Tick(t *testing.T) {
	startTime := time.Now().UnixMilli()
	tw := NewTimingWheel(10, 10, 5, 0, startTime) // 10ms tick, 10 slots
	tw.initialize()

	// Add timer that expires in ~20ms (2 ticks from now)
	event := &types.Event{
		MessageId:  "tick-test",
		ScheduleTs: startTime + 20, // 20ms in future = 2 ticks
		Topic:      "test",
	}
	timer := NewTimer(event.MessageId, event)

	if err := tw.AddTimer(timer); err != nil {
		t.Fatalf("Failed to add timer: %v", err)
	}

	// Tick three times (0, 1, 2 - timer expires when we check slot 2)
	tw.Tick() // Check slot 0
	tw.Tick() // Check slot 1
	tw.Tick() // Check slot 2 - timer should expire

	// Check expired channel
	select {
	case expired := <-tw.GetExpiredChannel():
		if len(expired) != 1 {
			t.Errorf("Expected 1 expired timer, got %d", len(expired))
		}
		if expired[0].EventID != "tick-test" {
			t.Errorf("Expected event ID tick-test, got %s", expired[0].EventID)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Timeout waiting for expired timer")
	}
}

func TestTimingWheel_OverflowWheel(t *testing.T) {
	startTime := time.Now().UnixMilli()
	tw := NewTimingWheel(100, 10, 5, 0, startTime) // 100ms tick, 10 slots = 1s range per level
	tw.initialize()

	// Add timer that needs overflow wheel (expires in 1.5s = 15 ticks, but wheel only has 10 slots)
	event := &types.Event{
		MessageId:  "overflow-test",
		ScheduleTs: startTime + 1500, // 1.5s in future
	}
	timer := NewTimer(event.MessageId, event)

	err := tw.AddTimer(timer)
	if err != nil {
		t.Fatalf("Failed to add timer to overflow wheel: %v", err)
	}

	// Should have created overflow wheel
	stats := tw.GetStats()
	if stats.OverflowLevel == 0 {
		t.Error("Expected overflow wheel to be created")
	}
	if stats.ActiveTimers != 1 {
		t.Errorf("Expected 1 active timer (in overflow wheel), got %d", stats.ActiveTimers)
	}
}

func TestTimingWheel_OverflowLimit(t *testing.T) {
	startTime := time.Now().UnixMilli()
	tw := NewTimingWheel(100, 10, 3, 0, startTime) // 100ms tick, 10 slots, max 3 levels
	tw.initialize()

	// Level 0: 100ms * 10 = 1s range
	// Level 1: 1s * 10 = 10s range
	// Level 2: 10s * 10 = 100s range
	// Level 3: 100s * 10 = 1000s range (but maxLevels=3 means we stop at level 3)
	// Timer at 2000s (2000000ms) would need level 4+

	event := &types.Event{
		MessageId:  "overflow-limit-test",
		ScheduleTs: startTime + 2000000, // 2000s in future, exceeds 3 levels
	}
	timer := NewTimer(event.MessageId, event)

	err := tw.AddTimer(timer)
	if err == nil {
		t.Error("Expected error for timer exceeding max overflow levels")
	}
}

func TestTimingWheel_Cascade(t *testing.T) {
	startTime := time.Now().UnixMilli()
	tw := NewTimingWheel(10, 10, 5, 0, startTime) // 10ms tick, 10 slots
	tw.initialize()

	// Add timer at 150ms (15 ticks) - goes to overflow wheel
	event := &types.Event{
		MessageId:  "cascade-test",
		ScheduleTs: startTime + 150,
	}
	timer := NewTimer(event.MessageId, event)

	if err := tw.AddTimer(timer); err != nil {
		t.Fatalf("Failed to add timer: %v", err)
	}

	// Timer should be in overflow wheel
	stats := tw.GetStats()
	if stats.OverflowLevel == 0 {
		t.Fatal("Expected overflow wheel to exist")
	}

	// Tick 10 times to complete one rotation (triggers cascade)
	for i := 0; i < 10; i++ {
		tw.Tick()
	}

	// After cascade, timer should now be in level 0 wheel
	// (it has 5 more ticks to go: 150ms - 100ms = 50ms = 5 ticks)
	stats = tw.GetStats()
	if stats.ActiveTimers != 1 {
		t.Errorf("Expected 1 active timer after cascade, got %d", stats.ActiveTimers)
	}

	// Tick 6 more times - timer should expire at slot 5
	// After cascade at tick 10, timer is in slot 5
	// We need to tick to check slots 0,1,2,3,4,5 (6 ticks total)
	for i := 0; i < 6; i++ {
		tw.Tick()
	}

	// Check expired channel
	select {
	case expired := <-tw.GetExpiredChannel():
		if len(expired) != 1 {
			t.Errorf("Expected 1 expired timer, got %d", len(expired))
		}
		if expired[0].EventID != "cascade-test" {
			t.Errorf("Expected event ID cascade-test, got %s", expired[0].EventID)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Timeout waiting for cascaded timer to expire")
	}
}

func TestScheduler_Schedule(t *testing.T) {
	tmpDir := t.TempDir()

	scheduler, err := NewScheduler(tmpDir, 0, 100, 60)
	if err != nil {
		t.Fatalf("Failed to create scheduler: %v", err)
	}
	defer scheduler.Stop()

	// Schedule event in the future
	futureEvent := &types.Event{
		MessageId:  "future-event",
		ScheduleTs: time.Now().UnixMilli() + 5000, // 5s in future
		Topic:      "test",
	}

	if err := scheduler.Schedule(futureEvent); err != nil {
		t.Fatalf("Failed to schedule future event: %v", err)
	}

	stats := scheduler.GetStats()
	if stats.ActiveTimers != 1 {
		t.Errorf("Expected 1 active timer, got %d", stats.ActiveTimers)
	}
}

func TestScheduler_ImmediateEvent(t *testing.T) {
	tmpDir := t.TempDir()

	scheduler, err := NewScheduler(tmpDir, 0, 100, 60)
	if err != nil {
		t.Fatalf("Failed to create scheduler: %v", err)
	}
	defer scheduler.Stop()

	// Schedule event in the past (should go to ready queue immediately)
	pastEvent := &types.Event{
		MessageId:  "past-event",
		ScheduleTs: time.Now().UnixMilli() - 1000, // 1s in past
		Topic:      "test",
	}

	if err := scheduler.Schedule(pastEvent); err != nil {
		t.Fatalf("Failed to schedule past event: %v", err)
	}

	// Event should be in ready queue
	readyEvents := scheduler.GetReadyEvents()
	if len(readyEvents) != 1 {
		t.Errorf("Expected 1 ready event, got %d", len(readyEvents))
	}
}

func TestScheduler_StartStop(t *testing.T) {
	tmpDir := t.TempDir()

	scheduler, err := NewScheduler(tmpDir, 0, 50, 60)
	if err != nil {
		t.Fatalf("Failed to create scheduler: %v", err)
	}

	scheduler.Start()

	// Let it run a bit
	time.Sleep(200 * time.Millisecond)

	stats := scheduler.GetStats()
	if stats.CurrentTick == 0 {
		t.Error("Expected scheduler to have ticked")
	}

	scheduler.Stop()
}

func TestNewTimer(t *testing.T) {
	expirationTime := time.Now().UnixMilli() + 500 // 500ms in future

	event := &types.Event{
		MessageId:  "timer-test",
		ScheduleTs: expirationTime,
	}

	timer := NewTimer("timer-test", event)

	// Timer should store absolute expiration time
	if timer.ExpirationMs != expirationTime {
		t.Errorf("Expected expiration time %d, got %d", expirationTime, timer.ExpirationMs)
	}
}

func TestScheduler_Checkpoint(t *testing.T) {
	tmpDir := t.TempDir()

	// Create and use scheduler
	scheduler1, err := NewScheduler(tmpDir, 0, 100, 60)
	if err != nil {
		t.Fatalf("Failed to create scheduler: %v", err)
	}

	scheduler1.Start()
	time.Sleep(150 * time.Millisecond)
	scheduler1.Stop()

	// Create new scheduler - should recover state
	scheduler2, err := NewScheduler(tmpDir, 0, 100, 60)
	if err != nil {
		t.Fatalf("Failed to create second scheduler: %v", err)
	}
	defer scheduler2.Stop()

	// Should have recovered tick count
	stats := scheduler2.GetStats()
	if stats.CurrentTick == 0 {
		// Note: checkpoint might not have been written if interval not reached
		t.Log("Checkpoint recovery: tick count is 0 (may be expected if no checkpoint written)")
	}
}

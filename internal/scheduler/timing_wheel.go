package scheduler

import (
	"container/list"
	"fmt"
	"log"
	"sync"
	"time"

	"cronos_db/pkg/types"
)

// TimingWheel implements a hierarchical timing wheel for efficient timer management
type TimingWheel struct {
	mu            sync.RWMutex
	tickMs        int32
	wheelSize     int32
	currentTick   int64
	startTimeMs   int64        // Absolute start time of the root wheel (Unix ms)
	wheel         []*list.List // Array of time slots
	overflowWheel *TimingWheel // Higher level wheel
	timers        map[string]*Timer
	expired       chan []*Timer
	quit          chan struct{}
	maxLevels     int32 // Maximum number of overflow wheel levels
	currentLevel  int32 // Current level in the hierarchy (0 = root)
}

// Timer represents a scheduled event
type Timer struct {
	EventID      string
	Event        *types.Event
	ExpirationMs int64 // Absolute expiration time in milliseconds (Unix timestamp)
	SlotIndex    int32
	CreatedTS    int64
}

// NewTimingWheel creates a new timing wheel
// startTimeMs is the absolute start time (only used by root wheel, pass 0 for overflow wheels)
func NewTimingWheel(tickMs int32, wheelSize int32, maxLevels int32, currentLevel int32, startTimeMs int64) *TimingWheel {
	return &TimingWheel{
		tickMs:       tickMs,
		wheelSize:    wheelSize,
		currentTick:  0,
		startTimeMs:  startTimeMs,
		wheel:        make([]*list.List, wheelSize),
		timers:       make(map[string]*Timer),
		expired:      make(chan []*Timer, 100),
		quit:         make(chan struct{}),
		maxLevels:    maxLevels,
		currentLevel: currentLevel,
	}
}

// initialize initializes wheel slots
func (tw *TimingWheel) initialize() {
	for i := int32(0); i < tw.wheelSize; i++ {
		tw.wheel[i] = list.New()
	}
}

// AddTimer adds a timer to the wheel
func (tw *TimingWheel) AddTimer(timer *Timer) error {
	tw.mu.Lock()
	defer tw.mu.Unlock()

	return tw.addTimerLocked(timer)
}

// addTimerLocked adds a timer (must be called with lock held)
func (tw *TimingWheel) addTimerLocked(timer *Timer) error {
	// Check if timer already exists
	if _, exists := tw.timers[timer.EventID]; exists {
		return fmt.Errorf("timer %s already exists", timer.EventID)
	}

	// Calculate current absolute time for this wheel
	currentAbsoluteMs := tw.startTimeMs + tw.currentTick*int64(tw.tickMs)

	// Calculate delay in milliseconds from now
	delayMs := timer.ExpirationMs - currentAbsoluteMs
	if delayMs < 0 {
		delayMs = 0
	}

	// Convert delay to ticks for this wheel's tick interval
	delayTicks := delayMs / int64(tw.tickMs)

	log.Printf("[TIMING-WHEEL] AddTimer: event=%s, expirationMs=%d, currentAbsMs=%d, delayMs=%d, delayTicks=%d, level=%d",
		timer.EventID, timer.ExpirationMs, currentAbsoluteMs, delayMs, delayTicks, tw.currentLevel)

	// Check if timer fits in current wheel
	if delayTicks < int64(tw.wheelSize) {
		// Fits in current wheel - place in appropriate slot
		slotIndex := int32((tw.currentTick + delayTicks) % int64(tw.wheelSize))
		timer.SlotIndex = slotIndex
		log.Printf("[TIMING-WHEEL] AddTimer: event %s placed in slot %d (level=%d, wheelSize=%d)",
			timer.EventID, slotIndex, tw.currentLevel, tw.wheelSize)
		tw.wheel[slotIndex].PushBack(timer)
		tw.timers[timer.EventID] = timer
		return nil
	}

	// Timer doesn't fit - needs overflow wheel
	if tw.maxLevels > 0 && tw.currentLevel >= tw.maxLevels {
		return fmt.Errorf("timer %s scheduled too far in the future (exceeds max overflow levels %d)",
			timer.EventID, tw.maxLevels)
	}

	log.Printf("[TIMING-WHEEL] AddTimer: event %s needs overflow wheel (delayTicks=%d > wheelSize=%d)",
		timer.EventID, delayTicks, tw.wheelSize)

	// Create overflow wheel if needed
	if tw.overflowWheel == nil {
		nextLevel := tw.currentLevel + 1
		// Overflow wheel has larger tick interval: tickMs * wheelSize
		// Its start time is the same as the root wheel's start time
		overflowTickMs := tw.tickMs * tw.wheelSize
		tw.overflowWheel = NewTimingWheel(overflowTickMs, tw.wheelSize, tw.maxLevels, nextLevel, tw.startTimeMs)
		tw.overflowWheel.initialize()
		log.Printf("[TIMING-WHEEL] Created overflow wheel: level=%d, tickMs=%d", nextLevel, overflowTickMs)
	}

	// Add to overflow wheel (it will calculate its own delay based on its tick interval)
	return tw.overflowWheel.addTimerLocked(timer)
}

// RemoveTimer removes a timer
func (tw *TimingWheel) RemoveTimer(eventID string) error {
	tw.mu.Lock()
	defer tw.mu.Unlock()

	timer, exists := tw.timers[eventID]
	if !exists {
		return fmt.Errorf("timer %s not found", eventID)
	}

	// Remove from wheel
	for e := tw.wheel[timer.SlotIndex].Front(); e != nil; e = e.Next() {
		t := e.Value.(*Timer)
		if t.EventID == eventID {
			tw.wheel[timer.SlotIndex].Remove(e)
			delete(tw.timers, eventID)
			return nil
		}
	}

	// Check overflow wheel
	if tw.overflowWheel != nil {
		return tw.overflowWheel.RemoveTimer(eventID)
	}

	return fmt.Errorf("timer %s not found in wheel", eventID)
}

// Tick advances the timing wheel by one tick
func (tw *TimingWheel) Tick() {
	tw.mu.Lock()
	defer tw.mu.Unlock()

	tw.tickLocked()
}

// tickLocked advances the wheel (must be called with lock held)
func (tw *TimingWheel) tickLocked() {
	// Get current slot
	currentSlot := int32(tw.currentTick % int64(tw.wheelSize))

	// Get all timers in current slot
	expiredTimers := make([]*Timer, 0)
	for e := tw.wheel[currentSlot].Front(); e != nil; e = e.Next() {
		timer := e.Value.(*Timer)
		expiredTimers = append(expiredTimers, timer)
		delete(tw.timers, timer.EventID)
	}

	// Clear the slot
	tw.wheel[currentSlot].Init()

	// Send expired timers to channel
	if len(expiredTimers) > 0 {
		log.Printf("[TIMING-WHEEL] Tick=%d expired %d events (slot=%d, level=%d)",
			tw.currentTick, len(expiredTimers), currentSlot, tw.currentLevel)
		select {
		case tw.expired <- expiredTimers:
		default:
			// Channel full, drop expired timers (should not happen in practice)
			log.Printf("[TIMING-WHEEL] WARNING: Expired channel full, dropping %d timers", len(expiredTimers))
		}
	}

	// Advance tick
	tw.currentTick++

	// When we complete a full rotation, cascade timers from overflow wheel
	if tw.overflowWheel != nil && tw.currentTick%int64(tw.wheelSize) == 0 {
		log.Printf("[TIMING-WHEEL] Cascading from overflow wheel (tick=%d, level=%d)", tw.currentTick, tw.currentLevel)
		tw.cascadeFromOverflow()
	}
}

// cascadeFromOverflow ticks the overflow wheel and brings expired timers back to this wheel
func (tw *TimingWheel) cascadeFromOverflow() {
	if tw.overflowWheel == nil {
		return
	}

	// Lock overflow wheel
	tw.overflowWheel.mu.Lock()
	defer tw.overflowWheel.mu.Unlock()

	// Advance overflow wheel's tick first
	tw.overflowWheel.currentTick++
	overflowTick := tw.overflowWheel.currentTick

	// Get the slot for the current tick
	overflowSlot := int32(overflowTick % int64(tw.overflowWheel.wheelSize))

	log.Printf("[TIMING-WHEEL] Cascade: overflow wheel tick=%d, checking slot=%d, level=%d",
		overflowTick, overflowSlot, tw.overflowWheel.currentLevel)

	// Move all timers from overflow slot to this wheel
	timersToMove := make([]*Timer, 0)
	for e := tw.overflowWheel.wheel[overflowSlot].Front(); e != nil; e = e.Next() {
		timer := e.Value.(*Timer)
		timersToMove = append(timersToMove, timer)
		delete(tw.overflowWheel.timers, timer.EventID)
	}

	// Clear the overflow slot
	tw.overflowWheel.wheel[overflowSlot].Init()

	// Re-add timers to this wheel (they will now fit within our range)
	for _, timer := range timersToMove {
		log.Printf("[TIMING-WHEEL] Cascading timer %s from level %d to level %d",
			timer.EventID, tw.currentLevel+1, tw.currentLevel)
		// Re-insert into this wheel - it should now fit within our range
		if err := tw.addTimerLocked(timer); err != nil {
			log.Printf("[TIMING-WHEEL] WARNING: Failed to cascade timer %s: %v", timer.EventID, err)
		}
	}

	// Recursively cascade if overflow wheel also completed a rotation
	if tw.overflowWheel.overflowWheel != nil &&
		overflowTick%int64(tw.overflowWheel.wheelSize) == 0 {
		tw.overflowWheel.cascadeFromOverflow()
	}
}

// AdvanceTo advances the wheel to a specific tick
func (tw *TimingWheel) AdvanceTo(targetTick int64) error {
	if targetTick < tw.currentTick {
		return fmt.Errorf("cannot advance backwards")
	}

	// Tick until we reach target
	for tw.currentTick < targetTick {
		tw.Tick()
		time.Sleep(time.Duration(tw.tickMs) * time.Millisecond)
	}

	return nil
}

// GetExpiredChannel returns the channel for expired timers
func (tw *TimingWheel) GetExpiredChannel() <-chan []*Timer {
	return tw.expired
}

// GetStats returns timing wheel statistics
func (tw *TimingWheel) GetStats() *SchedulerStats {
	tw.mu.RLock()
	defer tw.mu.RUnlock()

	activeTimers := int64(len(tw.timers))
	if tw.overflowWheel != nil {
		activeTimers += tw.overflowWheel.GetStats().ActiveTimers
	}

	return &SchedulerStats{
		ActiveTimers: activeTimers,
		CurrentTick:  tw.currentTick,
		TickMs:       tw.tickMs,
		WheelSize:    tw.wheelSize,
		OverflowLevel: func() int32 {
			if tw.overflowWheel == nil {
				return 0
			}
			return 1 + tw.overflowWheel.GetStats().OverflowLevel
		}(),
	}
}

// Close closes the timing wheel
func (tw *TimingWheel) Close() {
	close(tw.quit)
	if tw.overflowWheel != nil {
		tw.overflowWheel.Close()
	}
}

// SchedulerStats represents scheduler statistics
type SchedulerStats struct {
	ActiveTimers  int64
	CurrentTick   int64
	TickMs        int32
	WheelSize     int32
	OverflowLevel int32
	ReadyEvents   int64
}

// NewTimer creates a new timer
// eventID is the unique identifier for this timer
// event is the event to be triggered
// The timer stores the absolute expiration time from the event's schedule timestamp
func NewTimer(eventID string, event *types.Event) *Timer {
	return &Timer{
		EventID:      eventID,
		Event:        event,
		ExpirationMs: event.GetScheduleTs(), // Store absolute expiration time
		CreatedTS:    time.Now().UnixMilli(),
	}
}

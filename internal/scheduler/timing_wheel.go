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
	mu              sync.RWMutex
	tickMs          int32
	wheelSize       int32
	currentTick     int64
	wheel           []*list.List // Array of time slots
	overflowWheel   *TimingWheel // Higher level wheel
	timers          map[string]*Timer // EventID -> Timer
	expired         chan []*Timer
	quit            chan struct{}
	maxLevels       int32 // Maximum number of overflow wheel levels to prevent infinite recursion
	currentLevel    int32 // Current level in the hierarchy (0 = root)
}

// Timer represents a scheduled event
type Timer struct {
	EventID        string
	Event          *types.Event
	ExpirationTick int64
	SlotIndex      int32
	CreatedTS      int64
}

// NewTimingWheel creates a new timing wheel
// maxLevels limits the number of overflow wheel levels (0 = no limit, use with caution)
// currentLevel is the current level in the hierarchy (0 = root)
func NewTimingWheel(tickMs int32, wheelSize int32, maxLevels int32, currentLevel int32) *TimingWheel {
	return &TimingWheel{
		tickMs:      tickMs,
		wheelSize:   wheelSize,
		currentTick: 0,
		wheel:       make([]*list.List, wheelSize),
		timers:      make(map[string]*Timer),
		expired:     make(chan []*Timer, 100),
		quit:        make(chan struct{}),
		maxLevels:   maxLevels,
		currentLevel: currentLevel,
	}
}

// Initialize wheel slots
func (tw *TimingWheel) initialize() {
	for i := int32(0); i < tw.wheelSize; i++ {
		tw.wheel[i] = list.New()
	}
}

// AddTimer adds a timer to the wheel
func (tw *TimingWheel) AddTimer(timer *Timer) error {
	tw.mu.Lock()
	defer tw.mu.Unlock()

	// Check if timer already exists
	if _, exists := tw.timers[timer.EventID]; exists {
		return fmt.Errorf("timer %s already exists", timer.EventID)
	}

	// Calculate which wheel level to place timer
	// timer.ExpirationTick and tw.currentTick are both tick counts (not milliseconds)
	delayTicks := timer.ExpirationTick - tw.currentTick

	log.Printf("[TIMING-WHEEL] AddTimer: event=%s, expirationTick=%d, currentTick=%d, delayTicks=%d, level=%d",
		timer.EventID, timer.ExpirationTick, tw.currentTick, delayTicks, tw.currentLevel)

	if delayTicks < int64(tw.wheelSize) {
		// Fits in current wheel
		slotIndex := int32((tw.currentTick + delayTicks) % int64(tw.wheelSize))
		timer.SlotIndex = slotIndex
		log.Printf("[TIMING-WHEEL] AddTimer: event %s placed in slot %d (wheelSize=%d)",
			timer.EventID, slotIndex, tw.wheelSize)
		tw.wheel[slotIndex].PushBack(timer)
		tw.timers[timer.EventID] = timer
	} else {
		// Needs overflow wheel - prevent infinite recursion
		if tw.maxLevels > 0 && tw.currentLevel >= tw.maxLevels {
			return fmt.Errorf("timer %s scheduled too far in the future (exceeds max overflow levels %d)",
				timer.EventID, tw.maxLevels)
		}
		log.Printf("[TIMING-WHEEL] AddTimer: event %s needs overflow wheel (delayTicks=%d > wheelSize=%d)",
			timer.EventID, delayTicks, tw.wheelSize)
		if tw.overflowWheel == nil {
			// Create overflow wheel with larger ticks
			nextLevel := tw.currentLevel + 1
			tw.overflowWheel = NewTimingWheel(tw.tickMs*tw.wheelSize, tw.wheelSize, tw.maxLevels, nextLevel)
			tw.overflowWheel.initialize()
		}
		return tw.overflowWheel.AddTimer(timer)
	}

	return nil
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

	// Propagate to overflow wheel
	if tw.overflowWheel != nil && tw.currentTick%int64(tw.wheelSize) == 0 {
		log.Printf("[TIMING-WHEEL] Propagating tick to overflow wheel (tick=%d)", tw.currentTick)
		tw.overflowWheel.Tick()
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
// startTimeMs is the time when the scheduler started (Unix ms)
// tickMs is the tick duration for the ROOT wheel
func NewTimer(eventID string, event *types.Event, tickMs int32, startTimeMs int64) *Timer {
	// Calculate expiration tick relative to scheduler start
	// delayMs is how far in the future the event should fire
	delayMs := event.GetScheduleTs() - startTimeMs
	// Convert to ticks (relative to root wheel's tick size)
	expirationTick := delayMs / int64(tickMs)

	return &Timer{
		EventID:        eventID,
		Event:          event,
		ExpirationTick: expirationTick,
		CreatedTS:      time.Now().UnixMilli(),
	}
}

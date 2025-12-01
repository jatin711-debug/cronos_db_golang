package scheduler

import (
	"container/list"
	"fmt"
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
func NewTimingWheel(tickMs int32, wheelSize int32) *TimingWheel {
	if tickMs <= 0 {
		tickMs = 1
	}
	if wheelSize <= 0 {
		wheelSize = 1
	}

	tw := &TimingWheel{
		tickMs:    tickMs,
		wheelSize: wheelSize,
		currentTick: time.Now().UnixMilli() / int64(tickMs), // Initialize with current time tick
		wheel:     make([]*list.List, wheelSize),
		timers:    make(map[string]*Timer),
		expired:   make(chan []*Timer, 100),
		quit:      make(chan struct{}),
	}
	tw.initialize()
	return tw
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

	// Calculate expiration tick for *this specific wheel level*
	// timer.Event.ScheduleTs is absolute MS.
	// tw.tickMs is the tick duration of this wheel level.
	// This calculation ensures we compare apples to apples.
	expirationTick := timer.Event.GetScheduleTs() / int64(tw.tickMs)

	// Calculate delay in ticks
	delayTicks := expirationTick - tw.currentTick

	if delayTicks < 0 {
		delayTicks = 0
	}

	if delayTicks < int64(tw.wheelSize) {
		// Fits in current wheel
		slotIndex := int32((tw.currentTick + delayTicks) % int64(tw.wheelSize))
		if slotIndex < 0 {
			slotIndex = 0
		}
		timer.SlotIndex = slotIndex
		tw.wheel[slotIndex].PushBack(timer)
		tw.timers[timer.EventID] = timer
	} else {
		// Needs overflow wheel
		if tw.overflowWheel == nil {
			// Create overflow wheel
			// Overflow wheel creates a new wheel that ticks slower
			nextTickMs := int64(tw.tickMs) * int64(tw.wheelSize)
			if nextTickMs > 2147483647 { // MaxInt32
				nextTickMs = 2147483647
			}

			tw.overflowWheel = NewTimingWheel(int32(nextTickMs), tw.wheelSize)

			// Initialize overflow wheel current tick based on current tick
			// It should be roughly currentTick / wheelSize
			if tw.wheelSize > 0 {
				tw.overflowWheel.currentTick = tw.currentTick / int64(tw.wheelSize)
			}
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
		select {
		case tw.expired <- expiredTimers:
		default:
			// Channel full, drop expired timers (should not happen in practice)
		}
	}

	// Advance tick
	tw.currentTick++

	// Propagate to overflow wheel
	if tw.overflowWheel != nil && tw.currentTick%int64(tw.wheelSize) == 0 {
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
func NewTimer(eventID string, event *types.Event, tickMs int32) *Timer {
	return &Timer{
		EventID:        eventID,
		Event:          event,
		ExpirationTick: event.GetScheduleTs() / int64(tickMs),
		CreatedTS:      time.Now().UnixMilli(),
	}
}

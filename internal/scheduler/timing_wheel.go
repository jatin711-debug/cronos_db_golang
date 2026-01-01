package scheduler

import (
	"fmt"
	"strconv"
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
	wheel         []*Timer     // Array of time slots (linked list heads)
	overflowWheel *TimingWheel // Higher level wheel
	timers        map[string]*Timer
	expired       chan []*Timer
	quit          chan struct{}
	maxLevels     int32 // Maximum number of overflow wheel levels
	currentLevel  int32 // Current level in the hierarchy (0 = root)
	timerPool     *sync.Pool
}

// Timer represents a scheduled event
type Timer struct {
	EventID      string
	Event        *types.Event
	ExpirationMs int64 // Absolute expiration time in milliseconds (Unix timestamp)
	SlotIndex    int32
	CreatedTS    int64

	// Intrusive list pointers
	prev *Timer
	next *Timer
}

// NewTimingWheel creates a new timing wheel
// startTimeMs is the absolute start time (only used by root wheel, pass 0 for overflow wheels)
func NewTimingWheel(tickMs int32, wheelSize int32, maxLevels int32, currentLevel int32, startTimeMs int64) *TimingWheel {
	tw := &TimingWheel{
		tickMs:       tickMs,
		wheelSize:    wheelSize,
		currentTick:  0,
		startTimeMs:  startTimeMs,
		wheel:        make([]*Timer, wheelSize), // Slice of list heads
		timers:       make(map[string]*Timer),
		expired:      make(chan []*Timer, 100000), // 100K buffer for burst loads
		quit:         make(chan struct{}),
		maxLevels:    maxLevels,
		currentLevel: currentLevel,
		timerPool: &sync.Pool{
			New: func() interface{} {
				return &Timer{}
			},
		},
	}
	// No initialization loop needed for nil headers
	return tw
}

// initialize initializes wheel slots (legacy, now empty as slice is zero-inited)
func (tw *TimingWheel) initialize() {
	// No-op for intrusive list slice
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

	// Check if timer fits in current wheel
	if delayTicks < int64(tw.wheelSize) {
		// Fits in current wheel - place in appropriate slot
		slotIndex := int32((tw.currentTick + delayTicks) % int64(tw.wheelSize))
		timer.SlotIndex = slotIndex

		// Insert into intrusive list
		tw.insertTimer(slotIndex, timer)

		tw.timers[timer.EventID] = timer
		return nil
	}

	// Timer doesn't fit - needs overflow wheel
	if tw.maxLevels > 0 && tw.currentLevel >= tw.maxLevels {
		return fmt.Errorf("timer %s scheduled too far in the future (exceeds max overflow levels %d)",
			timer.EventID, tw.maxLevels)
	}

	// Create overflow wheel if needed
	if tw.overflowWheel == nil {
		nextLevel := tw.currentLevel + 1
		// Overflow wheel has larger tick interval: tickMs * wheelSize
		// Its start time is the same as the root wheel's start time
		overflowTickMs := tw.tickMs * tw.wheelSize
		tw.overflowWheel = NewTimingWheel(overflowTickMs, tw.wheelSize, tw.maxLevels, nextLevel, tw.startTimeMs)
		// Share pool with overflow wheel
		tw.overflowWheel.timerPool = tw.timerPool
		tw.overflowWheel.initialize()
	}

	// Add to overflow wheel (it will calculate its own delay based on its tick interval)
	return tw.overflowWheel.addTimerLocked(timer)
}

// insertTimer inserts a timer into the slot's list (at tail for FIFOish behavior, or head for speed - head is O(1))
// We'll insert at head for O(1) simplicity.
func (tw *TimingWheel) insertTimer(slot int32, timer *Timer) {
	head := tw.wheel[slot]

	timer.next = head
	timer.prev = nil

	if head != nil {
		head.prev = timer
	}

	tw.wheel[slot] = timer
}

// removeTimerFromList removes a timer from the linked list
func (tw *TimingWheel) removeTimerFromList(timer *Timer) {
	if timer.prev != nil {
		timer.prev.next = timer.next
	} else {
		// Logic to update head if we are removing the first element
		// But we need to know WHICH slot. Timer stores SlotIndex.
		// If timer.prev is nil, it MUST be the head of tw.wheel[timer.SlotIndex]
		tw.wheel[timer.SlotIndex] = timer.next
	}

	if timer.next != nil {
		timer.next.prev = timer.prev
	}

	timer.next = nil
	timer.prev = nil
}

// RemoveTimer removes a timer
func (tw *TimingWheel) RemoveTimer(eventID string) error {
	tw.mu.Lock()
	defer tw.mu.Unlock()

	timer, exists := tw.timers[eventID]
	if !exists {
		// Check overflow wheel
		if tw.overflowWheel != nil {
			return tw.overflowWheel.RemoveTimer(eventID)
		}
		return fmt.Errorf("timer %s not found", eventID)
	}

	// Remove from intrusive list
	tw.removeTimerFromList(timer)

	delete(tw.timers, eventID)

	// Return to pool? No, caller/expire logic handles putTimer usually, but here we are cancelling.
	// So we should put it back.
	tw.PutTimer(timer)

	return nil
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

	// Iterate intrusive list
	curr := tw.wheel[currentSlot]
	for curr != nil {
		next := curr.next // Save next before modifying

		expiredTimers = append(expiredTimers, curr)
		delete(tw.timers, curr.EventID)

		// Clear links
		curr.next = nil
		curr.prev = nil

		curr = next
	}

	// Clear the slot head
	tw.wheel[currentSlot] = nil

	// Send expired timers to channel (non-blocking with large buffer)
	// If buffer is full, we MUST NOT drop - use blocking send
	if len(expiredTimers) > 0 {
		select {
		case tw.expired <- expiredTimers:
			// Sent successfully
		default:
			// Channel full - send in goroutine to avoid blocking tick
			// This ensures no events are lost
			go func(timers []*Timer) {
				tw.expired <- timers
			}(expiredTimers)
		}
	}

	// Advance tick
	tw.currentTick++

	// When we complete a full rotation, cascade timers from overflow wheel
	if tw.overflowWheel != nil && tw.currentTick%int64(tw.wheelSize) == 0 {
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

	// Move all timers from overflow slot to this wheel
	timersToMove := make([]*Timer, 0)

	curr := tw.overflowWheel.wheel[overflowSlot]
	for curr != nil {
		next := curr.next

		timersToMove = append(timersToMove, curr)
		delete(tw.overflowWheel.timers, curr.EventID)

		curr.prev = nil
		curr.next = nil

		curr = next
	}

	// Clear the overflow slot
	tw.overflowWheel.wheel[overflowSlot] = nil

	// Re-add timers to this wheel (they will now fit within our range)
	for _, timer := range timersToMove {
		// Re-insert into this wheel - it should now fit within our range
		tw.addTimerLocked(timer)
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

// NewTimer creates a new timer (deprecated, use GetTimer)
func NewTimer(eventID string, event *types.Event) *Timer {
	return &Timer{
		EventID:      eventID,
		Event:        event,
		ExpirationMs: event.GetScheduleTs(), // Store absolute expiration time
		CreatedTS:    time.Now().UnixMilli(),
	}
}

// GetTimer gets a timer from the pool or creates a new one
func (tw *TimingWheel) GetTimer(eventID string, event *types.Event) *Timer {
	timer := tw.timerPool.Get().(*Timer)
	timer.EventID = eventID
	timer.Event = event
	timer.ExpirationMs = event.GetScheduleTs()
	timer.CreatedTS = time.Now().UnixMilli()
	timer.SlotIndex = -1
	timer.next = nil
	timer.prev = nil
	return timer
}

// GetTimerFast gets a timer using offset as ID (avoids string allocation)
func (tw *TimingWheel) GetTimerFast(offset int64, event *types.Event) *Timer {
	timer := tw.timerPool.Get().(*Timer)
	// Optimization: FormatInt is slightly faster than Sprintf, but still allocates.
	// Ideally we would change EventID to be an interface{} or int64, but that requires
	// larger refactoring of map keys.
	timer.EventID = strconv.FormatInt(offset, 10)
	timer.Event = event
	timer.ExpirationMs = event.GetScheduleTs()
	timer.CreatedTS = time.Now().UnixMilli()
	timer.SlotIndex = -1
	timer.next = nil
	timer.prev = nil
	return timer
}

// PutTimer returns a timer to the pool
func (tw *TimingWheel) PutTimer(timer *Timer) {
	timer.Event = nil
	timer.EventID = ""
	timer.next = nil
	timer.prev = nil
	tw.timerPool.Put(timer)
}

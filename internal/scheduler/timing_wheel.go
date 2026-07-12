package scheduler

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

// expiredSlicePool pools []*Timer slices to eliminate per-tick allocations.
var expiredSlicePool = sync.Pool{
	New: func() interface{} {
		return make([]*Timer, 0, 64)
	},
}

// TimingWheel implements a hierarchical timing wheel for efficient timer management
type TimingWheel struct {
	mu            sync.RWMutex
	currentTick   int64
	startTimeMs   int64        // Absolute start time of the root wheel (Unix ms)
	wheel         []*Timer     // Array of time slots (linked list heads)
	overflowWheel *TimingWheel // Higher level wheel
	timers        map[int64]*Timer
	expired       chan []*Timer
	quit          chan struct{}
	quitOnce      sync.Once
	timerPool     *sync.Pool
	expiredBuf    []*Timer   // Reusable scratch buffer for tick processing
	cascadeBuf    [][]*Timer // Reusable bucket buffer for cascade operations
	overflowRetry []*Timer   // Timers deferred for re-insertion when the expired channel is full; drained outside the wheel lock to avoid blocking publishers.
	tickMs        int32
	wheelSize     int32
	maxLevels     int32 // Maximum number of overflow wheel levels
	currentLevel  int32 // Current level in the hierarchy (0 = root)
}

// Timer represents a scheduled event.
// Fields are ordered for optimal cache alignment:
//   - Pointers together (8B each) to minimize GC scan overhead
//   - Hot int64 fields together (accessed every tick)
//   - Cold int32 field last
type Timer struct {
	next         *Timer
	prev         *Timer
	Event        *types.Event
	ExpirationMs int64 // Absolute expiration time in milliseconds (Unix timestamp)
	EventID      int64 // Offset-based ID (was string, now int64 — zero alloc)
	CreatedTS    int64
	SlotIndex    int32
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
		timers:       make(map[int64]*Timer),
		expired:      make(chan []*Timer, 100000), // 100K buffer for burst loads
		quit:         make(chan struct{}),
		maxLevels:    maxLevels,
		currentLevel: currentLevel,
		cascadeBuf:   make([][]*Timer, wheelSize), // Pre-allocate cascade buckets
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
		return fmt.Errorf("timer %d already exists", timer.EventID)
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
		return fmt.Errorf("timer %d scheduled too far in the future (exceeds max overflow levels %d)",
			timer.EventID, tw.maxLevels)
	}

	// Create overflow wheel if needed
	if tw.overflowWheel == nil {
		nextLevel := tw.currentLevel + 1
		// Overflow wheel has larger tick interval: tickMs * wheelSize
		// Its start time should be the absolute time at current tick position
		// This ensures proper cascade timing when timers move back from overflow
		overflowTickMs := tw.tickMs * tw.wheelSize
		overflowStartTime := tw.startTimeMs + tw.currentTick*int64(tw.tickMs)
		tw.overflowWheel = NewTimingWheel(overflowTickMs, tw.wheelSize, tw.maxLevels, nextLevel, overflowStartTime)
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
func (tw *TimingWheel) RemoveTimer(eventID int64) error {
	tw.mu.Lock()
	defer tw.mu.Unlock()

	timer, exists := tw.timers[eventID]
	if !exists {
		// Check overflow wheel
		if tw.overflowWheel != nil {
			return tw.overflowWheel.RemoveTimer(eventID)
		}
		return fmt.Errorf("timer %d not found", eventID)
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
	tw.tickLocked()
	// Grab any deferred overflow-retry timers under the lock, then release so
	// publishers can interleave while we re-insert. The previous implementation
	// re-inserted these inside tickLocked via addTimerLocked, holding tw.mu for
	// an O(N) critical section that blocked every concurrent AddTimer on the
	// publish path. Draining here keeps the lock hold bounded.
	retry := tw.overflowRetry
	tw.overflowRetry = nil
	tw.mu.Unlock()

	// Re-insert the deferred timers outside the wheel lock. AddTimers re-takes
	// the lock briefly for the batch, but publishers are no longer blocked for
	// the entire burst-recovery window.
	for len(retry) > 0 {
		_ = tw.AddTimers(retry)
		// AddTimers may have re-populated overflowRetry if the channel is still
		// saturated; loop until stable. Guard against pathological livelock by
		// bailing out (the next Tick will retry).
		tw.mu.Lock()
		next := tw.overflowRetry
		tw.overflowRetry = nil
		tw.mu.Unlock()
		if len(next) == 0 || len(next) >= len(retry) {
			// No progress — log and break to avoid a tight loop; the timers
			// remain in next and will be retried by the following Tick.
			if len(next) > 0 {
				log.Printf("[TIMING-WHEEL] WARNING: expired channel still saturated, deferring %d timers to next tick", len(next))
				tw.mu.Lock()
				tw.overflowRetry = append(tw.overflowRetry, next...)
				tw.mu.Unlock()
			}
			break
		}
		retry = next
	}
}

// tickLocked advances the wheel (must be called with lock held)
func (tw *TimingWheel) tickLocked() {
	// Get current slot
	currentSlot := int32(tw.currentTick % int64(tw.wheelSize))

	// Get all timers in current slot - reuse buffer to avoid per-tick allocation
	tw.expiredBuf = tw.expiredBuf[:0]

	// Iterate intrusive list
	curr := tw.wheel[currentSlot]
	for curr != nil {
		next := curr.next // Save next before modifying

		tw.expiredBuf = append(tw.expiredBuf, curr)
		delete(tw.timers, curr.EventID)

		// Clear links
		curr.next = nil
		curr.prev = nil

		curr = next
	}

	// Clear the slot head
	tw.wheel[currentSlot] = nil

	// Send expired timers via non-blocking send to prevent wheel stalls.
	// Use pooled slice to avoid per-tick allocation.
	if len(tw.expiredBuf) > 0 {
		expiredCopy := expiredSlicePool.Get().([]*Timer)
		expiredCopy = append(expiredCopy[:0], tw.expiredBuf...)
		select {
		case tw.expired <- expiredCopy:
		default:
			// Channel full — try once more with short yield
			select {
			case tw.expired <- expiredCopy:
			default:
				// Channel still full: defer re-insertion to overflowRetry and
				// drain it AFTER releasing tw.mu (see Tick). The previous code
				// called addTimerLocked in a loop here, which held tw.mu for an
				// O(N) critical section and blocked all publishers on the same
				// lock. Deferring keeps the lock hold to a single slice append.
				expiredSlicePool.Put(expiredCopy)
				tw.overflowRetry = append(tw.overflowRetry, tw.expiredBuf...)
			}
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
// Optimized to use bucket-based insertion instead of O(N) individual adds
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

	// Get current absolute time for this wheel
	currentAbsoluteMs := tw.startTimeMs + tw.currentTick*int64(tw.tickMs)

	// Collect timers in slot-indexed buckets using pre-allocated cascade buffer.
	// Reset all bucket slices (keeping underlying arrays for reuse).
	for i := int32(0); i < tw.wheelSize; i++ {
		tw.cascadeBuf[i] = tw.cascadeBuf[i][:0]
	}
	timersBySlot := tw.cascadeBuf

	curr := tw.overflowWheel.wheel[overflowSlot]
	for curr != nil {
		next := curr.next

		// Calculate which slot this timer should go into in this wheel
		delayMs := curr.ExpirationMs - currentAbsoluteMs
		if delayMs < 0 {
			delayMs = 0
		}
		delayTicks := delayMs / int64(tw.tickMs)
		targetSlot := int32((tw.currentTick + delayTicks) % int64(tw.wheelSize))

		curr.SlotIndex = targetSlot
		timersBySlot[targetSlot] = append(timersBySlot[targetSlot], curr)

		delete(tw.overflowWheel.timers, curr.EventID)
		curr.prev = nil
		curr.next = nil

		curr = next
	}

	// Clear the overflow slot
	tw.overflowWheel.wheel[overflowSlot] = nil

	// Batch insert all timers into their target slots - O(N) total
	for slot := int32(0); slot < tw.wheelSize; slot++ {
		timers := timersBySlot[slot]
		// Prepend all timers for this slot to the existing head (if any)
		if len(timers) > 0 {
			head := tw.wheel[slot]
			// Reverse order so they end up in correct relative order (FIFO)
			for i := len(timers)/2 - 1; i >= 0; i-- {
				j := len(timers) - 1 - i
				timers[i], timers[j] = timers[j], timers[i]
			}
			// Link batch timers internally (prev/next between consecutive timers)
			for i := 1; i < len(timers); i++ {
				timers[i].next = timers[i-1]
				timers[i-1].prev = timers[i]
			}
			// Connect first timer (tail of batch) to existing slot head
			timers[0].next = head
			timers[0].prev = nil
			if head != nil {
				head.prev = timers[0]
			}
			// Set last timer (head of batch) as new slot head
			lastTimer := timers[len(timers)-1]
			lastTimer.prev = nil
			tw.wheel[slot] = lastTimer

			// Add all timers to the timers map
			for _, t := range timers {
				tw.timers[t.EventID] = t
			}
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

	return tw.getStatsLocked()
}

// getStatsLocked returns stats without acquiring lock (caller must hold at least RLock).
// Prevents recursive RLock deadlock when accessing overflow wheel stats.
func (tw *TimingWheel) getStatsLocked() *SchedulerStats {
	activeTimers := int64(len(tw.timers))
	overflowLevel := int32(0)

	if tw.overflowWheel != nil {
		// Access overflow wheel directly — we already hold tw.mu,
		// and cascadeFromOverflow already locks overflow independently.
		tw.overflowWheel.mu.RLock()
		overflowStats := tw.overflowWheel.getStatsLocked()
		tw.overflowWheel.mu.RUnlock()
		activeTimers += overflowStats.ActiveTimers
		overflowLevel = 1 + overflowStats.OverflowLevel
	}

	return &SchedulerStats{
		ActiveTimers:  activeTimers,
		CurrentTick:   tw.currentTick,
		TickMs:        tw.tickMs,
		WheelSize:     tw.wheelSize,
		OverflowLevel: overflowLevel,
	}
}

// Close closes the timing wheel. Safe to call multiple times.
func (tw *TimingWheel) Close() {
	tw.quitOnce.Do(func() { close(tw.quit) })
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
func NewTimer(eventID int64, event *types.Event) *Timer {
	return &Timer{
		EventID:      eventID,
		Event:        event,
		ExpirationMs: event.GetScheduleTs(), // Store absolute expiration time
		CreatedTS:    time.Now().UnixMilli(),
	}
}

// GetTimer gets a timer from the pool or creates a new one
func (tw *TimingWheel) GetTimer(eventID int64, event *types.Event) *Timer {
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

// GetTimerFast gets a timer using offset as ID with pre-sampled time.
// Use this in batch operations where time.Now() is sampled once for the entire batch.
func (tw *TimingWheel) GetTimerFast(offset int64, event *types.Event, nowMs int64) *Timer {
	timer := tw.timerPool.Get().(*Timer)
	timer.EventID = offset
	timer.Event = event
	timer.ExpirationMs = event.GetScheduleTs()
	timer.CreatedTS = nowMs
	timer.SlotIndex = -1
	timer.next = nil
	timer.prev = nil
	return timer
}

// AddTimers adds multiple timers to the wheel with a single lock acquisition
func (tw *TimingWheel) AddTimers(timers []*Timer) error {
	tw.mu.Lock()
	defer tw.mu.Unlock()

	for _, timer := range timers {
		if err := tw.addTimerLocked(timer); err != nil {
			return err
		}
	}
	return nil
}

// GetCurrentTick returns the current tick in a thread-safe manner
func (tw *TimingWheel) GetCurrentTick() int64 {
	tw.mu.RLock()
	defer tw.mu.RUnlock()
	return tw.currentTick
}

// PutTimer returns a timer to the pool
func (tw *TimingWheel) PutTimer(timer *Timer) {
	timer.Event = nil
	timer.EventID = 0
	timer.next = nil
	timer.prev = nil
	tw.timerPool.Put(timer)
}

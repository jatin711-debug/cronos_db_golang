package scheduler

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
	"cronos_db/pkg/types"
)

// Scheduler manages timestamp-triggered event execution
type Scheduler struct {
	mu              sync.RWMutex
	timingWheel     *TimingWheel
	readyQueue      []*types.Event
	partitionID     int32
	dataDir         string
	active          bool
	workerDone      chan struct{}
	stats           *SchedulerStats
	lastCheckpointTS int64
	startTimeMs     int64 // Scheduler start time (Unix ms)
}

// NewScheduler creates a new scheduler
func NewScheduler(dataDir string, partitionID int32, tickMs int32, wheelSize int32) (*Scheduler, error) {
	// Create data directory
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("create scheduler data dir: %w", err)
	}

	startTime := time.Now().UnixMilli()

	scheduler := &Scheduler{
		timingWheel:   NewTimingWheel(tickMs, wheelSize, 10, 0), // Limit to 10 overflow levels
		readyQueue:    make([]*types.Event, 0),
		partitionID:   partitionID,
		dataDir:       dataDir,
		active:        false,
		workerDone:    make(chan struct{}),
		stats:         &SchedulerStats{},
		lastCheckpointTS: time.Now().UnixMilli(),
		startTimeMs:   startTime,
	}

	// Initialize timing wheel
	scheduler.timingWheel.initialize()

	// Recover state if exists
	if err := scheduler.recover(); err != nil {
		return nil, fmt.Errorf("recover scheduler: %w", err)
	}

	return scheduler, nil
}

// Schedule schedules an event
func (s *Scheduler) Schedule(event *types.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if event is already scheduled
	// For now, we'll use message_id as timer ID
	eventID := event.GetMessageId()

	log.Printf("[SCHEDULER] Scheduling event %s for partition %d, scheduleTs=%d (now=%d)",
		eventID, s.partitionID, event.GetScheduleTs(), time.Now().UnixMilli())

	// If event is already expired, add to ready queue
	if event.GetScheduleTs() <= time.Now().UnixMilli() {
		log.Printf("[SCHEDULER] Event %s is already expired, adding to ready queue", eventID)
		s.readyQueue = append(s.readyQueue, event)
		return nil
	}

	// Create timer and add to timing wheel
	timer := NewTimer(eventID, event, s.timingWheel.tickMs, s.startTimeMs)
	log.Printf("[SCHEDULER] Created timer for event %s, expirationTick=%d",
		eventID, timer.ExpirationTick)

	return s.timingWheel.AddTimer(timer)
}

// GetReadyEvents returns events ready for execution
func (s *Scheduler) GetReadyEvents() []*types.Event {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Get expired events from timing wheel
	select {
	case expiredTimers := <-s.timingWheel.GetExpiredChannel():
		log.Printf("[SCHEDULER] GetReadyEvents: received %d expired timers", len(expiredTimers))
		for _, timer := range expiredTimers {
			s.readyQueue = append(s.readyQueue, timer.Event)
		}
	default:
		// No new expired events
	}

	// Return ready events
	if len(s.readyQueue) == 0 {
		return nil
	}

	log.Printf("[SCHEDULER] GetReadyEvents: returning %d events", len(s.readyQueue))
	events := s.readyQueue
	s.readyQueue = make([]*types.Event, 0)
	return events
}

// Start starts the scheduler worker
func (s *Scheduler) Start() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.active {
		return
	}

	s.active = true
	go s.worker()
}

// worker is the scheduler worker loop
func (s *Scheduler) worker() {
	log.Printf("[SCHEDULER-WORKER] Started for partition %d", s.partitionID)
	ticker := time.NewTicker(time.Duration(s.timingWheel.tickMs) * time.Millisecond)
	defer ticker.Stop()

	for s.active {
		select {
		case <-ticker.C:
			s.timingWheel.Tick()
			log.Printf("[SCHEDULER-WORKER] Tick received, advancing timing wheel, currentTick=%d", s.timingWheel.currentTick)
			s.checkpoint()

		case <-s.workerDone:
			return
		}
	}
}

// checkpoint saves scheduler state
func (s *Scheduler) checkpoint() {
	now := time.Now().UnixMilli()

	// Checkpoint every 10 seconds
	if now-s.lastCheckpointTS < 10000 {
		return
	}

	s.mu.Lock()

	// Build checkpoint
	checkpoint := &SchedulerCheckpoint{
		PartitionID:   s.partitionID,
		CurrentTick:   s.timingWheel.currentTick,
		ActiveTimers:  int64(len(s.timingWheel.timers)),
		ReadyEvents:   int64(len(s.readyQueue)),
		NextTickMs:    int64(s.timingWheel.tickMs),
		WheelSize:     int64(s.timingWheel.wheelSize),
		LastCheckpointTS: now,
	}

	// Write to file
	checkpointPath := filepath.Join(s.dataDir, "timer_state.json")
	data, err := json.MarshalIndent(checkpoint, "", "  ")
	if err != nil {
		s.mu.Unlock()
		return
	}

	tmpPath := checkpointPath + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		s.mu.Unlock()
		return
	}

	os.Rename(tmpPath, checkpointPath)
	s.lastCheckpointTS = now
	s.mu.Unlock()
}

// recover recovers scheduler state
func (s *Scheduler) recover() error {
	checkpointPath := filepath.Join(s.dataDir, "timer_state.json")

	data, err := os.ReadFile(checkpointPath)
	if err != nil {
		if os.IsNotExist(err) {
			// No checkpoint, fresh start
			return nil
		}
		return fmt.Errorf("read checkpoint: %w", err)
	}

	var checkpoint SchedulerCheckpoint
	if err := json.Unmarshal(data, &checkpoint); err != nil {
		return fmt.Errorf("unmarshal checkpoint: %w", err)
	}

	// Restore state
	s.timingWheel.currentTick = checkpoint.CurrentTick
	s.stats.ActiveTimers = checkpoint.ActiveTimers

	return nil
}

// Stop stops the scheduler
func (s *Scheduler) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.active {
		return
	}

	s.active = false
	close(s.workerDone)

	// Final checkpoint
	s.checkpoint()
}

// GetStats returns scheduler statistics
func (s *Scheduler) GetStats() *SchedulerStats {
	s.mu.RLock()
	defer s.mu.RUnlock()

	twStats := s.timingWheel.GetStats()
	return &SchedulerStats{
		ActiveTimers:  twStats.ActiveTimers,
		CurrentTick:   twStats.CurrentTick,
		TickMs:        twStats.TickMs,
		WheelSize:     twStats.WheelSize,
		OverflowLevel: twStats.OverflowLevel,
		ReadyEvents:   int64(len(s.readyQueue)),
	}
}

// GetNextEventTime returns the time of the next event to trigger
func (s *Scheduler) GetNextEventTime() time.Time {
	s.mu.RLock()
	defer s.mu.RUnlock()

	nextTick := s.timingWheel.currentTick + 1
	nextTime := time.UnixMilli(nextTick * int64(s.timingWheel.tickMs))
	return nextTime
}

// SchedulerCheckpoint represents scheduler checkpoint state
type SchedulerCheckpoint struct {
	PartitionID     int32  `json:"partition_id"`
	CurrentTick     int64  `json:"current_tick"`
	ActiveTimers    int64  `json:"active_timers"`
	ReadyEvents     int64  `json:"ready_events"`
	NextTickMs      int64  `json:"next_tick_ms"`
	WheelSize       int64  `json:"wheel_size"`
	LastCheckpointTS int64 `json:"last_checkpoint_ts"`
}

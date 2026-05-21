package scheduler

import (
	"cronos_db/pkg/types"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	schedulerReadyEvents = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "cronos_scheduler_ready_events",
			Help: "Number of events in the scheduler ready queue waiting for dispatch",
		},
		[]string{"partition"},
	)
	schedulerActiveTimers = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "cronos_scheduler_active_timers",
			Help: "Number of active timers in the timing wheel",
		},
		[]string{"partition"},
	)
)

// Scheduler manages timestamp-triggered event execution
type Scheduler struct {
	mu               sync.RWMutex
	timingWheel      *TimingWheel
	readyQueue       []*types.Event
	readySignal      chan struct{}
	partitionID      int32
	dataDir          string
	active           bool
	workerDone       chan struct{}
	stats            *SchedulerStats
	lastCheckpointTS int64
	startTimeMs      int64 // Scheduler start time (Unix ms)
}

// NewScheduler creates a new scheduler
func NewScheduler(dataDir string, partitionID int32, tickMs int32, wheelSize int32) (*Scheduler, error) {
	// Create data directory
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("create scheduler data dir: %w", err)
	}

	startTime := time.Now().UnixMilli()

	scheduler := &Scheduler{
		timingWheel:      NewTimingWheel(tickMs, wheelSize, 10, 0, startTime), // Pass startTime to root wheel
		readyQueue:       make([]*types.Event, 0),
		readySignal:      make(chan struct{}, 1),
		partitionID:      partitionID,
		dataDir:          dataDir,
		active:           false,
		workerDone:       make(chan struct{}),
		stats:            &SchedulerStats{},
		lastCheckpointTS: time.Now().UnixMilli(),
		startTimeMs:      startTime,
	}

	// Initialize timing wheel
	scheduler.timingWheel.initialize()

	// Recover state if exists
	if err := scheduler.recover(); err != nil {
		return nil, fmt.Errorf("recover scheduler: %w", err)
	}

	return scheduler, nil
}

func (s *Scheduler) notifyReady() {
	select {
	case s.readySignal <- struct{}{}:
	default:
	}
}

// ReadySignal returns a notification channel that is signaled when new ready
// events are available.
func (s *Scheduler) ReadySignal() <-chan struct{} {
	return s.readySignal
}

// Schedule schedules an event
func (s *Scheduler) Schedule(event *types.Event) error {
	now := time.Now().UnixMilli()
	// If event is already expired, add to ready queue
	if event.GetScheduleTs() <= now {
		s.mu.Lock()
		s.readyQueue = append(s.readyQueue, event)
		s.mu.Unlock()
		s.notifyReady()
		return nil
	}

	// Create timer and add to timing wheel (locks tw.mu)
	timer := s.timingWheel.GetTimerFast(event.GetOffset(), event, now)
	if err := s.timingWheel.AddTimer(timer); err != nil {
		s.timingWheel.PutTimer(timer)
		return err
	}
	return nil
}

// ScheduleBatch schedules multiple events efficiently
func (s *Scheduler) ScheduleBatch(events []*types.Event) error {
	if len(events) == 0 {
		return nil
	}

	now := time.Now().UnixMilli()
	var readyEvents []*types.Event
	var wheelTimers []*Timer

	for _, event := range events {
		// If event is already expired, add to ready queue
		if event.GetScheduleTs() <= now {
			readyEvents = append(readyEvents, event)
			continue
		}

		// Create timer with pre-sampled time
		timer := s.timingWheel.GetTimerFast(event.GetOffset(), event, now)
		wheelTimers = append(wheelTimers, timer)
	}

	if len(wheelTimers) > 0 {
		if err := s.timingWheel.AddTimers(wheelTimers); err != nil {
			// Put timers back to pool on error to avoid leak
			for _, t := range wheelTimers {
				s.timingWheel.PutTimer(t)
			}
			return err
		}
	}

	if len(readyEvents) > 0 {
		s.mu.Lock()
		s.readyQueue = append(s.readyQueue, readyEvents...)
		s.mu.Unlock()
		s.notifyReady()
	}

	return nil
}

// GetReadyEvents returns events ready for execution
func (s *Scheduler) GetReadyEvents() []*types.Event {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Return ready events
	if len(s.readyQueue) == 0 {
		return nil
	}

	events := s.readyQueue
	s.readyQueue = s.readyQueue[:0]
	return events
}

func (s *Scheduler) drainExpiredToReady() {
	// FIX: Drain channel WITHOUT holding s.mu to prevent lock contention
	// between the timing wheel tick and concurrent Schedule/GetReadyEvents calls.
	var localBuf []*types.Event
	for {
		select {
		case expiredTimers := <-s.timingWheel.GetExpiredChannel():
			for _, timer := range expiredTimers {
				localBuf = append(localBuf, timer.Event)
				s.timingWheel.PutTimer(timer)
			}
		default:
			goto done
		}
	}
done:
	if len(localBuf) > 0 {
		s.mu.Lock()
		s.readyQueue = append(s.readyQueue, localBuf...)
		s.mu.Unlock()
		s.notifyReady()
	}
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
	go s.checkpointLoop()
}

// worker is the scheduler worker loop
func (s *Scheduler) worker() {
	log.Printf("[SCHEDULER-WORKER] Started for partition %d", s.partitionID)
	ticker := time.NewTicker(time.Duration(s.timingWheel.tickMs) * time.Millisecond)
	defer ticker.Stop()

	partitionLabel := fmt.Sprintf("%d", s.partitionID)
	var tickCount int64

	for {
		select {
		case <-ticker.C:
			s.timingWheel.Tick()
			s.drainExpiredToReady()

			// FIX: Only emit metrics every 10th tick to reduce
			// RLock acquisitions and GetStats overhead
			tickCount++
			if tickCount%10 == 0 {
				s.mu.RLock()
				readyCount := int64(len(s.readyQueue))
				twStats := s.timingWheel.GetStats()
				s.mu.RUnlock()
				schedulerReadyEvents.WithLabelValues(partitionLabel).Set(float64(readyCount))
				schedulerActiveTimers.WithLabelValues(partitionLabel).Set(float64(twStats.ActiveTimers))
			}

		case <-s.workerDone:
			return
		}
	}
}

func (s *Scheduler) checkpointLoop() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.checkpoint()
		case <-s.workerDone:
			return
		}
	}
}

// checkpoint saves scheduler state
func (s *Scheduler) checkpoint() {
	now := time.Now().UnixMilli()

	s.mu.RLock()
	lastCheckpointTS := s.lastCheckpointTS
	partitionID := s.partitionID
	readyEvents := int64(len(s.readyQueue))
	twStats := s.timingWheel.GetStats()
	s.mu.RUnlock()

	if now-lastCheckpointTS < 10000 {
		return
	}

	checkpoint := &SchedulerCheckpoint{
		PartitionID:      partitionID,
		CurrentTick:      twStats.CurrentTick,
		ActiveTimers:     twStats.ActiveTimers,
		ReadyEvents:      readyEvents,
		NextTickMs:       int64(twStats.TickMs),
		WheelSize:        int64(twStats.WheelSize),
		LastCheckpointTS: now,
	}

	// Write to file
	checkpointPath := filepath.Join(s.dataDir, "timer_state.json")
	data, err := json.Marshal(checkpoint)
	if err != nil {
		return
	}

	tmpPath := checkpointPath + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return
	}

	if err := os.Rename(tmpPath, checkpointPath); err != nil {
		return
	}

	s.mu.Lock()
	if now > s.lastCheckpointTS {
		s.lastCheckpointTS = now
	}
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
	if !s.active {
		s.mu.Unlock()
		return
	}

	s.active = false
	close(s.workerDone)
	s.mu.Unlock()

	// Final checkpoint after loops have stopped.
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

	nextTick := s.timingWheel.GetCurrentTick() + 1
	nextTime := time.UnixMilli(nextTick * int64(s.timingWheel.tickMs))
	return nextTime
}

// SchedulerCheckpoint represents scheduler checkpoint state
type SchedulerCheckpoint struct {
	PartitionID      int32 `json:"partition_id"`
	CurrentTick      int64 `json:"current_tick"`
	ActiveTimers     int64 `json:"active_timers"`
	ReadyEvents      int64 `json:"ready_events"`
	NextTickMs       int64 `json:"next_tick_ms"`
	WheelSize        int64 `json:"wheel_size"`
	LastCheckpointTS int64 `json:"last_checkpoint_ts"`
}

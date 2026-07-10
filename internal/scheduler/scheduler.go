package scheduler

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
	"github.com/jatin711-debug/cronos_db_golang/pkg/utils"
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
	schedulerColdStoreEntries = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "cronos_scheduler_cold_store_entries",
			Help: "Number of events in the cold store (far-future timers)",
		},
		[]string{"partition"},
	)
	schedulerHydratedEvents = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cronos_scheduler_hydrated_events_total",
			Help: "Total events moved from cold store to timing wheel",
		},
		[]string{"partition"},
	)
	schedulerHydratorInterval = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "cronos_scheduler_hydrator_interval_ms",
			Help: "Current adaptive hydrator scan interval in milliseconds",
		},
		[]string{"partition"},
	)
	schedulerHydratorScanDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "cronos_scheduler_hydrator_scan_duration_seconds",
			Help:    "Time spent scanning and hydrating from cold store",
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 12), // 1ms to ~4s
		},
		[]string{"partition"},
	)
)

// EventReader reads a single event by offset from the WAL.
// Implemented by *storage.WAL to avoid a direct dependency.
type EventReader interface {
	ReadEvent(offset int64) (*types.Event, error)
}

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

	// Two-tier scheduling: cold store for far-future events
	coldStore    *ColdStore
	eventReader  EventReader
	hotWindowMs  int64

	// Adaptive hydrator state
	hydratorMinInterval time.Duration
	hydratorMaxInterval time.Duration
	hydratorInterval    time.Duration
	lastColdStoreCount  int64
}

// NewScheduler creates a new scheduler.
// If hotWindowMinutes > 0 and eventReader != nil, far-future events are stored in the cold store.
func NewScheduler(dataDir string, partitionID int32, tickMs int32, wheelSize int32, hotWindowMinutes int, eventReader EventReader, cache *pebble.Cache) (*Scheduler, error) {
	// Create data directory
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("create scheduler data dir: %w", err)
	}

	startTime := time.Now().UnixMilli()

	var coldStore *ColdStore
	if hotWindowMinutes > 0 {
		var err error
		coldStore, err = NewColdStore(dataDir, cache)
		if err != nil {
			return nil, fmt.Errorf("create cold store: %w", err)
		}
	}

	scheduler := &Scheduler{
		timingWheel:         NewTimingWheel(tickMs, wheelSize, 10, 0, startTime),
		readyQueue:          make([]*types.Event, 0),
		readySignal:         make(chan struct{}, 1),
		partitionID:         partitionID,
		dataDir:             dataDir,
		active:              false,
		workerDone:          make(chan struct{}),
		stats:               &SchedulerStats{},
		lastCheckpointTS:    time.Now().UnixMilli(),
		startTimeMs:         startTime,
		coldStore:           coldStore,
		eventReader:         eventReader,
		hotWindowMs:         int64(hotWindowMinutes) * 60 * 1000,
		hydratorMinInterval: 5 * time.Second,
		hydratorMaxInterval: 5 * time.Minute,
		hydratorInterval:    60 * time.Second,
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

	// Two-tier routing: far-future events go to cold store
	if s.coldStore != nil && event.GetScheduleTs() > now+s.hotWindowMs {
		return s.coldStore.Store(event.GetOffset(), event.GetScheduleTs())
	}

	// Hot path: add to timing wheel
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
	var coldEntries []struct {
		Offset     int64
		ScheduleTS int64
	}

	for _, event := range events {
		// If event is already expired, add to ready queue
		if event.GetScheduleTs() <= now {
			readyEvents = append(readyEvents, event)
			continue
		}

		// Two-tier routing
		if s.coldStore != nil && event.GetScheduleTs() > now+s.hotWindowMs {
			coldEntries = append(coldEntries, struct {
				Offset     int64
				ScheduleTS int64
			}{Offset: event.GetOffset(), ScheduleTS: event.GetScheduleTs()})
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

	if len(coldEntries) > 0 && s.coldStore != nil {
		if err := s.coldStore.StoreBatch(coldEntries); err != nil {
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
			// Return the slice to the pool after processing
			expiredSlicePool.Put(expiredTimers)
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

// SetHydratorIntervals configures the adaptive hydrator bounds.
// A min/max of 0 means "use defaults" (5s / 5m).
func (s *Scheduler) SetHydratorIntervals(minMs, maxMs int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if minMs > 0 {
		s.hydratorMinInterval = time.Duration(minMs) * time.Millisecond
	}
	if maxMs > 0 {
		s.hydratorMaxInterval = time.Duration(maxMs) * time.Millisecond
	}
	// Ensure current interval is within bounds
	if s.hydratorInterval < s.hydratorMinInterval {
		s.hydratorInterval = s.hydratorMinInterval
	}
	if s.hydratorInterval > s.hydratorMaxInterval {
		s.hydratorInterval = s.hydratorMaxInterval
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
	utils.GoSafe("scheduler-worker", s.worker)
	utils.GoSafe("scheduler-checkpoint", s.checkpointLoop)
	utils.GoSafe("scheduler-hydrator", s.hydratorLoop)
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

// hydratorLoop adaptively scans the cold store and hydrates near-future events.
// The scan interval adjusts based on hydration volume: faster when busy, slower when idle.
func (s *Scheduler) hydratorLoop() {
	if s.coldStore == nil || s.eventReader == nil {
		return
	}

	partitionLabel := fmt.Sprintf("%d", s.partitionID)
	timer := time.NewTimer(s.hydratorInterval)
	defer timer.Stop()

	for {
		select {
		case <-timer.C:
			scanStart := time.Now()
			hydrated := s.hydrate()
			scanDuration := time.Since(scanStart)

			// Update metrics
			schedulerColdStoreEntries.WithLabelValues(partitionLabel).Set(float64(s.coldStore.Count()))
			schedulerHydratorScanDuration.WithLabelValues(partitionLabel).Observe(scanDuration.Seconds())

			// Adaptive interval adjustment
			s.adaptHydratorInterval(hydrated, scanDuration)
			schedulerHydratorInterval.WithLabelValues(partitionLabel).Set(float64(s.hydratorInterval.Milliseconds()))

			timer.Reset(s.hydratorInterval)
		case <-s.workerDone:
			return
		}
	}
}

// adaptHydratorInterval adjusts the next scan interval based on workload.
//   - High hydration volume → shorter interval (catch up faster)
//   - Zero hydration → longer interval (save I/O)
//   - Cold store growing → shorter interval (prevent backlog)
//   - Scan taking too long → shorter interval (don't fall behind)
func (s *Scheduler) adaptHydratorInterval(hydrated int, scanDuration time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	interval := s.hydratorInterval

	switch {
	case hydrated > 10000:
		interval = time.Duration(float64(interval) * 0.4) // much faster
	case hydrated > 5000:
		interval = time.Duration(float64(interval) * 0.5)
	case hydrated > 1000:
		interval = time.Duration(float64(interval) * 0.6)
	case hydrated > 100:
		interval = time.Duration(float64(interval) * 0.8)
	case hydrated == 0:
		interval = time.Duration(float64(interval) * 1.5) // slow down
	default:
		interval = time.Duration(float64(interval) * 1.1) // slight slow down
	}

	// If scan took > 50% of the interval, we need more headroom
	if scanDuration > interval/2 {
		interval = time.Duration(float64(interval) * 0.7)
	}

	// If cold store is growing, speed up
	if s.coldStore != nil {
		currentCount := s.coldStore.Count()
		if currentCount > s.lastColdStoreCount+1000 {
			interval = time.Duration(float64(interval) * 0.5)
		}
		s.lastColdStoreCount = currentCount
	}

	// Clamp to bounds
	if interval < s.hydratorMinInterval {
		interval = s.hydratorMinInterval
	}
	if interval > s.hydratorMaxInterval {
		interval = s.hydratorMaxInterval
	}

	s.hydratorInterval = interval
}

// hydrate scans cold store for events entering the hot window and loads them into the timing wheel.
// It returns the number of events successfully hydrated.
func (s *Scheduler) hydrate() int {
	now := time.Now().UnixMilli()
	startTS := now + s.hotWindowMs
	// Adaptive lookahead: use the current hydrator interval as the lookahead window
	// so we naturally scan further ahead when running less frequently.
	s.mu.RLock()
	lookaheadMs := s.hydratorInterval.Milliseconds()
	if lookaheadMs < 10000 {
		lookaheadMs = 10000 // minimum 10s lookahead
	}
	s.mu.RUnlock()
	endTS := startTS + lookaheadMs

	offsets, err := s.coldStore.ScanRange(startTS, endTS)
	if err != nil {
		log.Printf("[SCHEDULER] Hydrator scan error (partition=%d): %v", s.partitionID, err)
		return 0
	}
	if len(offsets) == 0 {
		return 0
	}

	partitionLabel := fmt.Sprintf("%d", s.partitionID)
	var hydrated int
	var toDelete []struct {
		Offset     int64
		ScheduleTS int64
	}

	for _, offset := range offsets {
		event, err := s.eventReader.ReadEvent(offset)
		if err != nil {
			log.Printf("[SCHEDULER] Hydrator read error at offset %d: %v", offset, err)
			continue
		}
		if event == nil {
			continue
		}

		now := time.Now().UnixMilli()
		if event.GetScheduleTs() <= now {
			// Already expired — send directly to ready queue
			s.mu.Lock()
			s.readyQueue = append(s.readyQueue, event)
			s.mu.Unlock()
			s.notifyReady()
		} else {
			// Add to timing wheel
			timer := s.timingWheel.GetTimerFast(event.GetOffset(), event, now)
			if err := s.timingWheel.AddTimer(timer); err != nil {
				s.timingWheel.PutTimer(timer)
				log.Printf("[SCHEDULER] Hydrator add timer error: %v", err)
				continue
			}
		}

		toDelete = append(toDelete, struct {
			Offset     int64
			ScheduleTS int64
		}{Offset: offset, ScheduleTS: event.GetScheduleTs()})
		hydrated++
	}

	if len(toDelete) > 0 {
		if err := s.coldStore.DeleteBatch(toDelete); err != nil {
			log.Printf("[SCHEDULER] Hydrator delete error: %v", err)
		}
	}
	if hydrated > 0 {
		schedulerHydratedEvents.WithLabelValues(partitionLabel).Add(float64(hydrated))
		log.Printf("[SCHEDULER] Hydrated %d events from cold store to timing wheel (partition=%d)", hydrated, s.partitionID)
	}
	return hydrated
}

// GetReadyQueueDepth returns the number of events in the ready queue.
func (s *Scheduler) GetReadyQueueDepth() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return int64(len(s.readyQueue))
}

// GetTimingWheelDepth returns the number of active timers in the timing wheel.
func (s *Scheduler) GetTimingWheelDepth() int64 {
	return s.timingWheel.GetStats().ActiveTimers
}

// GetColdStoreCount returns the number of events in the cold store.
func (s *Scheduler) GetColdStoreCount() int64 {
	if s.coldStore == nil {
		return 0
	}
	return s.coldStore.Count()
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

	// Close cold store if present
	if s.coldStore != nil {
		if err := s.coldStore.Close(); err != nil {
			log.Printf("[SCHEDULER] Cold store close error: %v", err)
		}
	}
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

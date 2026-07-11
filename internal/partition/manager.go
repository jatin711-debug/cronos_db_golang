package partition

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/internal/config"
	"github.com/jatin711-debug/cronos_db_golang/internal/consumer"
	"github.com/jatin711-debug/cronos_db_golang/internal/dedup"
	"github.com/jatin711-debug/cronos_db_golang/internal/delivery"
	"github.com/jatin711-debug/cronos_db_golang/internal/replication"
	"github.com/jatin711-debug/cronos_db_golang/internal/scheduler"
	"github.com/jatin711-debug/cronos_db_golang/internal/storage"
	"github.com/jatin711-debug/cronos_db_golang/internal/tenant"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
	"github.com/jatin711-debug/cronos_db_golang/pkg/utils"

	log2 "log/slog"

	"github.com/cockroachdb/pebble"
)

// Partition represents a data partition
type Partition struct {
	ID               int32
	Topic            string
	DataDir          string
	Wal              *storage.WAL
	Scheduler        *scheduler.Scheduler
	ConsumerGroup    *consumer.GroupManager
	DedupStore       *dedup.Manager
	Dispatcher       *delivery.Dispatcher
	Worker           *delivery.Worker
	Follower         *replication.Follower // For receiving replicated data
	ReplLeader       *replication.Leader   // For sending replication to followers
	Leader           bool
	Epoch            int64  // Cluster-assigned epoch for split-brain fencing
	MinKey           string // Minimum key boundary (inclusive)
	MaxKey           string // Maximum key boundary (exclusive)
	CreatedTS        time.Time
	UpdatedTS        time.Time
	deliveryQuit     chan struct{} // Quit channel for delivery goroutine
	deliveryQuitOnce sync.Once
	replayErr        atomic.Pointer[error]
}

// GetReplayError returns the last WAL replay error for this partition, if any.
func (p *Partition) GetReplayError() error {
	if err := p.replayErr.Load(); err != nil {
		return *err
	}
	return nil
}

// setReplayError stores the last WAL replay error.
func (p *Partition) setReplayError(err error) {
	p.replayErr.Store(&err)
}

// toTypesPartition converts an internal Partition to the public API representation,
// populating NextOffset and HighWatermark from the WAL when available.
// The caller must hold pm.mu (at least read-locked) while partition is valid.
func (pm *PartitionManager) toTypesPartition(partition *Partition) *types.Partition {
	if partition == nil {
		return nil
	}
	tp := &types.Partition{
		ID:        partition.ID,
		Topic:     partition.Topic,
		Active:    true,
		CreatedTS: partition.CreatedTS.UnixMilli(),
		UpdatedTS: partition.UpdatedTS.UnixMilli(),
	}
	if partition.Wal != nil {
		tp.NextOffset = partition.Wal.GetNextOffset()
		tp.HighWatermark = partition.Wal.GetHighWatermark()
	}
	return tp
}

// IsKeyInBounds returns true if key falls within the partition bounds [MinKey, MaxKey).
// If boundaries are empty, all keys are considered in bounds.
func (p *Partition) IsKeyInBounds(key string) bool {
	if p.MinKey != "" && key < p.MinKey {
		return false
	}
	if p.MaxKey != "" && key >= p.MaxKey {
		return false
	}
	return true
}

// PartitionManager manages all partitions
type PartitionManager struct {
	mu               sync.RWMutex
	partitions       map[int32]*Partition
	nodeID           string
	config           *types.Config
	pebbleCache      *pebble.Cache
	tenantAccountant tenantAccountant
	splitting        map[int32]bool // tracks partitions currently undergoing split
	splittingMu      sync.RWMutex
	backpressureMgr  *BackpressureManager
}

// tenantAccountant is the minimal interface needed for delivery callbacks.
type tenantAccountant interface {
	RecordDelivery(tenant tenant.ID)
}

// NewPartitionManager creates a new partition manager
func NewPartitionManager(nodeID string, config *types.Config) *PartitionManager {
	return &PartitionManager{
		partitions:      make(map[int32]*Partition),
		nodeID:          nodeID,
		config:          config,
		splitting:       make(map[int32]bool),
		backpressureMgr: NewBackpressureManager(config.MaxMemoryUsagePercent, config.MemoryCheckIntervalMs),
	}
}

// SetTenantAccountant configures the tenant accountant for delivery tracking.
// It also applies the callback to all existing partition dispatchers.
func (pm *PartitionManager) SetTenantAccountant(ta tenantAccountant) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	pm.tenantAccountant = ta
	for _, p := range pm.partitions {
		if p.Dispatcher != nil {
			p.Dispatcher.OnDeliveryComplete = func(tenantID string) {
				ta.RecordDelivery(tenant.ID(tenantID))
			}
		}
	}
}

// NewPartitionManagerWithCache creates a new partition manager with a shared PebbleDB cache.
func NewPartitionManagerWithCache(nodeID string, config *types.Config, cache *pebble.Cache) *PartitionManager {
	pm := NewPartitionManager(nodeID, config)
	pm.pebbleCache = cache
	return pm
}

// createPartitionLocked creates a new partition (assumes lock is held)
func (pm *PartitionManager) createPartitionLocked(partitionID int32, topic string) error {
	// Check if partition already exists
	if _, exists := pm.partitions[partitionID]; exists {
		return fmt.Errorf("partition %d already exists", partitionID)
	}

	// Create data directory
	dataDir := fmt.Sprintf("%s/partitions/%d", pm.config.DataDir, partitionID)

	// Create WAL
	segmentSize := pm.config.SegmentSizeBytes
	if segmentSize <= 0 {
		segmentSize = config.DefaultSegmentSizeBytes
	}
	indexInterval := pm.config.IndexInterval
	if indexInterval <= 0 {
		indexInterval = config.DefaultIndexInterval
	}
	walConfig := &storage.WALConfig{
		SegmentSizeBytes: segmentSize,
		IndexInterval:    indexInterval,
		FsyncMode:        pm.config.FsyncMode,
		FlushIntervalMS:  pm.config.FlushIntervalMS,
	}
	var cipher *storage.SegmentCipher
	if pm.config.EncryptionEnabled && pm.config.EncryptionKeyFile != "" {
		key, err := storage.LoadMasterKey(pm.config.EncryptionKeyFile)
		if err != nil {
			return fmt.Errorf("load encryption key: %w", err)
		}
		cipher, err = storage.NewSegmentCipher(key, partitionID)
		if err != nil {
			return fmt.Errorf("create cipher: %w", err)
		}
	}
	wal, err := storage.NewWAL(dataDir, partitionID, walConfig, cipher)
	if err != nil {
		return fmt.Errorf("create WAL: %w", err)
	}
	// Clean up WAL on any subsequent failure
	defer func() {
		if err != nil {
			wal.Close()
		}
	}()

	// Create scheduler with two-tier cold store support
	hotWindowMinutes := pm.config.HotWindowMinutes
	if hotWindowMinutes <= 0 {
		hotWindowMinutes = 60 // Default 1 hour if not configured
	}
	sched, err := scheduler.NewScheduler(dataDir, partitionID, int32(pm.config.TickMS), int32(pm.config.WheelSize), hotWindowMinutes, wal, pm.pebbleCache)
	if err != nil {
		return fmt.Errorf("create scheduler: %w", err)
	}
	// Configure adaptive hydrator intervals if specified
	if pm.config.HydratorMinIntervalMs > 0 || pm.config.HydratorMaxIntervalMs > 0 {
		sched.SetHydratorIntervals(pm.config.HydratorMinIntervalMs, pm.config.HydratorMaxIntervalMs)
	}

	// Create dedup store with bloom filter for high performance
	// Expected items per partition with 1% false positive rate
	// Uses ~10MB memory per partition for bloom filter (at 10M items)
	dedupStore, err := dedup.NewBloomPebbleStore(dataDir, partitionID, int32(pm.config.DedupTTLHours), pm.config.BloomCapacity, 0.01, pm.pebbleCache)
	if err != nil {
		return fmt.Errorf("create dedup store: %w", err)
	}
	dedupManager := dedup.NewManager(dedupStore)

	// Create offset store for persistent consumer offsets
	offsetStore, err := consumer.NewOffsetStore(dataDir, partitionID, pm.pebbleCache)
	if err != nil {
		return fmt.Errorf("create offset store: %w", err)
	}

	// Create consumer group manager with persistent offset store
	consumerGroup := consumer.NewGroupManagerWithStore(offsetStore)

	// Create dispatcher
	dispatcherConfig := delivery.DefaultConfig()
	dispatcher := delivery.NewDispatcher(dispatcherConfig)

	// Create worker
	worker := delivery.NewWorker(dispatcher, 1)

	// Wire tenant delivery callback if configured
	if pm.tenantAccountant != nil {
		dispatcher.OnDeliveryComplete = func(tenantID string) {
			pm.tenantAccountant.RecordDelivery(tenant.ID(tenantID))
		}
	}

	// Create partition
	partition := &Partition{
		ID:            partitionID,
		Topic:         topic,
		DataDir:       dataDir,
		Wal:           wal,
		Scheduler:     sched,
		ConsumerGroup: consumerGroup,
		DedupStore:    dedupManager,
		Dispatcher:    dispatcher,
		Worker:        worker,
		Leader:        false,
		CreatedTS:     time.Now(),
		UpdatedTS:     time.Now(),
		deliveryQuit:  make(chan struct{}),
	}

	pm.partitions[partitionID] = partition

	// Set up rate limiter for this partition if configured
	if pm.backpressureMgr != nil && pm.config.MaxIngestRatePerPartition > 0 && pm.config.IngestRateBurstSize > 0 {
		pm.backpressureMgr.SetRateLimiter(partitionID, pm.config.MaxIngestRatePerPartition, pm.config.IngestRateBurstSize)
	}

	return nil
}

// CreatePartition creates a new partition (public method with lock management)
func (pm *PartitionManager) CreatePartition(partitionID int32, topic string) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	return pm.createPartitionLocked(partitionID, topic)
}

// GetPartition gets a partition by ID
func (pm *PartitionManager) GetPartition(partitionID int32) (*types.Partition, error) {
	partition, err := pm.GetInternalPartition(partitionID)
	if err != nil {
		return nil, err
	}

	return pm.toTypesPartition(partition), nil
}

// GetInternalPartition gets the internal partition object
func (pm *PartitionManager) GetInternalPartition(partitionID int32) (*Partition, error) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	partition, exists := pm.partitions[partitionID]
	if !exists {
		return nil, types.ErrPartitionNotFound
	}

	return partition, nil
}

// GetPartitionIDForTopic returns the stable partition ID for a topic without
// creating or starting any local partition state.
func (pm *PartitionManager) GetPartitionIDForTopic(topic string) int32 {
	return utils.HashToPartitionID(topic, pm.config.PartitionCount)
}

// GetPartitionIDForKey returns the stable partition ID for a key without
// creating or starting any local partition state.
func (pm *PartitionManager) GetPartitionIDForKey(key string) int32 {
	return utils.HashToPartitionID(key, pm.config.PartitionCount)
}

// GetPartitionForTopic gets partition for a topic using consistent hashing.
// It computes the partition ID from the topic hash (same algorithm as the
// cluster router) so that every node derives the same partition ID for a
// given topic. If the partition does not exist locally, it is auto-created.
func (pm *PartitionManager) GetPartitionForTopic(topic string) (*types.Partition, error) {
	partitionID := pm.GetPartitionIDForTopic(topic)

	// Fast path: read lock for existing partition lookups.
	pm.mu.RLock()
	if partition, exists := pm.partitions[partitionID]; exists {
		pm.mu.RUnlock()
		return pm.toTypesPartition(partition), nil
	}
	pm.mu.RUnlock()

	// Slow path: partition missing, escalate to write lock.
	pm.mu.Lock()
	defer pm.mu.Unlock()

	// Check if the computed partition already exists locally
	if partition, exists := pm.partitions[partitionID]; exists {
		return pm.toTypesPartition(partition), nil
	}

	// Auto-create the partition with the CORRECT hash-derived ID
	if err := pm.createPartitionLocked(partitionID, topic); err != nil {
		return nil, fmt.Errorf("auto-create partition: %w", err)
	}

	// Start the newly created partition synchronously (before returning)
	partition := pm.partitions[partitionID]
	if err := pm.startPartitionInternal(partition); err != nil {
		log.Printf("Failed to start auto-created partition %d: %v", partitionID, err)
		// Continue anyway - partition is created but may not deliver
	}

	return &types.Partition{
		ID:            partition.ID,
		Topic:         partition.Topic,
		NextOffset:    0,
		HighWatermark: 0,
		Active:        true,
		CreatedTS:     partition.CreatedTS.UnixMilli(),
		UpdatedTS:     partition.UpdatedTS.UnixMilli(),
	}, nil
}

// GetPartitionForKey gets partition for a key using hash-based distribution.
// It uses the same SHA-256 hash algorithm as the cluster router so that all
// nodes agree on which partition owns a given key. The partition ID is
// derived from key hash modulo PartitionCount (not len(pm.partitions)),
// ensuring stable routing regardless of how many partitions are currently
// created on this node.
func (pm *PartitionManager) GetPartitionForKey(key string) (*types.Partition, error) {
	partitionID := pm.GetPartitionIDForKey(key)

	// Fast path: read lock for existing partition lookups.
	pm.mu.RLock()
	if partition, exists := pm.partitions[partitionID]; exists {
		pm.mu.RUnlock()
		return pm.toTypesPartition(partition), nil
	}
	pm.mu.RUnlock()

	// Slow path: partition missing, escalate to write lock.
	pm.mu.Lock()
	defer pm.mu.Unlock()

	// Check if the computed partition already exists locally
	if partition, exists := pm.partitions[partitionID]; exists {
		return pm.toTypesPartition(partition), nil
	}

	// Auto-create the partition with the CORRECT hash-derived ID
	if err := pm.createPartitionLocked(partitionID, key); err != nil {
		return nil, fmt.Errorf("auto-create partition: %w", err)
	}

	// Start the newly created partition synchronously (before returning)
	partition := pm.partitions[partitionID]
	if err := pm.startPartitionInternal(partition); err != nil {
		log.Printf("Failed to start auto-created partition %d: %v", partitionID, err)
		// Continue anyway - partition is created but may not deliver
	}

	return &types.Partition{
		ID:            partition.ID,
		Topic:         partition.Topic,
		NextOffset:    0,
		HighWatermark: 0,
		Active:        true,
		CreatedTS:     partition.CreatedTS.UnixMilli(),
		UpdatedTS:     partition.UpdatedTS.UnixMilli(),
	}, nil
}

// CanAccept returns true if the partition can accept new publishes without exceeding capacity limits.
func (pm *PartitionManager) CanAccept(partitionID int32) bool {
	// Check backpressure first (memory + rate limiting)
	if pm.backpressureMgr != nil && !pm.backpressureMgr.CanAccept(partitionID) {
		return false
	}

	pm.mu.RLock()
	partition, exists := pm.partitions[partitionID]
	pm.mu.RUnlock()
	if !exists {
		return true // Non-existent partition can always be created
	}

	// Check admission control limits
	if pm.config.MaxReadyQueueSize > 0 {
		depth := partition.Scheduler.GetReadyQueueDepth()
		if depth >= pm.config.MaxReadyQueueSize {
			return false
		}
		// Load shedding: reject if above threshold percentage of max
		if pm.config.LoadSheddingThreshold > 0 {
			threshold := int64(float64(pm.config.MaxReadyQueueSize) * pm.config.LoadSheddingThreshold)
			if depth >= threshold {
				return false
			}
		}
	}
	if pm.config.MaxTimingWheelSize > 0 {
		if partition.Scheduler.GetTimingWheelDepth() >= pm.config.MaxTimingWheelSize {
			return false
		}
	}
	if pm.config.MaxInFlightPerPartition > 0 {
		if partition.Dispatcher.GetStats().ActiveDeliveries >= pm.config.MaxInFlightPerPartition {
			return false
		}
	}
	return true
}

// ExactlyOnceCommitsEnabled reports whether strict monotonic consumer commits are enabled.
func (pm *PartitionManager) ExactlyOnceCommitsEnabled() bool {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	return pm.config != nil && pm.config.ExactlyOnceCommits
}

// FollowerReadsEnabled reports whether follower nodes may serve replay reads.
func (pm *PartitionManager) FollowerReadsEnabled() bool {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	return pm.config != nil && pm.config.FollowerReadsEnabled
}

// GetOrCreateInternalPartition gets or auto-creates and starts an internal partition by ID.
// If the partition does not exist locally, it is created with the given topic and its
// background workers (scheduler, delivery, compaction, dedup pruning) are started.
// This method is safe for concurrent use.
func (pm *PartitionManager) GetOrCreateInternalPartition(partitionID int32, topic string) (*Partition, error) {
	// Fast path: read lock for existing partition lookups.
	pm.mu.RLock()
	if partition, exists := pm.partitions[partitionID]; exists {
		pm.mu.RUnlock()
		return partition, nil
	}
	pm.mu.RUnlock()

	// Slow path: partition missing, escalate to write lock.
	pm.mu.Lock()
	defer pm.mu.Unlock()

	// Double-checked locking
	if partition, exists := pm.partitions[partitionID]; exists {
		return partition, nil
	}

	// Auto-create
	if err := pm.createPartitionLocked(partitionID, topic); err != nil {
		return nil, fmt.Errorf("auto-create partition: %w", err)
	}

	// Start the newly created partition synchronously
	partition := pm.partitions[partitionID]
	if err := pm.startPartitionInternal(partition); err != nil {
		log.Printf("Failed to start auto-created partition %d: %v", partitionID, err)
	}

	return partition, nil
}

// startPartitionLocked starts a partition (assumes lock is held)
func (pm *PartitionManager) startPartitionLocked(partitionID int32) error {
	partition, exists := pm.partitions[partitionID]
	if !exists {
		return fmt.Errorf("partition %d not found", partitionID)
	}

	return pm.startPartitionInternal(partition)
}

// startPartitionInternal starts a partition's background workers
func (pm *PartitionManager) startPartitionInternal(partition *Partition) error {
	// Try to load snapshot for fast recovery
	snapshotMgr := NewSnapshotManager(partition.DataDir, partition.ID)
	snapshot, err := snapshotMgr.LoadSnapshot()
	if err != nil {
		log.Printf("[Partition %d] Failed to load snapshot: %v", partition.ID, err)
	}

	if snapshot != nil {
		// Fast recovery: skip WAL replay up to snapshot point
		log.Printf("[Partition %d] Fast recovery from snapshot: HWM=%d, scheduled=%d",
			partition.ID, snapshot.HighWatermark, snapshot.LastScheduledOffset)

		// Restore consumer offsets
		for groupID, offset := range snapshot.ConsumerOffsets {
			if partition.ConsumerGroup != nil {
				// Create or update consumer group with restored offset
				_ = partition.ConsumerGroup.CommitOffset(groupID, int64(partition.ID), offset)
			}
		}

		// Replay only from snapshot point forward
		if snapshot.LastScheduledOffset < partition.Wal.GetLastOffset() {
			pm.replayWALTimersFromOffset(partition, snapshot.LastScheduledOffset+1)
		} else {
			log.Printf("[Partition %d] No new events since snapshot, skipping replay", partition.ID)
		}
	} else {
		// Full WAL replay (slow path)
		pm.replayWALTimers(partition)
	}

	// Start scheduler
	partition.Scheduler.Start()

	// Start worker
	partition.Worker.Start()

	// Start delivery loop (event-driven): consume scheduler ready signals and
	// immediately hand over batches to the worker.
	utils.GoSafe("partition-delivery-loop", func() {
		for {
			select {
			case <-partition.Scheduler.ReadySignal():
				for {
					readyEvents := partition.Scheduler.GetReadyEvents()
					if len(readyEvents) == 0 {
						break
					}
					partition.Worker.AddReadyEvents(readyEvents)
				}
			case <-partition.deliveryQuit:
				return
			}
		}
	})

	// Start compaction loop (runs every 10 minutes)
	compactionInterval := 10 * time.Minute
	utils.GoSafe("partition-compaction-loop", func() {
		ticker := time.NewTicker(compactionInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				partition.runCompaction()
			case <-partition.deliveryQuit:
				return
			}
		}
	})

	// Start dedup pruning loop (runs every hour)
	utils.GoSafe("partition-dedup-prune-loop", func() {
		ticker := time.NewTicker(1 * time.Hour)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				if partition.DedupStore != nil {
					pruned, err := partition.DedupStore.PruneExpired()
					if err != nil {
						log.Printf("[Partition %d] Dedup prune error: %v", partition.ID, err)
					} else if pruned > 0 {
						log.Printf("[Partition %d] Pruned %d expired dedup entries", partition.ID, pruned)
					}
				}
			case <-partition.deliveryQuit:
				return
			}
		}
	})

	// Start periodic snapshot creation (every 5 minutes)
	utils.GoSafe("partition-snapshot-loop", func() {
		snapshotMgr.StartPeriodicSnapshots(partition, 5*time.Minute)
	})

	return nil
}

// replayWALTimers reads all events from the WAL and re-schedules any whose
// schedule_ts is still in the future. This recovers timers lost during a crash.
// Uses incremental checkpointing to avoid O(N) replay on each boot.
func (pm *PartitionManager) replayWALTimers(partition *Partition) {
	lastOffset := partition.Wal.GetLastOffset()
	if lastOffset < 0 {
		return // Empty WAL, nothing to replay
	}

	// Read incremental checkpoint to avoid replaying entire WAL
	checkpoint := pm.readTimerCheckpoint(partition)
	startOffset := int64(0)
	if checkpoint != nil && checkpoint.LastScheduledOffset >= 0 {
		startOffset = checkpoint.LastScheduledOffset + 1
		log.Printf("[Partition %d] Using timer checkpoint: resuming from offset %d", partition.ID, startOffset)
	}

	// If we're already at the end, nothing to replay
	if startOffset > lastOffset {
		log.Printf("[Partition %d] Timer replay complete: already up to date at offset %d", partition.ID, lastOffset)
		return
	}

	now := time.Now().UnixMilli()
	scheduledCount := 0
	expiredCount := 0
	lastScheduled := startOffset - 1

	const replayBatchSize int64 = 10000
	for batchStart := startOffset; batchStart <= lastOffset; batchStart += replayBatchSize {
		batchEnd := min(batchStart+replayBatchSize-1, lastOffset)

		events, err := partition.Wal.ReadEvents(batchStart, batchEnd)
		if err != nil {
			err = fmt.Errorf("WAL replay failed at offsets %d-%d: %w", batchStart, batchEnd, err)
			partition.setReplayError(err)
			log2.Warn("WAL replay failed", "partition", partition.ID, "start_offset", batchStart, "end_offset", batchEnd, "error", err)
			return
		}

		for _, event := range events {
			if event.GetScheduleTs() > now {
				// Future event — re-schedule it
				if err := partition.Scheduler.Schedule(event); err != nil {
					log2.Warn("WAL replay scheduler error", "partition", partition.ID, "offset", event.Offset, "error", err)
					continue
				}
				scheduledCount++
			} else {
				expiredCount++
			}
			lastScheduled = event.Offset
		}
	}

	// Update checkpoint incrementally
	pm.writeTimerCheckpoint(partition, lastScheduled)

	log.Printf("[Partition %d] WAL replay complete: %d future events re-scheduled, %d already expired (offsets %d-%d)",
		partition.ID, scheduledCount, expiredCount, startOffset, lastScheduled)
}

// replayWALTimersFromOffset replays WAL events starting from a specific offset
// (used for snapshot recovery to avoid full WAL replay).
func (pm *PartitionManager) replayWALTimersFromOffset(partition *Partition, startOffset int64) {
	lastOffset := partition.Wal.GetLastOffset()
	if lastOffset < 0 {
		return // Empty WAL, nothing to replay
	}

	if startOffset > lastOffset {
		log.Printf("[Partition %d] Timer replay from offset %d: already up to date at offset %d", partition.ID, startOffset, lastOffset)
		return
	}

	now := time.Now().UnixMilli()
	scheduledCount := 0
	expiredCount := 0
	lastScheduled := startOffset - 1

	const replayBatchSize int64 = 10000
	for batchStart := startOffset; batchStart <= lastOffset; batchStart += replayBatchSize {
		batchEnd := min(batchStart+replayBatchSize-1, lastOffset)

		events, err := partition.Wal.ReadEvents(batchStart, batchEnd)
		if err != nil {
			err = fmt.Errorf("WAL replay from offset %d failed at offsets %d-%d: %w", startOffset, batchStart, batchEnd, err)
			partition.setReplayError(err)
			log2.Warn("WAL replay from offset failed", "partition", partition.ID, "start_offset", batchStart, "end_offset", batchEnd, "error", err)
			return
		}

		for _, event := range events {
			if event.GetScheduleTs() > now {
				// Future event — re-schedule it
				if err := partition.Scheduler.Schedule(event); err != nil {
					log2.Warn("WAL replay scheduler error", "partition", partition.ID, "offset", event.Offset, "error", err)
					continue
				}
				scheduledCount++
			} else {
				expiredCount++
			}
			lastScheduled = event.Offset
		}
	}

	// Update checkpoint incrementally
	pm.writeTimerCheckpoint(partition, lastScheduled)

	log.Printf("[Partition %d] WAL replay from offset %d complete: %d future events re-scheduled, %d already expired (offsets %d-%d)",
		partition.ID, startOffset, scheduledCount, expiredCount, startOffset, lastScheduled)
}

// TimerCheckpoint stores the incremental replay progress
type TimerCheckpoint struct {
	LastScheduledOffset int64 `json:"last_scheduled_offset"`
	LastCheckpointTime  int64 `json:"last_checkpoint_time"`
}

// readTimerCheckpoint reads the timer replay checkpoint
func (pm *PartitionManager) readTimerCheckpoint(partition *Partition) *TimerCheckpoint {
	cpPath := fmt.Sprintf("%s/timer_replay_checkpoint.json", partition.DataDir)
	data, err := os.ReadFile(cpPath)
	if err != nil {
		return nil
	}
	var cp TimerCheckpoint
	if err := json.Unmarshal(data, &cp); err != nil {
		return nil
	}
	return &cp
}

// writeTimerCheckpoint writes the timer replay checkpoint
func (pm *PartitionManager) writeTimerCheckpoint(partition *Partition, lastOffset int64) {
	cp := TimerCheckpoint{
		LastScheduledOffset: lastOffset,
		LastCheckpointTime:  time.Now().UnixMilli(),
	}
	data, err := json.Marshal(cp)
	if err != nil {
		log2.Warn("Failed to marshal timer checkpoint", "error", err)
		return
	}
	cpPath := fmt.Sprintf("%s/timer_replay_checkpoint.json", partition.DataDir)
	tmpPath := cpPath + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		log2.Warn("Failed to write timer checkpoint", "error", err)
		return
	}
	os.Rename(tmpPath, cpPath)
}

// runCompaction calculates the minimum consumed offset across all consumer groups
// and safely removes obsolete WAL segments.
// RunCompaction triggers compaction on this partition (exported for external callers).
func (p *Partition) RunCompaction() {
	p.runCompaction()
}

func (p *Partition) runCompaction() {
	groups := p.ConsumerGroup.ListGroups()

	hasActiveConsumers := false
	minConsumedOffset := p.Wal.GetHighWatermark()

	for _, group := range groups {
		hasPartition := false
		for _, partID := range group.Partitions {
			if partID == p.ID {
				hasPartition = true
				break
			}
		}

		if !hasPartition {
			continue
		}

		hasActiveConsumers = true

		offset, ok := group.CommittedOffsets[p.ID]
		if ok {
			if offset == -1 {
				// Consumer hasn't consumed anything, can't discard data
				minConsumedOffset = 0
			} else if offset < minConsumedOffset {
				minConsumedOffset = offset
			}
		} else {
			// Partition is assigned but no offset committed yet
			minConsumedOffset = 0
		}
	}

	if !hasActiveConsumers {
		return // No active consumers to bound the min offset
	}

	if minConsumedOffset > 0 {
		deleted, err := p.Wal.CompactByOffset(minConsumedOffset)
		if err != nil {
			log.Printf("[Partition %d] WAL compaction error: %v", p.ID, err)
		} else if deleted > 0 {
			log.Printf("[Partition %d] Compacted %d WAL segments up to offset %d", p.ID, deleted, minConsumedOffset)
		}
	}
}

// ListPartitions lists all partitions
func (pm *PartitionManager) ListPartitions() []*Partition {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	partitions := make([]*Partition, 0, len(pm.partitions))
	for _, partition := range pm.partitions {
		partitions = append(partitions, partition)
	}

	return partitions
}

// StartPartition starts a partition
func (pm *PartitionManager) StartPartition(partitionID int32) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	return pm.startPartitionLocked(partitionID)
}

// StopPartition stops a partition
func (pm *PartitionManager) StopPartition(partitionID int32) error {
	partition, err := pm.GetInternalPartition(partitionID)
	if err != nil {
		return err
	}

	// Signal delivery goroutines to stop FIRST to avoid circular lock deadlock
	// Delivery goroutines read from deliveryQuit channel - closing it allows them to exit
	partition.deliveryQuitOnce.Do(func() { close(partition.deliveryQuit) })

	// Stop scheduler: no new events will be added to the ready queue
	if partition.Scheduler != nil {
		partition.Scheduler.Stop()
	}

	// Stop delivery worker: let it finish processing its remaining queue
	if partition.Worker != nil {
		partition.Worker.Stop()
	}

	// Drain in-flight deliveries: wait for active deliveries to ack or timeout
	if partition.Dispatcher != nil {
		if err := partition.Dispatcher.Drain(30 * time.Second); err != nil {
			log.Printf("[Partition %d] Drain incomplete: %v", partition.ID, err)
		}
		partition.Dispatcher.Close()
	}

	// Close durable stores before the WAL so background goroutines stop
	// touching the data directory. Otherwise snapshot/dedup/offset workers can
	// still be writing when the test's TempDir cleanup runs and we leak files
	// ("directory not empty").
	if partition.DedupStore != nil {
		if err := partition.DedupStore.Close(); err != nil {
			log.Printf("[Partition %d] Dedup store close failed: %v", partition.ID, err)
		}
	}
	if partition.ConsumerGroup != nil {
		if err := partition.ConsumerGroup.Close(); err != nil {
			log.Printf("[Partition %d] Consumer group close failed: %v", partition.ID, err)
		}
	}

	// Flush and close WAL
	if partition.Wal != nil {
		if err := partition.Wal.Close(); err != nil {
			return fmt.Errorf("close WAL: %w", err)
		}
	}

	return nil
}

// StopAllPartitions stops all active partitions concurrently and gracefully
func (pm *PartitionManager) StopAllPartitions() error {
	partitions := pm.ListPartitions()

	var wg sync.WaitGroup
	errCh := make(chan error, len(partitions))

	for _, p := range partitions {
		wg.Add(1)
		go func(partitionID int32) {
			defer wg.Done()
			if err := pm.StopPartition(partitionID); err != nil {
				errCh <- fmt.Errorf("partition %d: %w", partitionID, err)
			}
		}(p.ID)
	}

	wg.Wait()
	close(errCh)

	var errs []error
	for err := range errCh {
		errs = append(errs, err)
	}

	if len(errs) > 0 {
		return fmt.Errorf("failed to stop %d partitions: %v", len(errs), errs[0])
	}
	return nil
}

// GetStats returns partition manager statistics
func (pm *PartitionManager) GetStats() *PartitionManagerStats {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	return &PartitionManagerStats{
		TotalPartitions:  int64(len(pm.partitions)),
		LeaderPartitions: pm.countLeaderPartitions(),
		ActivePartitions: int64(len(pm.partitions)), // Simplified
	}
}

// countLeaderPartitions counts leader partitions
func (pm *PartitionManager) countLeaderPartitions() int64 {
	var count int64
	for _, partition := range pm.partitions {
		if partition.Leader {
			count++
		}
	}
	return count
}

// PartitionManagerStats represents partition manager statistics
type PartitionManagerStats struct {
	TotalPartitions  int64
	LeaderPartitions int64
	ActivePartitions int64
}

// GetOrCreatePartition gets an existing partition or creates a new one for sync
func (pm *PartitionManager) GetOrCreatePartition(partitionID int32) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if _, exists := pm.partitions[partitionID]; exists {
		return nil
	}

	// Auto-create partition for sync (topic will be set later)
	return pm.createPartitionLocked(partitionID, fmt.Sprintf("partition-%d", partitionID))
}

// replicationTLSConfig builds the internal replication mTLS config from the
// global node configuration. It returns nil when replication TLS is disabled.
func (pm *PartitionManager) replicationTLSConfig() *replication.MTLSConfig {
	if pm.config == nil {
		return nil
	}
	return &replication.MTLSConfig{
		Enabled:  pm.config.ReplicationTLSEnabled,
		CAFile:   pm.config.ReplicationTLSCAFile,
		CertFile: pm.config.ReplicationTLSCertFile,
		KeyFile:  pm.config.ReplicationTLSKeyFile,
	}
}

// SyncPartitionFromLeader syncs a partition from its leader via bulk transfer
func (pm *PartitionManager) SyncPartitionFromLeader(partitionID int32, leaderAddr string) error {
	pm.mu.RLock()
	partition, exists := pm.partitions[partitionID]
	pm.mu.RUnlock()

	if !exists {
		return fmt.Errorf("partition %d not found", partitionID)
	}

	// Get or create follower
	pm.mu.Lock()
	if partition.Follower == nil {
		partition.Follower = replication.NewFollower(partitionID, partition.Wal, pm.nodeID, pm.replicationTLSConfig())
	}
	pm.mu.Unlock()

	// Set leader info
	partition.Follower.SetLeader(fmt.Sprintf("leader-%d", partitionID), leaderAddr, 0)

	// Perform bulk file sync
	if err := partition.Follower.SyncFilesFromLeader(); err != nil {
		return fmt.Errorf("bulk sync from leader %s: %w", leaderAddr, err)
	}

	log.Printf("[PARTITION] Partition %d synced from leader %s", partitionID, leaderAddr)
	return nil
}

// PromoteToLeader promotes a local partition to leader and starts replication.
func (pm *PartitionManager) PromoteToLeader(partitionID int32, epoch int64) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	partition, exists := pm.partitions[partitionID]
	if !exists {
		return fmt.Errorf("partition %d not found", partitionID)
	}

	if partition.ReplLeader != nil {
		partition.Leader = true
		partition.Epoch = epoch
		return nil // Already leader, just update epoch
	}

	leader := replication.NewLeader(partitionID, int32(pm.config.ReplicationBatchSize), pm.config.ReplicationTimeout, partition.Wal, pm.config.MinInSyncReplicas, pm.nodeID, pm.replicationTLSConfig())
	leader.Start()
	partition.ReplLeader = leader
	partition.Leader = true
	partition.Epoch = epoch

	log.Printf("[PARTITION] Partition %d promoted to leader (epoch=%d)", partitionID, epoch)
	return nil
}

// AddFollower adds a follower to a local leader partition.
func (pm *PartitionManager) AddFollower(partitionID int32, followerID string, followerAddr string) error {
	pm.mu.RLock()
	partition, exists := pm.partitions[partitionID]
	pm.mu.RUnlock()

	if !exists {
		return fmt.Errorf("partition %d not found", partitionID)
	}

	if partition.ReplLeader == nil {
		return fmt.Errorf("partition %d is not a leader", partitionID)
	}

	if err := partition.ReplLeader.AddFollower(followerID, followerAddr); err != nil {
		return fmt.Errorf("add follower %s: %w", followerID, err)
	}

	log.Printf("[PARTITION] Follower %s added to partition %d", followerID, partitionID)
	return nil
}

// DemoteFromLeader demotes a local partition from leader.
func (pm *PartitionManager) DemoteFromLeader(partitionID int32) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	partition, exists := pm.partitions[partitionID]
	if !exists {
		return fmt.Errorf("partition %d not found", partitionID)
	}

	if partition.ReplLeader != nil {
		partition.ReplLeader.Stop()
		partition.ReplLeader = nil
	}
	partition.Leader = false

	log.Printf("[PARTITION] Partition %d demoted from leader", partitionID)
	return nil
}

// GetPartitionEpoch returns the cluster epoch for a partition.
func (pm *PartitionManager) GetPartitionEpoch(partitionID int32) int64 {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	if partition, exists := pm.partitions[partitionID]; exists {
		return partition.Epoch
	}
	return 0
}

// GetPartitionReplicaOffsets returns the latest high-watermark offsets for a partition's
// replicas, including the local WAL high watermark for this node if it leads the partition.
func (pm *PartitionManager) GetPartitionReplicaOffsets(partitionID int32) map[string]int64 {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	partition, exists := pm.partitions[partitionID]
	if !exists || partition == nil {
		return nil
	}

	offsets := make(map[string]int64)
	if partition.Wal != nil {
		// Local replica offset is the WAL high watermark (last durable offset).
		offsets[pm.nodeID] = partition.Wal.GetHighWatermark()
	}
	if partition.ReplLeader != nil {
		for followerID, offset := range partition.ReplLeader.GetFollowerOffsets() {
			offsets[followerID] = offset
		}
	}
	return offsets
}

// PartitionLoadStatus exposes backpressure signals for a partition.
type PartitionLoadStatus struct {
	PartitionID        int32
	ReadyQueueDepth    int64
	TimingWheelDepth   int64
	InFlightDeliveries int64
	DLQSize            int64
	CanAccept          bool
}

// GetLoadStatus returns backpressure metrics for a partition.
func (pm *PartitionManager) GetLoadStatus(partitionID int32) (*PartitionLoadStatus, error) {
	pm.mu.RLock()
	partition, exists := pm.partitions[partitionID]
	pm.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("partition %d not found", partitionID)
	}

	status := &PartitionLoadStatus{
		PartitionID:      partitionID,
		ReadyQueueDepth:  partition.Scheduler.GetReadyQueueDepth(),
		TimingWheelDepth: partition.Scheduler.GetTimingWheelDepth(),
		CanAccept:        pm.CanAccept(partitionID),
	}

	if partition.Dispatcher != nil {
		stats := partition.Dispatcher.GetStats()
		status.InFlightDeliveries = stats.ActiveDeliveries
		status.DLQSize = stats.DLQSize
	}

	return status, nil
}

// NewPartitionManagerWithAccessor creates a new partition manager that implements PartitionAccessor
func NewPartitionManagerWithAccessor(nodeID string, config *types.Config) *PartitionManager {
	return NewPartitionManager(nodeID, config)
}

// GetDataDir returns the configured data directory
func (pm *PartitionManager) GetDataDir() string {
	if pm.config != nil {
		return pm.config.DataDir
	}
	return ""
}

// SetSplitting marks a partition as actively undergoing a split.
func (pm *PartitionManager) SetSplitting(partitionID int32, active bool) {
	pm.splittingMu.Lock()
	defer pm.splittingMu.Unlock()
	if active {
		pm.splitting[partitionID] = true
	} else {
		delete(pm.splitting, partitionID)
	}
}

// IsSplitting returns whether a partition is undergoing a split.
func (pm *PartitionManager) IsSplitting(partitionID int32) bool {
	pm.splittingMu.RLock()
	defer pm.splittingMu.RUnlock()
	return pm.splitting[partitionID]
}

// SetPartitionBounds updates a partition's key range boundaries.
func (pm *PartitionManager) SetPartitionBounds(partitionID int32, minKey, maxKey string) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	p, exists := pm.partitions[partitionID]
	if !exists {
		return fmt.Errorf("partition %d not found", partitionID)
	}
	p.MinKey = minKey
	p.MaxKey = maxKey
	p.UpdatedTS = time.Now()
	return nil
}

// GetPartitionBounds returns the boundaries for a partition.
func (pm *PartitionManager) GetPartitionBounds(partitionID int32) (string, string, error) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	p, exists := pm.partitions[partitionID]
	if !exists {
		return "", "", fmt.Errorf("partition %d not found", partitionID)
	}
	return p.MinKey, p.MaxKey, nil
}

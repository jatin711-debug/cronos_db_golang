package partition

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/internal/consumer"
	"github.com/jatin711-debug/cronos_db_golang/internal/dedup"
	"github.com/jatin711-debug/cronos_db_golang/internal/delivery"
	"github.com/jatin711-debug/cronos_db_golang/internal/replication"
	"github.com/jatin711-debug/cronos_db_golang/internal/scheduler"
	"github.com/jatin711-debug/cronos_db_golang/internal/storage"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
	"github.com/jatin711-debug/cronos_db_golang/pkg/utils"

	log2 "log/slog"

	"github.com/cockroachdb/pebble"
)

// Partition represents a data partition
type Partition struct {
	ID            int32
	Topic         string
	DataDir       string
	Wal           *storage.WAL
	Scheduler     *scheduler.Scheduler
	ConsumerGroup *consumer.GroupManager
	DedupStore    *dedup.Manager
	Dispatcher    *delivery.Dispatcher
	Worker        *delivery.Worker
	Follower      *replication.Follower // For receiving replicated data
	Leader        bool
	CreatedTS     time.Time
	UpdatedTS     time.Time
	deliveryQuit  chan struct{} // Quit channel for delivery goroutine
}

// PartitionManager manages all partitions
type PartitionManager struct {
	mu          sync.RWMutex
	partitions  map[int32]*Partition
	nodeID      string
	config      *types.Config
	pebbleCache *pebble.Cache
}

// NewPartitionManager creates a new partition manager
func NewPartitionManager(nodeID string, config *types.Config) *PartitionManager {
	return &PartitionManager{
		partitions: make(map[int32]*Partition),
		nodeID:     nodeID,
		config:     config,
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
	walConfig := &storage.WALConfig{
		SegmentSizeBytes: pm.config.SegmentSizeBytes,
		IndexInterval:    pm.config.IndexInterval,
		FsyncMode:        pm.config.FsyncMode,
		FlushIntervalMS:  pm.config.FlushIntervalMS,
	}
	wal, err := storage.NewWAL(dataDir, partitionID, walConfig)
	if err != nil {
		return fmt.Errorf("create WAL: %w", err)
	}
	// Clean up WAL on any subsequent failure
	defer func() {
		if err != nil {
			wal.Close()
		}
	}()

	// Create scheduler
	scheduler, err := scheduler.NewScheduler(dataDir, partitionID, int32(pm.config.TickMS), int32(pm.config.WheelSize))
	if err != nil {
		return fmt.Errorf("create scheduler: %w", err)
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
	worker := delivery.NewWorker(dispatcher, 100)

	// Create partition
	partition := &Partition{
		ID:            partitionID,
		Topic:         topic,
		DataDir:       dataDir,
		Wal:           wal,
		Scheduler:     scheduler,
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

	return &types.Partition{
		ID:            partition.ID,
		Topic:         partition.Topic,
		NextOffset:    0, // Would get from WAL
		HighWatermark: 0, // Would get from WAL
		Active:        true,
		CreatedTS:     partition.CreatedTS.UnixMilli(),
		UpdatedTS:     partition.UpdatedTS.UnixMilli(),
	}, nil
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
	pm.mu.RUnlock()

	// Slow path: partition missing, escalate to write lock.
	pm.mu.Lock()
	defer pm.mu.Unlock()

	// Check if the computed partition already exists locally
	if partition, exists := pm.partitions[partitionID]; exists {
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
	pm.mu.RUnlock()

	// Slow path: partition missing, escalate to write lock.
	pm.mu.Lock()
	defer pm.mu.Unlock()

	// Check if the computed partition already exists locally
	if partition, exists := pm.partitions[partitionID]; exists {
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

// GetOrCreateInternalPartition gets or auto-creates and starts an internal partition by ID
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
	// Replay WAL to recover scheduled timers that haven't fired yet
	pm.replayWALTimers(partition)

	// Start scheduler
	partition.Scheduler.Start()

	// Start worker
	partition.Worker.Start()

	// Start delivery loop (event-driven): consume scheduler ready signals and
	// immediately hand over batches to the worker.
	go func() {
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
	}()

	// Start compaction loop (runs every 10 minutes)
	compactionInterval := 10 * time.Minute
	go func() {
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
	}()

	// Start dedup pruning loop (runs every hour)
	go func() {
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
	}()

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
			log2.Warn("WAL replay failed", "partition", partition.ID, "start_offset", batchStart, "end_offset", batchEnd, "error", err)
			return
		}

		for _, event := range events {
			if event.GetScheduleTs() > now {
				// Future event — re-schedule it
				if err := partition.Scheduler.Schedule(event); err != nil {
					// Timer may already exist if recovery runs twice; skip silently
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
	select {
	case <-partition.deliveryQuit:
		// already closed
	default:
		close(partition.deliveryQuit)
	}

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
		partition.Follower = replication.NewFollower(partitionID, partition.Wal, pm.nodeID)
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

// NewPartitionManagerWithAccessor creates a new partition manager that implements PartitionAccessor
func NewPartitionManagerWithAccessor(nodeID string, config *types.Config) *PartitionManager {
	return NewPartitionManager(nodeID, config)
}

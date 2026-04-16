package partition

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"os"
	"sync"
	"time"

	"cronos_db/internal/consumer"
	"cronos_db/internal/dedup"
	"cronos_db/internal/delivery"
	"cronos_db/internal/replication"
	"cronos_db/internal/scheduler"
	"cronos_db/internal/storage"
	"cronos_db/pkg/types"

	log2 "log/slog"
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
	mu         sync.RWMutex
	partitions map[int32]*Partition
	nodeID     string
	config     *types.Config
}

// NewPartitionManager creates a new partition manager
func NewPartitionManager(nodeID string, config *types.Config) *PartitionManager {
	return &PartitionManager{
		partitions: make(map[int32]*Partition),
		nodeID:     nodeID,
		config:     config,
	}
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
	dedupStore, err := dedup.NewBloomPebbleStore(dataDir, partitionID, int32(pm.config.DedupTTLHours), pm.config.BloomCapacity, 0.01)
	if err != nil {
		return fmt.Errorf("create dedup store: %w", err)
	}
	dedupManager := dedup.NewManager(dedupStore)

	// Create offset store for persistent consumer offsets
	offsetStore, err := consumer.NewOffsetStore(dataDir, partitionID)
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

// GetPartitionForTopic gets partition for a topic using consistent hashing
func (pm *PartitionManager) GetPartitionForTopic(topic string) (*types.Partition, error) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	// Check if partition already exists for this topic
	for _, partition := range pm.partitions {
		if partition.Topic == topic {
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
	}

	// Auto-create partition for unknown topic
	partitionID := int32(len(pm.partitions))
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

// GetPartitionForKey gets partition for a key using hash-based distribution
// This provides Kafka-like key-based partitioning for even distribution
func (pm *PartitionManager) GetPartitionForKey(key string) (*types.Partition, error) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if len(pm.partitions) == 0 {
		return nil, fmt.Errorf("no partitions available")
	}

	// Hash the key to determine partition
	h := fnv.New32a()
	h.Write([]byte(key))
	hash := h.Sum32()

	// Select partition based on hash
	partitionID := int32(hash % uint32(len(pm.partitions)))

	partition, exists := pm.partitions[partitionID]
	if !exists {
		return nil, fmt.Errorf("partition %d not found", partitionID)
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

	// Get poll interval from config (default 10ms for better responsiveness)
	pollInterval := time.Duration(pm.config.DeliveryPollMS) * time.Millisecond
	if pollInterval == 0 {
		pollInterval = 10 * time.Millisecond
	}

	// Create dispatch channel for parallel processing
	dispatchCh := make(chan *types.Event, 10000)

	// Start multiple dispatch workers (parallel delivery)
	numWorkers := 4
	for i := 0; i < numWorkers; i++ {
		go func() {
			for {
				select {
				case event, ok := <-dispatchCh:
					if !ok {
						return
					}
					if err := partition.Dispatcher.Dispatch(event); err != nil {
						log.Printf("Dispatch failed: %v", err)
					}
				case <-partition.deliveryQuit:
					return
				}
			}
		}()
	}

	// Start delivery loop (poll scheduler for ready events and dispatch them)
	go func() {
		ticker := time.NewTicker(pollInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				// Get ready events from scheduler
				readyEvents := partition.Scheduler.GetReadyEvents()
				if len(readyEvents) > 0 {
					// Send to dispatch workers
					for _, event := range readyEvents {
						select {
						case dispatchCh <- event:
						default:
							// Channel full - dispatch directly
							if err := partition.Dispatcher.Dispatch(event); err != nil {
								log.Printf("Dispatch failed: %v", err)
							}
						}
					}
				}
			case <-partition.deliveryQuit:
				close(dispatchCh)
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
	events, err := partition.Wal.ReadEvents(startOffset, lastOffset)
	if err != nil {
		log2.Warn("WAL replay failed", "partition", partition.ID, "error", err)
		return
	}

	scheduledCount := 0
	expiredCount := 0
	lastScheduled := startOffset - 1

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

	// Update checkpoint incrementally
	pm.writeTimerCheckpoint(partition, lastScheduled)

	log.Printf("[Partition %d] WAL replay complete: %d future events re-scheduled, %d already expired (offsets %d-%d)",
		partition.ID, scheduledCount, expiredCount, startOffset, lastScheduled)
}

// TimerCheckpoint stores the incremental replay progress
type TimerCheckpoint struct {
	LastScheduledOffset int64 `json:"last_scheduled_offset"`
	LastCheckpointTime int64 `json:"last_checkpoint_time"`
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

	// Stop scheduler (safe now since delivery goroutines will exit)
	if partition.Scheduler != nil {
		partition.Scheduler.Stop()
	}

	// Stop delivery worker
	if partition.Worker != nil {
		partition.Worker.Stop()
	}

	// Close WAL
	if partition.Wal != nil {
		// Flush before close to ensure durability
		partition.Wal.Flush()
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

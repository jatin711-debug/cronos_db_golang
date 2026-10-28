// Package partition owns local partition lifecycle: WAL, scheduling, delivery,
// consumer groups, dedup, snapshots, backpressure, disk pressure, and split.
// PartitionManager is the entry point for create/start/stop and admission control.
package partition

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/internal/config"
	"github.com/jatin711-debug/cronos_db_golang/internal/consumer"
	"github.com/jatin711-debug/cronos_db_golang/internal/dedup"
	"github.com/jatin711-debug/cronos_db_golang/internal/delivery"
	"github.com/jatin711-debug/cronos_db_golang/internal/lifecycle"
	"github.com/jatin711-debug/cronos_db_golang/internal/logcontrol"
	"github.com/jatin711-debug/cronos_db_golang/internal/recoverystate"
	"github.com/jatin711-debug/cronos_db_golang/internal/replication"
	"github.com/jatin711-debug/cronos_db_golang/internal/scheduler"
	"github.com/jatin711-debug/cronos_db_golang/internal/storage"
	"github.com/jatin711-debug/cronos_db_golang/internal/tenant"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
	"github.com/jatin711-debug/cronos_db_golang/pkg/utils"

	log2 "log/slog"

	"github.com/cockroachdb/pebble"
)

// Partition is a local data partition with its durable log, timer wheel,
// consumers, and delivery pipeline.
type Partition struct {
	// ID is the stable partition identifier (hash-derived or cluster-assigned).
	ID int32
	// Topic is the logical topic (or key label) associated with this partition.
	Topic string
	// DataDir is the on-disk root for this partition's WAL, indexes, and stores.
	DataDir string
	// Wal is the segmented write-ahead log for this partition.
	Wal *storage.WAL
	// Scheduler holds the timing wheel and ready queue for delayed delivery.
	Scheduler *scheduler.Scheduler
	// ConsumerGroup manages consumer groups and committed offsets.
	ConsumerGroup *consumer.GroupManager
	// DedupStore tracks recently accepted message IDs for exactly-once publish.
	DedupStore *dedup.Manager
	// Dispatcher delivers events to subscribers and tracks in-flight work.
	Dispatcher *delivery.Dispatcher
	// Worker pulls ready events and hands them to the Dispatcher in batches.
	Worker *delivery.Worker
	// DLQ is the durable dead-letter queue for poison messages after max retries.
	DLQ *delivery.DeadLetterQueue
	// Follower receives replicated Append RPCs when this node is not leader.
	Follower *replication.Follower
	// ReplLeader sends replication to followers when this node leads the partition.
	ReplLeader *replication.Leader
	// ReplicateMu serializes WAL append + replication for this partition when it
	// is a replication leader, so events reach followers in strict offset order
	// (Leader.Replicate requires contiguous offsets and is not safe to call
	// concurrently out of order). It is only taken on the replicated write path;
	// RF=1 / non-leader partitions (ReplLeader == nil) never acquire it, keeping
	// the single-node fast path fully pipelined.
	ReplicateMu sync.Mutex
	// leader is true while this node leads the partition. Promotion and
	// demotion change it with the manager lock held; a replication append and a
	// position request read it without that lock, so it is atomic.
	leader atomic.Bool
	// epoch is the highest leadership epoch this replica has accepted: the
	// cluster-assigned term used for split-brain fencing. Replication appends,
	// position requests, health checks and reconciliation all read it, on
	// different locks or none, so it is atomic. epochMu serializes changes to
	// it and guards the rest of the leadership record.
	epoch   atomic.Int64
	epochMu sync.Mutex
	// epochLeader is the node whose writes this replica accepts at epoch. Empty
	// when the epoch was recorded before any leader identified itself.
	epochLeader string
	// MinKey is the inclusive lower key boundary for range-partitioned keys.
	// Empty means no lower bound.
	MinKey string
	// MaxKey is the exclusive upper key boundary for range-partitioned keys.
	// Empty means no upper bound.
	MaxKey string
	// CreatedTS is when this partition instance was first created on this node.
	CreatedTS time.Time
	// UpdatedTS is when partition metadata (e.g. bounds) was last updated.
	UpdatedTS time.Time
	// deliveryQuit is closed to stop delivery, compaction, and snapshot loops.
	deliveryQuit chan struct{}
	started      bool
	background   sync.WaitGroup
	// deliveryQuitOnce ensures deliveryQuit is closed at most once.
	deliveryQuitOnce sync.Once
	// stopOnce makes stopPartition run once; stopErr is its result.
	stopOnce sync.Once
	stopErr  error
	// storesMu keeps the stores of a partition open while a backup copies them:
	// the backup holds it for reading (holdStores), and closePartition takes it
	// for writing before it closes the first store. storesClosed, which it
	// guards, tells a backup that began later to leave the partition out.
	storesMu     sync.RWMutex
	storesClosed bool
	// replayErr holds the last WAL timer-replay error, if any.
	replayErr atomic.Pointer[error]
	// replicated is true for a partition of a cluster or with more than one
	// replica. Its log is pruned from the start only, by its leader, and the
	// other replicas follow (see retention.go). Immutable.
	replicated bool
	// followed is true for a partition with more than one replica: its leader
	// replicates to followers. Immutable.
	followed bool
	// logStartSeen is the highest log start a leader has told this replica.
	logStartSeen atomic.Int64
	// pruneMu lets one pruning pass run at a time. pruneWaitsAt, which it
	// guards, is the offset of the entry that stopped the last attempt to prune
	// from the start of the log, or -1.
	pruneMu      sync.Mutex
	pruneWaitsAt int64
	// persistedEpoch and persistedLeader mirror epoch.json (0 = nothing stored).
	// Guarded by epochMu.
	persistedEpoch  int64
	persistedLeader string
	// publishing counts publishes between BeginPublish and EndPublish.
	publishing atomic.Int64
	// held lists, in offset order, the events that are in the log but whose
	// publish was not accepted (see unaccepted.go). heldCount mirrors its
	// length for the publish path.
	heldMu    sync.Mutex
	held      []heldRange
	heldCount atomic.Int32
	// feed exports accepted events (see changefeed.go); nil when nothing
	// consumes them. It is set before the partition takes appends.
	feed *changeFeed
	// replQuorum is ReplLeader for readers that do not hold the manager lock.
	replQuorum atomic.Pointer[replication.Leader]
	// lifecycleMu gates eligibility through stream sends against durable mutations.
	lifecycleMu sync.RWMutex
	// lifecycleIdx is set once, when the partition opens in lifecycle mode or
	// a follower switches to it (EnableReplicatedLifecycle), and never unset.
	lifecycleIdx    atomic.Pointer[lifecycle.Index]
	termStartMode   atomic.Bool
	termStartEpoch  int64 // guarded by ReplicateMu; last local barrier appended
	termStartOffset int64 // guarded by ReplicateMu
	relocationBytes int64
	diskAdmission   func() error
}

// leadershipRecord is the fencing state a replica keeps on disk: the highest
// leadership epoch it has accepted and the node that holds it.
type leadershipRecord struct {
	Epoch    int64  `json:"epoch"`
	LeaderID string `json:"leader_id,omitempty"`
}

// parseLeadershipRecord reads epoch.json. Older builds stored a bare number.
func parseLeadershipRecord(data []byte) (leadershipRecord, error) {
	var record leadershipRecord
	if err := json.Unmarshal(data, &record.Epoch); err == nil {
		return record, nil
	}
	err := json.Unmarshal(data, &record)
	return record, err
}

// IsLeader reports whether this node currently leads the partition.
func (p *Partition) IsLeader() bool { return p.leader.Load() }

// Epoch returns the highest leadership epoch this replica has accepted.
func (p *Partition) Epoch() int64 { return p.epoch.Load() }

// EpochLeader returns the node whose writes this replica accepts at Epoch. It
// is empty when no leader has identified itself for that epoch yet.
func (p *Partition) EpochLeader() string {
	p.epochMu.Lock()
	defer p.epochMu.Unlock()
	return p.epochLeader
}

// restoreLeadership sets the leadership record as it was read from epoch.json
// when the partition was opened.
func (p *Partition) restoreLeadership(record leadershipRecord) {
	p.epochMu.Lock()
	defer p.epochMu.Unlock()
	p.epoch.Store(record.Epoch)
	p.epochLeader = record.LeaderID
	p.persistedEpoch, p.persistedLeader = record.Epoch, record.LeaderID
}

// PersistEpoch durably fences older leaders before accepting their successors.
func (p *Partition) PersistEpoch(epoch int64) error {
	return p.AcceptLeadership(epoch, "")
}

// AcceptLeadership records, durably and before any of its writes are applied,
// that leaderID holds epoch on this replica. It refuses an epoch older than
// the one already accepted, and a second node claiming an epoch that already
// has a holder: a term has exactly one writer, which is what lets a follower
// tell a deposed leader from its successor. An empty leaderID leaves the
// holder open for the first node that identifies itself.
//
// It is safe for concurrent use: a replication append and a promotion can
// both be deciding about the same epoch.
func (p *Partition) AcceptLeadership(epoch int64, leaderID string) error {
	p.epochMu.Lock()
	defer p.epochMu.Unlock()
	current := p.epoch.Load()
	if epoch < current {
		return fmt.Errorf("epoch regression: %d < %d", epoch, current)
	}
	holder := leaderID
	if epoch == current {
		if p.epochLeader != "" && leaderID != "" && leaderID != p.epochLeader {
			return fmt.Errorf("epoch %d is already held by %s", epoch, p.epochLeader)
		}
		if holder == "" {
			holder = p.epochLeader
		}
	}
	// Leadership reconciliation re-asserts the current epoch on every tick while
	// holding the manager lock. Only a change needs the fsynced write; repeating
	// it stalls every publish on this node behind that lock.
	if epoch > 0 && epoch == p.persistedEpoch && holder == p.persistedLeader {
		p.epoch.Store(epoch)
		p.epochLeader = holder
		return nil
	}
	data, err := json.Marshal(leadershipRecord{Epoch: epoch, LeaderID: holder})
	if err != nil {
		return err
	}
	if err := utils.AtomicWriteFile(p.DataDir+"/epoch.json", data, 0600); err != nil {
		return err
	}
	p.epoch.Store(epoch)
	p.epochLeader = holder
	p.persistedEpoch, p.persistedLeader = epoch, holder
	return nil
}

// LogPosition returns the offset and term of the last entry in this replica's
// log: (-1, 0) when it is empty.
func (p *Partition) LogPosition() (lastOffset, lastTerm int64) {
	lastOffset = p.Wal.GetLastOffset()
	if lastOffset < 0 {
		return -1, 0
	}
	if term, err := p.Wal.GetTermForOffset(lastOffset); err == nil {
		lastTerm = term
	}
	return lastOffset, lastTerm
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

// PartitionManager manages local partitions: create/start/stop, admission
// control, leadership promotion, and recovery (snapshot + WAL replay).
type PartitionManager struct {
	mu               sync.RWMutex
	partitions       map[int32]*Partition // partitionID -> live partition
	nodeID           string               // local node identity for replication
	config           *types.Config        // shared server configuration
	pebbleCache      *pebble.Cache        // optional shared Pebble block cache
	tenantAccountant tenantAccountant     // optional delivery accounting hook
	splitting        map[int32]bool       // partitions currently undergoing split
	splittingMu      sync.RWMutex         // protects splitting map
	backpressureMgr  *BackpressureManager // memory + per-partition rate limits
	fsyncCoalescer   *storage.FsyncCoalescer
	// lifecycleRequired is the cluster's record of which partitions require
	// the lifecycle format; nil outside a cluster. Guarded by mu.
	lifecycleRequired func(partitionID int32) bool
	termStartRequired func(partitionID int32) bool
	// writable is the cluster's view of whether this node may accept publishes
	// for a partition; nil means every led partition is writable.
	writable func(partitionID int32) bool
	// changeFeed receives the accepted events of every partition; nil when
	// nothing consumes them.
	changeFeed FeedFunc
	// pinned partitions stay loaded even when this node holds no replica of
	// them; releasing marks partitions whose stores are still being closed.
	// Both are guarded by mu.
	pinned    map[int32]bool
	releasing map[int32]bool
	// diskGuard refuses publishes while the data disk is nearly full; nil
	// when there is none.
	diskGuard       *DiskGuard
	schedulerBudget *scheduler.MemoryBudget
}

// tenantAccountant is the minimal interface needed for delivery callbacks.
type tenantAccountant interface {
	RecordDelivery(tenant tenant.ID)
}

// NewPartitionManager creates a partition manager for the given node.
// When config.FlushIntervalMS > 0, a shared FsyncCoalescer is started.
func NewPartitionManager(nodeID string, config *types.Config) *PartitionManager {
	budgetBytes := config.SchedulerMemoryBytes
	if budgetBytes <= 0 {
		budgetBytes = 64 << 20
	}
	pm := &PartitionManager{
		schedulerBudget: scheduler.NewMemoryBudget(budgetBytes),
		partitions:      make(map[int32]*Partition),
		nodeID:          nodeID,
		config:          config,
		splitting:       make(map[int32]bool),
		backpressureMgr: NewBackpressureManager(config.MaxMemoryUsagePercent, config.MemoryCheckIntervalMs, uint64(max(config.MemoryLimitBytes, 0))),
	}
	if config.FlushIntervalMS > 0 {
		pm.fsyncCoalescer = storage.NewFsyncCoalescer(time.Duration(config.FlushIntervalMS) * time.Millisecond)
	}
	return pm
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

// deliveryConfig takes the dispatcher's settings from the server
// configuration: ack timeout, retries, backoff, credits and the circuit
// breaker. A setting left at zero keeps the dispatcher's default, which is
// what a configuration built by hand, as tests do, relies on.
func deliveryConfig(cfg *types.Config) *delivery.Config {
	c := delivery.DefaultConfig()
	if cfg.MaxRetries > 0 {
		c.MaxRetries = int32(cfg.MaxRetries)
	}
	if cfg.DefaultAckTimeout > 0 {
		c.DefaultAckTimeout = cfg.DefaultAckTimeout
	}
	if cfg.RetryBackoff > 0 {
		c.RetryBackoff = cfg.RetryBackoff
	}
	if cfg.MaxDeliveryCredits > 0 {
		c.MaxDeliveryCredits = int32(cfg.MaxDeliveryCredits)
	}
	if cfg.CircuitBreakerFailureThreshold > 0 {
		c.CircuitBreakerFailureThreshold = cfg.CircuitBreakerFailureThreshold
	}
	if cfg.CircuitBreakerMinAttempts > 0 {
		c.CircuitBreakerMinAttempts = cfg.CircuitBreakerMinAttempts
	}
	if cfg.CircuitBreakerOpenDurationMs > 0 {
		c.CircuitBreakerOpenDurationMs = cfg.CircuitBreakerOpenDurationMs
	}
	return c
}

// createPartitionLocked creates a new partition (assumes lock is held)
func (pm *PartitionManager) createPartitionLocked(partitionID int32, topic string) (createErr error) {
	termMarker := filepath.Join(pm.config.DataDir, "partitions", fmt.Sprint(partitionID), logcontrol.TermStartMarker)
	termMode := false
	if data, err := os.ReadFile(termMarker); err == nil {
		if string(data) != "1\n" {
			return fmt.Errorf("partition %d has an unsupported term-start format", partitionID)
		}
		termMode = true
	} else if !os.IsNotExist(err) {
		return err
	}
	if pm.config.EventLifecycle && ((!pm.config.ClusterEnabled && pm.config.ReplicationFactor != 1) || (pm.config.ClusterEnabled && pm.config.ReplicationFactor < 2) || pm.changeFeed != nil || pm.config.ExperimentalFeatures) {
		return fmt.Errorf("event-lifecycle requires standalone RF=1 or a replicated cluster, without a change feed or experimental features")
	}
	marker := filepath.Join(pm.config.DataDir, "partitions", fmt.Sprint(partitionID), lifecycle.Marker)
	lifecycleData := false
	if format, err := os.ReadFile(marker); err == nil {
		// In a cluster a partition is put into lifecycle mode when it comes to
		// require the format (EnableReplicatedLifecycle), not by the flag.
		if !pm.config.EventLifecycle && !pm.config.ClusterEnabled {
			return fmt.Errorf("partition %d contains lifecycle data; --event-lifecycle is required", partitionID)
		}
		if string(format) != "1\n" {
			return fmt.Errorf("partition %d has an unsupported lifecycle format", partitionID)
		}
		if err := pm.replicatedLifecycleAllowed(); pm.config.ClusterEnabled && err != nil {
			return fmt.Errorf("partition %d contains lifecycle data: %w", partitionID, err)
		}
		lifecycleData = true
	} else if !os.IsNotExist(err) {
		return err
	}
	// Its previous instance still holds the files until it has finished closing.
	if pm.releasing[partitionID] {
		return fmt.Errorf("partition %d is being released; retry", partitionID)
	}
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
	wal, err := storage.NewWALWithCoalescer(dataDir, partitionID, walConfig, cipher, pm.fsyncCoalescer)
	if err != nil {
		return fmt.Errorf("create WAL: %w", err)
	}
	var eventIndex *lifecycle.Index
	if (pm.config.EventLifecycle && !pm.config.ClusterEnabled) || lifecycleData {
		eventIndex, err = lifecycle.Open(dataDir, pm.pebbleCache)
		if err != nil {
			wal.Close()
			return fmt.Errorf("open lifecycle index: %w", err)
		}
		if err = eventIndex.Rebuild(context.Background(), wal); err != nil {
			eventIndex.Close()
			wal.Close()
			return fmt.Errorf("rebuild lifecycle index: %w", err)
		}
	}
	// Clean up WAL on any subsequent failure
	defer func() {
		if createErr != nil {
			if eventIndex != nil {
				eventIndex.Close()
			}
			wal.Close()
		}
	}()

	// Create scheduler with two-tier cold store support
	hotWindowMinutes := pm.config.HotWindowMinutes
	sched, err := scheduler.NewScheduler(dataDir, partitionID, int32(pm.config.TickMS), int32(pm.config.WheelSize), hotWindowMinutes, wal, pm.pebbleCache)
	if err != nil {
		return fmt.Errorf("create scheduler: %w", err)
	}
	defer func() {
		if createErr != nil {
			sched.Stop()
		}
	}()
	if err := sched.SetMemoryBudget(pm.schedulerBudget, pm.config.PartitionCount, pm.pebbleCache); err != nil {
		return fmt.Errorf("configure scheduler budget: %w", err)
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
	defer func() {
		if createErr != nil {
			dedupManager.Close()
		}
	}()

	// Create offset store for persistent consumer offsets
	offsetStore, err := consumer.NewOffsetStore(dataDir, partitionID, pm.pebbleCache)
	if err != nil {
		return fmt.Errorf("create offset store: %w", err)
	}

	// Create consumer group manager with persistent offset store
	consumerGroup := consumer.NewGroupManagerWithStore(offsetStore)
	consumerGroup.SetMaxGroups(pm.config.MaxConsumerGroups)
	defer func() {
		if createErr != nil {
			consumerGroup.Close()
		}
	}()

	// Create dispatcher with a durable dead-letter queue so poison messages
	// (delivery failed after max retries) are captured on disk instead of being
	// silently dropped. Retry is operator-driven (no auto-retry loop) to avoid
	// re-driving genuinely-poison messages forever.
	dispatcherConfig := deliveryConfig(pm.config)
	// A dead-lettered event is stored whole, so its file is encrypted like the
	// log. (A nil *SegmentCipher must not become a non-nil interface.)
	var dlqCipher delivery.EntryCipher
	if cipher != nil {
		dlqCipher = cipher
	}
	dlq, err := delivery.NewEncryptedDeadLetterQueue(dataDir, 0, dlqCipher) // 0 → default max entries
	if err != nil {
		return fmt.Errorf("create dead-letter queue: %w", err)
	}
	dispatcher := delivery.NewDispatcherWithDLQ(dispatcherConfig, dlq)
	defer func() {
		if createErr != nil {
			dispatcher.Close()
			dlq.Close()
		}
	}()
	dispatcher.IsCompleted = func(group string, offset int64) bool { return consumerGroup.IsCompleted(group, partitionID, offset) }
	// A dead-lettered event has reached its final disposition for the group;
	// without a completion record the WAL redrive would deliver it again.
	dispatcher.OnDeadLettered = func(group string, events []*types.Event) {
		if err := consumerGroup.CommitDelivery(group, partitionID, events); err != nil {
			log.Printf("[Partition %d] record dead-letter disposition for group %s: %v", partitionID, group, err)
		}
	}

	// Create worker. Batch size of 100 amortizes DispatchBatch overhead
	// (metrics observe, map allocations, in-flight CAS, shard write-lock)
	// across many events. Round-robin fairness across subscribers within a
	// consumer group is handled by the dispatcher's per-group cursor.
	worker := delivery.NewWorker(dispatcher, 100)

	// Wire tenant delivery callback if configured
	if pm.tenantAccountant != nil {
		dispatcher.OnDeliveryComplete = func(tenantID string) {
			pm.tenantAccountant.RecordDelivery(tenant.ID(tenantID))
		}
	}

	// Create partition
	partition := &Partition{
		relocationBytes: pm.config.WALRelocationBytes,
		diskAdmission:   pm.DiskAdmission,
		replicated:      pm.config.ClusterEnabled || pm.config.ReplicationFactor > 1,
		followed:        pm.config.ReplicationFactor > 1,
		pruneWaitsAt:    -1,
		ID:              partitionID,
		Topic:           topic,
		DataDir:         dataDir,
		Wal:             wal,
		Scheduler:       sched,
		ConsumerGroup:   consumerGroup,
		DedupStore:      dedupManager,
		Dispatcher:      dispatcher,
		DLQ:             dlq,
		Worker:          worker,
		CreatedTS:       time.Now(),
		UpdatedTS:       time.Now(),
		deliveryQuit:    make(chan struct{}),
	}
	// What lies below the start of the log was finished before it was removed.
	if err = consumerGroup.SetLogStart(partitionID, wal.GetFirstOffset()); err != nil {
		return fmt.Errorf("apply log start to consumer progress: %w", err)
	}
	// And nothing is finished beyond its end: a log that came back from a
	// crash without its last entries gives their offsets to new events.
	if err = partition.forgetBeyondLog(wal.GetNextOffset(), "the log ends there"); err != nil {
		return err
	}
	dispatcher.DeliverableThrough = partition.DeliverableThrough
	partition.termStartMode.Store(termMode)
	partition.lifecycleIdx.Store(eventIndex)
	if eventIndex != nil || pm.config.ClusterEnabled || pm.config.ReplicationFactor > 1 || termMode {
		// A partition of a cluster can switch to lifecycle mode while it runs
		// (EnableReplicatedLifecycle), so its hooks are installed from the
		// start and pass every event through until it does.
		dispatcher.LockEvents = func() func() {
			if partition.eventIndex() == nil {
				return func() {}
			}
			partition.lifecycleMu.RLock()
			return partition.lifecycleMu.RUnlock
		}
		dispatcher.EventEligible = func(e *types.Event) bool {
			if logcontrol.IsTermStart(e) {
				return partition.ActiveEvent(e)
			}
			if partition.eventIndex() == nil {
				return true
			}
			return e.ScheduleTs <= time.Now().UnixMilli() && partition.ActiveEvent(e)
		}
		dispatcher.PublicEvent = func(e *types.Event) *types.Event {
			if partition.eventIndex() == nil {
				return e
			}
			return lifecycle.Public(e)
		}
	}

	if data, err := os.ReadFile(dataDir + "/epoch.json"); err == nil {
		record, err := parseLeadershipRecord(data)
		if err != nil {
			return fmt.Errorf("read partition epoch: %w", err)
		}
		partition.restoreLeadership(record)
	} else if !os.IsNotExist(err) {
		return err
	}
	if err := partition.applyRecoverySnapshot(); err != nil {
		return fmt.Errorf("recover snapshot state: %w", err)
	}
	if eventIndex != nil && !partition.replicated {
		// A lost periodic-fsync tail can reuse offsets after restart. Persist
		// a new term before assigning handles, even for the same message ID.
		generation := partition.Epoch()
		if last := wal.GetLastOffset(); last >= wal.GetFirstOffset() {
			term, termErr := wal.GetTermForOffset(last)
			if termErr != nil {
				return termErr
			}
			generation = max(generation, term)
		}
		if generation == 1<<63-1 {
			return fmt.Errorf("lifecycle generation exhausted")
		}
		if err = partition.AcceptLeadership(generation+1, pm.nodeID); err != nil {
			return err
		}
		wal.SetCurrentTerm(generation + 1)
	}
	if pm.changeFeed != nil {
		if err := partition.openChangeFeed(pm.changeFeed, pm.config.ClusterEnabled); err != nil {
			return err
		}
	}
	pm.partitions[partitionID] = partition

	// Set up rate limiter for this partition if configured
	if pm.backpressureMgr != nil && pm.config.MaxIngestRatePerPartition > 0 && pm.config.IngestRateBurstSize > 0 {
		pm.backpressureMgr.SetRateLimiter(partitionID, pm.config.MaxIngestRatePerPartition, pm.config.IngestRateBurstSize)
	}

	return nil
}

// CreatePartition creates a new local partition with WAL, scheduler, and
// delivery components. The partition is not started until StartPartition.
func (pm *PartitionManager) CreatePartition(partitionID int32, topic string) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	return pm.createPartitionLocked(partitionID, topic)
}

// GetPartition returns the public types.Partition view for partitionID.
func (pm *PartitionManager) GetPartition(partitionID int32) (*types.Partition, error) {
	partition, err := pm.GetInternalPartition(partitionID)
	if err != nil {
		return nil, err
	}

	return pm.toTypesPartition(partition), nil
}

// GetInternalPartition returns the internal Partition object for advanced use
// (WAL access, replication wiring). Returns ErrPartitionNotFound if missing.
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
		return nil, fmt.Errorf("start partition %d: %w", partitionID, err)
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
		return nil, fmt.Errorf("start partition %d: %w", partitionID, err)
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

// SetDiskGuard makes publishes depend on g (see DiskAdmission). Set it before
// the node serves requests.
func (pm *PartitionManager) SetDiskGuard(g *DiskGuard) {
	pm.diskGuard = g
}

// DiskAdmission returns an error while the node refuses publishes because its
// data disk is nearly full.
func (pm *PartitionManager) DiskAdmission() error {
	return pm.diskGuard.Check()
}

// CanAccept returns true if the partition can accept new publishes without exceeding capacity limits.
func (pm *PartitionManager) CanAccept(partitionID int32) bool {
	return pm.CanAcceptBatch(partitionID, 1)
}

// CanAcceptBatch performs admission checks once for a batch. Queue, timing
// wheel, and in-flight limits include the incoming batch cardinality.
func (pm *PartitionManager) CanAcceptBatch(partitionID int32, count int64) bool {
	if count <= 0 {
		return true
	}
	// Check backpressure first (memory + rate limiting)
	if pm.backpressureMgr != nil && !pm.backpressureMgr.CanAcceptN(partitionID, count) {
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
		if depth+count > pm.config.MaxReadyQueueSize {
			return false
		}
		// Load shedding: reject if above threshold percentage of max
		if pm.config.LoadSheddingThreshold > 0 {
			threshold := int64(float64(pm.config.MaxReadyQueueSize) * pm.config.LoadSheddingThreshold)
			if depth+count >= threshold {
				return false
			}
		}
	}
	if pm.config.MaxTimingWheelSize > 0 {
		if partition.Scheduler.GetTimingWheelDepth()+count > pm.config.MaxTimingWheelSize {
			return false
		}
	}
	if pm.config.MaxInFlightPerPartition > 0 {
		if partition.Dispatcher.GetStats().ActiveDeliveries+count > pm.config.MaxInFlightPerPartition {
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
		return partition, partition.GetReplayError()
	}
	pm.mu.RUnlock()

	// Slow path: partition missing, escalate to write lock.
	pm.mu.Lock()
	defer pm.mu.Unlock()

	// Double-checked locking
	if partition, exists := pm.partitions[partitionID]; exists {
		return partition, partition.GetReplayError()
	}

	// Auto-create
	if err := pm.createPartitionLocked(partitionID, topic); err != nil {
		return nil, fmt.Errorf("auto-create partition: %w", err)
	}

	// Start the newly created partition synchronously
	partition := pm.partitions[partitionID]
	if err := pm.startPartitionInternal(partition); err != nil {
		return nil, fmt.Errorf("start partition %d: %w", partitionID, err)
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
	if partition.started {
		return nil
	}
	// Snapshots/checkpoints contain metadata, not the pending timer set. Rebuild
	// from retained WAL records on every start; duplicate delivery is allowed.
	//
	// A partition of a cluster does this when its node is made leader, and
	// not before. Until then it delivers nothing, the log may still change,
	// and a timer holds its event: scheduled on every replica, the events that
	// wait to come due were held in memory once for each of them.
	snapshotMgr := NewSnapshotManager(partition.DataDir, partition.ID)
	if !partition.replicated {
		pm.replayWALTimers(partition)
		if err := partition.GetReplayError(); err != nil {
			return err
		}
	}
	// Re-seed the dedup store from the WAL tail. The dedup Pebble store runs with
	// DisableWAL + NoSync for throughput, so claims made just before a crash may be
	// missing; the WAL is the durable record of what was actually accepted.
	if err := pm.recoverDedupFromWAL(partition); err != nil {
		return err
	}
	partition.started = true

	// Start scheduler
	partition.Scheduler.Start()

	// Start worker
	partition.Worker.Start()

	// Start delivery loop (event-driven): consume scheduler ready signals and
	// immediately hand over batches to the worker.
	partition.background.Add(1)
	utils.GoSafe("partition-delivery-loop", func() {
		defer partition.background.Done()
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

	if partition.feed != nil {
		partition.background.Add(1)
		utils.GoSafe("partition-change-feed", func() {
			defer partition.background.Done()
			partition.runChangeFeed()
		})
	}

	// Start compaction loop (every ten minutes unless configured otherwise)
	compactionInterval := pm.config.CompactionInterval
	if compactionInterval <= 0 {
		compactionInterval = 10 * time.Minute
	}
	partition.background.Add(1)
	utils.GoSafe("partition-compaction-loop", func() {
		defer partition.background.Done()
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
	partition.background.Add(1)
	utils.GoSafe("partition-dedup-prune-loop", func() {
		defer partition.background.Done()
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
	partition.background.Add(1)
	utils.GoSafe("partition-snapshot-loop", func() {
		defer partition.background.Done()
		snapshotMgr.StartPeriodicSnapshots(partition, 5*time.Minute)
	})

	return nil
}

// recoverDedupFromWAL replays a byte-bounded suffix after a durable checkpoint.
// Without a matching checkpoint it scans the entire retained log. No event
// count is evidence of durability, especially on a newly promoted follower.
func (pm *PartitionManager) recoverDedupFromWAL(partition *Partition) (retErr error) {
	defer func() {
		if retErr != nil {
			partition.setReplayError(retErr)
		}
	}()
	if partition.DedupStore == nil || partition.Wal == nil {
		return nil
	}
	lastOffset := partition.Wal.GetLastOffset()
	if lastOffset < 0 {
		return nil
	}

	startOffset := partition.Wal.GetFirstOffset()
	point, err := partition.DedupStore.LoadRecoveryPoint()
	if err != nil {
		return fmt.Errorf("load dedup recovery point: %w", err)
	}
	if point != nil && point.Offset >= startOffset && point.Offset <= lastOffset {
		event, err := partition.Wal.ReadEvent(point.Offset)
		if err != nil {
			return fmt.Errorf("validate dedup recovery point: %w", err)
		}
		if event.Term == point.Term && event.MessageId == point.MessageID {
			startOffset = point.Offset + 1
		}
	}

	ttlMs := int64(pm.config.DedupTTLHours) * 60 * 60 * 1000
	minCreatedTS := int64(0)
	if ttlMs > 0 {
		minCreatedTS = time.Now().UnixMilli() - ttlMs
	}

	recovered := 0
	for from := startOffset; from <= lastOffset; {
		to := min(from+logWalkEvents-1, lastOffset)
		events, err := partition.Wal.ReadEventsWithin(from, to, logWalkBytes)
		if err != nil {
			return fmt.Errorf("partition %d dedup recovery read %d-%d: %w", partition.ID, from, to, err)
		}
		if len(events) == 0 {
			from = to + 1 // removed from the log meanwhile
			continue
		}
		from = events[len(events)-1].Offset + 1
		for _, ev := range events {
			if logcontrol.IsTermStart(ev) {
				if err := logcontrol.ValidateTermStart(ev); err != nil {
					return err
				}
				if err := partition.enableTermStart(); err != nil {
					return err
				}
				continue
			}
			if lifecycle.HasReservedMeta(ev) {
				if partition.eventIndex() == nil {
					return fmt.Errorf("lifecycle WAL requires --event-lifecycle")
				}
				// A durable replacement proves the logical publish existed. Seed
				// its ID as accepted if the original was pruned or this is a restore.
				if minCreatedTS == 0 || ev.CreatedTs == 0 || ev.CreatedTs >= minCreatedTS {
					if err := partition.DedupStore.Put(dedup.EventKey(ev), ev.Offset, ev.CreatedTs); err != nil {
						return fmt.Errorf("recover lifecycle dedup: %w", err)
					}
				}
				continue
			}
			mid := ev.GetMessageId()
			if mid == "" {
				continue
			}
			// Skip entries already past the dedup TTL — they carry no dedup value.
			if minCreatedTS > 0 && ev.GetCreatedTs() > 0 && ev.GetCreatedTs() < minCreatedTS {
				continue
			}
			key := dedup.EventKey(ev)
			stored, exists, err := partition.DedupStore.GetOffset(key)
			if err != nil {
				return fmt.Errorf("dedup recovery lookup %q: %w", mid, err)
			}
			if outcome, _ := dedup.DecodeOffset(stored); exists && (outcome == dedup.Accepted || stored == dedup.AppendedAt(ev.Offset)) {
				continue
			}
			// The log holds the event, which does not show that its publish
			// was accepted. Recording where it is lets a retry finish that
			// publish instead of appending the event a second time.
			if err := partition.DedupStore.Put(key, dedup.AppendedAt(ev.Offset), ev.GetCreatedTs()); err != nil {
				return fmt.Errorf("dedup recovery put %q: %w", mid, err)
			}
			recovered++
		}
	}
	if recovered > 0 {
		log.Printf("[Partition %d] Dedup recovery: re-seeded %d message IDs from WAL tail (offsets %d-%d)",
			partition.ID, recovered, startOffset, lastOffset)
	}
	if startOffset <= lastOffset {
		event, err := partition.Wal.ReadEvent(lastOffset)
		if err != nil {
			return fmt.Errorf("read dedup recovery end: %w", err)
		}
		if err := partition.DedupStore.SaveRecoveryPoint(dedup.RecoveryPoint{Offset: event.Offset, Term: event.Term, MessageID: event.MessageId}); err != nil {
			return fmt.Errorf("save dedup recovery point: %w", err)
		}
	}
	return nil
}

// logWalkBytes and logWalkEvents bound one read of a walk through the log at
// a start or a promotion: about this many bytes of records, and no more than
// this many events. Such a walk used to read ten thousand events at a time,
// which is megabytes of small events and gigabytes of large ones.
const (
	logWalkBytes  = 16 << 20
	logWalkEvents = 10_000
)

// replayWALTimers walks the log and schedules the events that are not due
// yet, which restores the timers a stopped node lost.
//
// Events that are due already are left where they are. A subscription reads
// what its group has not finished from the log when it starts, at the pace
// its consumers take it (redriveRetained), and nothing is delivered without
// one. Queuing them here as well put the whole undelivered backlog in memory
// at every start, where nothing bounded it: a node with more waiting on disk
// than it has memory did not come back.
func (pm *PartitionManager) replayWALTimers(partition *Partition) {
	// Every event in the log is scheduled below, held or not.
	partition.dropHeld()
	// What was scheduled before goes first. It was read from the log as it
	// was then, and the log may have been cut and written again since.
	if err := partition.resetSchedule(); err != nil {
		partition.setReplayError(err)
		return
	}
	lastOffset := partition.Wal.GetLastOffset()
	if lastOffset < 0 {
		return // Empty WAL, nothing to replay
	}

	startOffset := partition.Wal.GetFirstOffset()

	now := time.Now().UnixMilli()
	scheduledCount := 0
	maturedCount := 0
	lastScheduled := startOffset - 1

	for batchStart := startOffset; batchStart <= lastOffset; {
		batchEnd := min(batchStart+logWalkEvents-1, lastOffset)

		events, err := partition.Wal.ReadEventsWithin(batchStart, batchEnd, logWalkBytes)
		if err != nil {
			err = fmt.Errorf("WAL replay failed at offsets %d-%d: %w", batchStart, batchEnd, err)
			partition.setReplayError(err)
			log2.Warn("WAL replay failed", "partition", partition.ID, "start_offset", batchStart, "end_offset", batchEnd, "error", err)
			return
		}
		if len(events) == 0 {
			batchStart = batchEnd + 1 // removed from the log meanwhile
			continue
		}
		batchStart = events[len(events)-1].Offset + 1

		for _, event := range events {
			lastScheduled = event.Offset
			if !partition.ActiveEvent(event) {
				continue
			}
			if event.GetScheduleTs() <= now {
				maturedCount++ // a subscription reads it from the log
				continue
			}
			if err := partition.Scheduler.Schedule(event); err != nil {
				log2.Warn("WAL replay scheduler error", "partition", partition.ID, "offset", event.Offset, "error", err)
				continue
			}
			scheduledCount++
		}
	}

	// Update checkpoint incrementally
	pm.writeTimerCheckpoint(partition, lastScheduled)

	log.Printf("[Partition %d] WAL replay complete: %d future events re-scheduled, %d already due left in the log for subscriptions (offsets %d-%d)",
		partition.ID, scheduledCount, maturedCount, startOffset, lastScheduled)
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
	maturedCount := 0
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
			// See replayWALTimers: schedule every event so matured-during-downtime
			if !partition.ActiveEvent(event) {
				continue
			}
			// timers are enqueued for immediate delivery rather than dropped.
			if err := partition.Scheduler.Schedule(event); err != nil {
				log2.Warn("WAL replay scheduler error", "partition", partition.ID, "offset", event.Offset, "error", err)
				continue
			}
			if event.GetScheduleTs() > now {
				scheduledCount++
			} else {
				maturedCount++
			}
			lastScheduled = event.Offset
		}
	}

	// Update checkpoint incrementally
	pm.writeTimerCheckpoint(partition, lastScheduled)

	log.Printf("[Partition %d] WAL replay from offset %d complete: %d future events re-scheduled, %d matured-during-downtime enqueued (offsets %d-%d)",
		partition.ID, startOffset, scheduledCount, maturedCount, startOffset, lastScheduled)
}

// TimerCheckpoint stores incremental WAL timer-replay progress so restarts
// need not re-scan the entire log.
type TimerCheckpoint struct {
	// LastScheduledOffset is the highest WAL offset whose timer was re-scheduled.
	LastScheduledOffset int64 `json:"last_scheduled_offset"`
	// LastCheckpointTime is when this checkpoint was written (Unix milliseconds).
	LastCheckpointTime int64 `json:"last_checkpoint_time"`
}

// readTimerCheckpoint reads the timer replay checkpoint from disk, if present.
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

// writeTimerCheckpoint atomically writes the timer replay checkpoint to disk.
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
	if err := utils.AtomicWriteFile(cpPath, data, 0644); err != nil {
		log2.Warn("Failed to write timer checkpoint", "error", err)
	}
}

// RunCompaction prunes segments whose events have durable completion records
// for every matching group (exported for external callers).
func (p *Partition) RunCompaction() {
	p.runCompaction()
}

// runCompaction uses per-event completion, the same gate as manual retention.
func (p *Partition) runCompaction() {
	if p.replicated && !p.IsLeader() {
		return // a replica follows its leader's log start
	}
	deleted, err := p.PruneWAL(context.Background(), storage.PruneOptions{AllCompleted: true})
	if err != nil {
		log.Printf("[Partition %d] WAL compaction error: %v", p.ID, err)
	} else if deleted > 0 {
		log.Printf("[Partition %d] Compacted %d completed WAL segments", p.ID, deleted)
	}
}

// ListPartitions returns a snapshot of all local partitions.
func (pm *PartitionManager) ListPartitions() []*Partition {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	partitions := make([]*Partition, 0, len(pm.partitions))
	for _, partition := range pm.partitions {
		partitions = append(partitions, partition)
	}

	return partitions
}

// FailedLogs returns the IDs of the local partitions whose log failed and
// takes no more writes (see storage.ErrLogFailed).
func (pm *PartitionManager) FailedLogs() []int32 {
	var failed []int32
	for _, p := range pm.ListPartitions() {
		if p.Wal != nil && p.Wal.Failure() != nil {
			failed = append(failed, p.ID)
		}
	}
	sort.Slice(failed, func(i, j int) bool { return failed[i] < failed[j] })
	return failed
}

// StartPartition starts background workers (scheduler, delivery, compaction,
// snapshot) for an existing partition, replaying WAL timers as needed.
func (pm *PartitionManager) StartPartition(partitionID int32) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	return pm.startPartitionLocked(partitionID)
}

// StopPartition gracefully stops delivery, drains in-flight work, and closes
// durable stores and the WAL for the given partition.
func (pm *PartitionManager) StopPartition(partitionID int32) error {
	partition, err := pm.GetInternalPartition(partitionID)
	if err != nil {
		return err
	}
	return stopPartition(partition)
}

// stopPartition stops a partition's workers and closes its stores. A partition
// is stopped once: closing a store twice panics, and the manager keeps a
// stopped partition listed, so a release and a shutdown can both reach it. A
// second caller waits for the first and gets its result.
func stopPartition(partition *Partition) error {
	partition.stopOnce.Do(func() { partition.stopErr = closePartition(partition) })
	return partition.stopErr
}

func closePartition(partition *Partition) error {

	// Signal delivery goroutines to stop FIRST to avoid circular lock deadlock
	// Delivery goroutines read from deliveryQuit channel - closing it allows them to exit
	partition.deliveryQuitOnce.Do(func() { close(partition.deliveryQuit) })
	partition.background.Wait()

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

	// Wait for a backup that is copying the stores, and keep later ones off them.
	partition.storesMu.Lock()
	partition.storesClosed = true
	partition.storesMu.Unlock()

	// Close the dead-letter queue so its segment writer flushes and releases the
	// data directory before WAL close / temp cleanup.
	if partition.DLQ != nil {
		if err := partition.DLQ.Close(); err != nil {
			log.Printf("[Partition %d] DLQ close failed: %v", partition.ID, err)
		}
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
	if partition.eventIndex() != nil {
		if err := partition.eventIndex().Close(); err != nil {
			return fmt.Errorf("close lifecycle index: %w", err)
		}
	}
	if partition.Wal != nil {
		if err := partition.Wal.Close(); err != nil {
			return fmt.Errorf("close WAL: %w", err)
		}
	}

	return nil
}

// holdStores keeps the partition's stores open until releaseStores. It reports
// false, holding nothing, if they are already closed.
func (p *Partition) holdStores() bool {
	p.storesMu.RLock()
	if p.storesClosed {
		p.storesMu.RUnlock()
		return false
	}
	return true
}

func (p *Partition) releaseStores() { p.storesMu.RUnlock() }

// PinPartition keeps a partition loaded on this node whether or not the
// cluster assigns it here. The node's first partition is pinned because the
// public handlers are built on its stores.
func (pm *PartitionManager) PinPartition(partitionID int32) {
	pm.mu.Lock()
	if pm.pinned == nil {
		pm.pinned = make(map[int32]bool)
	}
	pm.pinned[partitionID] = true
	pm.mu.Unlock()
}

// ReleasePartition unloads a partition that this node no longer leads or
// holds a replica of: its timers and delivery stop, its stores are closed and
// the manager forgets it. An idle partition otherwise keeps its memory, its
// file handles and its preallocated log segment for as long as the process
// runs, and its scheduler would go on firing timers for a log this node no
// longer owns.
//
// The partition's directory is removed only if its log is empty. Otherwise
// the files stay on disk, and the partition is reopened from them if it is
// assigned to this node again.
//
// It is a no-op for a partition that is not loaded or is pinned, and an error
// for one that is still leading or has a publish in flight.
func (pm *PartitionManager) ReleasePartition(partitionID int32) error {
	pm.mu.Lock()
	partition, exists := pm.partitions[partitionID]
	if !exists || pm.pinned[partitionID] {
		pm.mu.Unlock()
		return nil
	}
	if partition.IsLeader() || partition.ReplLeader != nil || partition.publishing.Load() > 0 {
		pm.mu.Unlock()
		return fmt.Errorf("partition %d is still in use", partitionID)
	}
	delete(pm.partitions, partitionID)
	if pm.releasing == nil {
		pm.releasing = make(map[int32]bool)
	}
	pm.releasing[partitionID] = true
	pm.mu.Unlock()

	empty := partition.Wal != nil && partition.Wal.GetLastOffset() < 0
	err := stopPartition(partition)
	if err == nil && empty {
		err = os.RemoveAll(partition.DataDir)
	}

	pm.mu.Lock()
	delete(pm.releasing, partitionID)
	pm.mu.Unlock()
	if err != nil {
		return fmt.Errorf("release partition %d: %w", partitionID, err)
	}
	log.Printf("[PARTITION] Partition %d released: this node no longer holds it (data removed=%v)", partitionID, empty)
	return nil
}

// StopAllPartitions stops all active partitions concurrently and gracefully.
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

// Close stops all partitions and shuts down the global fsync coalescer.
// It should be called once during application shutdown after all partitions
// have been drained.
func (pm *PartitionManager) Close() error {
	var errs []error
	if err := pm.StopAllPartitions(); err != nil {
		errs = append(errs, err)
	}
	if pm.fsyncCoalescer != nil {
		pm.fsyncCoalescer.Close()
		pm.fsyncCoalescer = nil
	}
	if len(errs) > 0 {
		return fmt.Errorf("partition manager close: %v", errs[0])
	}
	return nil
}

// GetStats returns aggregate partition manager statistics.
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
		if partition.IsLeader() {
			count++
		}
	}
	return count
}

// PartitionManagerStats holds aggregate counts for monitoring.
type PartitionManagerStats struct {
	// TotalPartitions is the number of local partitions currently loaded.
	TotalPartitions int64
	// LeaderPartitions is how many of those partitions this node leads.
	LeaderPartitions int64
	// ActivePartitions is currently equal to TotalPartitions (all loaded are active).
	ActivePartitions int64
}

// GetOrCreatePartition ensures a local partition exists (creating it if needed)
// for replication sync. The partition is not automatically started.
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

// SyncPartitionFromLeader syncs a partition from its leader via bulk snapshot
// install over the internal replication gRPC channel.
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
		partition.Follower.SetDeadLetterSink(func(records []*types.DeadLetterRecord) error {
			return partition.DLQ.ApplyReplica(partitionID, records)
		})
		// A snapshot of a log in lifecycle mode is installed only where the
		// cluster has committed that the partition requires the format. The
		// partition switches before the snapshot replaces its log, and its
		// index is rebuilt from the installed log; both run under ReplicateMu,
		// which is held for the install below.
		partition.Follower.SetLifecycleHooks(LifecycleFormat, func() error {
			if !pm.LifecycleRequired(partitionID) {
				return fmt.Errorf("partition %d does not require the lifecycle format on this node", partitionID)
			}
			return pm.EnableReplicatedLifecycle(partition)
		}, partition.rebuildLifecycleIndex)
		partition.Follower.SetTermStartHook(logcontrol.TermStartFormat, func() error {
			if !pm.TermStartRequired(partitionID) {
				return fmt.Errorf("partition %d does not require the term-start format", partitionID)
			}
			return partition.enableTermStart()
		})
	}
	if partition.LifecycleEnabled() || (pm.lifecycleRequired != nil && pm.lifecycleRequired(partitionID)) {
		partition.Follower.SetRecoveryHook(recoverystate.Format, partition.applyRecoverySnapshot)
	}
	pm.mu.Unlock()

	// Perform bulk snapshot install.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	partition.ReplicateMu.Lock()
	defer partition.ReplicateMu.Unlock()
	// The follower refuses a source that is behind the epoch accepted here.
	partition.Follower.SetLeader(fmt.Sprintf("leader-%d", partitionID), leaderAddr, partition.Epoch())
	if err := partition.Follower.InstallSnapshot(ctx, leaderAddr, partitionID, 0); err != nil {
		return fmt.Errorf("install snapshot from leader %s: %w", leaderAddr, err)
	}
	// This replica now holds a log written up to the source's epoch, and must
	// not take appends from an older leader after a restart either.
	if epoch := partition.Follower.GetEpoch(); epoch > partition.Epoch() {
		if err := partition.PersistEpoch(epoch); err != nil {
			return fmt.Errorf("record epoch %d of installed snapshot: %w", epoch, err)
		}
	}
	// The installed log starts where the source's does; what the source had
	// removed before that was finished.
	if partition.ConsumerGroup != nil {
		if err := partition.ConsumerGroup.SetLogStart(partitionID, partition.Wal.GetFirstOffset()); err != nil {
			return fmt.Errorf("apply log start of installed snapshot: %w", err)
		}
	}
	// And it ends where the source's did: nothing is finished beyond that.
	if err := partition.forgetBeyondLog(partition.Wal.GetNextOffset(), "the installed log ends there"); err != nil {
		return err
	}

	log.Printf("[PARTITION] Partition %d synced from leader %s", partitionID, leaderAddr)
	return nil
}

// PromoteToLeader promotes a local partition to leader and starts replication.
func (pm *PartitionManager) PromoteToLeader(partitionID int32, epoch int64) error {
	// Match incoming appends: ReplicateMu precedes pm.mu. This also serializes
	// term-start appends with ordinary publishes on an already-running leader.
	p, err := pm.GetOrCreateInternalPartition(partitionID, fmt.Sprintf("partition-%d", partitionID))
	if err != nil {
		return err
	}
	p.ReplicateMu.Lock()
	defer p.ReplicateMu.Unlock()
	pm.mu.Lock()
	defer pm.mu.Unlock()

	partition, exists := pm.partitions[partitionID]
	if !exists || partition != p {
		return fmt.Errorf("partition %d was released during promotion; retry", partitionID)
	}
	if epoch <= 0 {
		return fmt.Errorf("leadership epoch must be positive")
	}
	if pm.termStartRequired != nil && pm.termStartRequired(partitionID) {
		if err := partition.enableTermStart(); err != nil {
			return err
		}
	}
	if pm.lifecycleRequired != nil && pm.lifecycleRequired(partitionID) {
		if err := pm.EnableReplicatedLifecycle(partition); err != nil {
			return err
		}
	}
	// Claim the epoch for this node. If another node already holds it here,
	// this node may not lead until the cluster assigns a newer one.
	if err := partition.AcceptLeadership(epoch, pm.nodeID); err != nil {
		return err
	}
	if partition.ReplLeader != nil {
		partition.leader.Store(true)
		// Propagate the epoch into the replication leader so its outgoing Append
		// RPCs carry the true term. Without this the leader keeps the epoch it was
		// created with (1) forever, so a genuinely-stale leader and a new leader
		// both advertise term 1 and the follower's stale-term fence can't tell them
		// apart — the split-brain guard is defeated.
		partition.ReplLeader.SetEpoch(epoch)
		if err := partition.ensureTermStart(epoch); err != nil {
			partition.leader.Store(false)
			return err
		}
		return nil
	}

	// From here on this log is the partition's. What it does not hold was
	// never finished, whatever a former leader's progress said about offsets
	// this replica did not get to.
	if !partition.IsLeader() {
		if err := partition.forgetBeyondLog(partition.Wal.GetNextOffset(), "this replica leads from there"); err != nil {
			return err
		}
	}
	wasStarted := partition.started
	if !wasStarted {
		if err := pm.startPartitionInternal(partition); err != nil {
			return err
		}
	}
	if !partition.IsLeader() {
		// A start has walked the log for its timers, unless the partition is
		// replicated, and has recovered the dedup store either way: the walk and
		// the recovery are needed here only for a partition that was running.
		if wasStarted || partition.replicated {
			pm.replayWALTimers(partition)
		}
		if wasStarted {
			if err := pm.recoverDedupFromWAL(partition); err != nil {
				return err
			}
		}
		if err := partition.GetReplayError(); err != nil {
			return err
		}
	}
	if pm.config.ReplicationFactor <= 1 {
		partition.leader.Store(true)
		partition.wakeFeed()
		return nil
	}
	// The third argument is the reconnect tick, not the RPC deadline: 0 keeps
	// the leader's default so a dropped follower is redialed promptly.
	leader := replication.NewLeader(partitionID, int32(pm.config.ReplicationBatchSize), 0, partition.Wal, pm.config.MinInSyncReplicas, pm.nodeID, pm.replicationTLSConfig())
	leader.SetReplicateTimeout(pm.config.ReplicationTimeout)
	leader.SetEpoch(epoch) // advertise the real cluster epoch on the wire, not the default 1
	if partition.eventIndex() != nil {
		// Lifecycle records go to followers only through the versioned append.
		leader.SetLifecycleFormat(LifecycleFormat)
	}
	// Followers receive this group progress so a failover does not redeliver
	// what consumers already finished.
	consumerGroup := partition.ConsumerGroup
	leader.SetProgressPageSource(consumerGroup.ProgressVersion, func(cursor types.ProgressCursor) ([]*types.ConsumerGroupProgress, types.ProgressCursor, bool, error) {
		return consumerGroup.ExportProgressPage(partitionID, cursor)
	})
	leader.SetDeadLetterSource(partition.DLQ.Version, partition.DLQ.ExportPage)
	// A publish whose replication failed is finished once catch-up has taken
	// its events to a quorum, even if nobody retries it.
	leader.SetQuorumObserver(func(offset int64) {
		if err := partition.AcceptThrough(offset); err != nil {
			log.Printf("[PARTITION] Partition %d: accepting replicated publishes up to offset %d failed: %v", partitionID, offset, err)
		}
		partition.wakeFeed()
	})
	// Followers also learn how far the change feed has got, so that the one
	// that takes over continues it instead of repeating or skipping events.
	leader.SetChangeFeedSource(partition.ChangeFeedPosition)
	// A follower that answers for a newer term has a newer leader. This node
	// stops leading then, without waiting to hear of it from the cluster.
	leader.SetSupersededObserver(func(term int64) {
		go pm.stepDownFor(partitionID, leader, term)
	})
	partition.ReplLeader = leader
	if err := partition.ensureTermStart(epoch); err != nil {
		partition.ReplLeader = nil
		leader.Stop()
		return err
	}
	leader.Start()
	partition.replQuorum.Store(leader)
	partition.leader.Store(true)
	partition.wakeFeed()

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

// RetainFollowers stops a leading partition replicating to every follower
// whose ID is not in keep. A partition this node does not lead has none.
func (pm *PartitionManager) RetainFollowers(partitionID int32, keep []string) error {
	pm.mu.RLock()
	partition, exists := pm.partitions[partitionID]
	pm.mu.RUnlock()
	if !exists || partition.ReplLeader == nil {
		return nil
	}
	for _, id := range partition.ReplLeader.FollowerIDs() {
		if slices.Contains(keep, id) {
			continue
		}
		if err := partition.ReplLeader.RemoveFollower(id); err != nil {
			return fmt.Errorf("remove follower %s: %w", id, err)
		}
		log.Printf("[PARTITION] Follower %s removed from partition %d: it is no longer a replica", id, partitionID)
	}
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
	pm.demoteLocked(partition)
	return nil
}

// demoteLocked stops a partition leading. The caller holds pm.mu.
func (pm *PartitionManager) demoteLocked(partition *Partition) {
	if !partition.IsLeader() && partition.ReplLeader == nil {
		return // reconciliation calls this for every partition it does not lead
	}

	// Not leading comes first. What follows forgets which entries were never
	// accepted, and the change feed must see that this node stopped leading
	// before it can see that.
	partition.leader.Store(false)
	if partition.ReplLeader != nil {
		partition.ReplLeader.Stop()
		partition.ReplLeader = nil
	}
	partition.replQuorum.Store(nil)
	// The new leader decides what becomes of this node's unreplicated tail.
	partition.dropHeld()
	// Nothing is delivered from here until this node leads again, and then
	// the log is scheduled anew: it may not be the same log by then.
	if err := partition.resetSchedule(); err != nil {
		log.Printf("[PARTITION] Partition %d: clearing the schedule on demotion failed: %v", partition.ID, err)
	}

	log.Printf("[PARTITION] Partition %d demoted from leader", partition.ID)
}

// stepDownFor stops this node leading a partition after a follower answered
// leader for a newer term than this node's. The term is recorded first, which is what keeps
// this node from being promoted again under the one it had: the cluster's
// records here may go on naming it leader for a moment.
func (pm *PartitionManager) stepDownFor(partitionID int32, leader *replication.Leader, term int64) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	partition, exists := pm.partitions[partitionID]
	if !exists || partition.ReplLeader != leader {
		return // demoted, or promoted anew, in the meantime
	}
	if err := partition.PersistEpoch(term); err != nil {
		log.Printf("[PARTITION] Partition %d: recording term %d of the leader that replaced this node failed: %v", partitionID, term, err)
	}
	log.Printf("[PARTITION] Partition %d has a leader of term %d; this node stops leading it", partitionID, term)
	pm.demoteLocked(partition)
}

// ReplicaPosition describes where a replica's log ends.
type ReplicaPosition struct {
	// Found is false when the node holds no data for the partition.
	Found bool
	// LastOffset and LastTerm identify the last log entry (-1, 0 when empty).
	LastOffset int64
	LastTerm   int64
	// Epoch is the highest leadership epoch the replica has accepted.
	Epoch int64
	// AcceptingWrites is true while the node serves publishes as leader.
	AcceptingWrites bool
}

// SetWritableCheck supplies the cluster's view of whether this node may accept
// publishes for a partition. Without it every led partition counts as writable.
func (pm *PartitionManager) SetWritableCheck(writable func(partitionID int32) bool) {
	pm.mu.Lock()
	pm.writable = writable
	pm.mu.Unlock()
}

// LocalReplicaPosition reports this node's log position for a partition.
//
// AcceptingWrites is false only when the node could not extend the log any
// more: it does not lead the partition, or the cluster has stopped it from
// taking publishes and none is still in flight. Publishes announce themselves
// before they check writability, so that answer is final until the node is
// made writable again, and the position returned with it is where the log
// ends.
func (pm *PartitionManager) LocalReplicaPosition(partitionID int32) ReplicaPosition {
	pm.mu.RLock()
	partition, exists := pm.partitions[partitionID]
	writable := pm.writable
	pm.mu.RUnlock()
	if !exists && pm.partitionOnDisk(partitionID) {
		// After a restart a node holds its partitions on disk until something
		// uses them. Answering "nothing here" for those would let an election
		// pass over the most complete replica, and would hide the epoch a
		// restored replica has accepted.
		loaded, err := pm.GetOrCreateInternalPartition(partitionID, fmt.Sprintf("partition-%d", partitionID))
		if err != nil {
			log.Printf("[PARTITION] Partition %d is on disk but could not be loaded to report its position: %v", partitionID, err)
		} else {
			partition, exists = loaded, true
		}
	}
	if !exists || partition.Wal == nil {
		return ReplicaPosition{LastOffset: -1}
	}
	// Order matters: writability first, then publishes in flight.
	accepting := partition.IsLeader()
	if accepting && writable != nil && !writable(partitionID) {
		accepting = partition.publishing.Load() > 0
	}
	lastOffset, lastTerm := partition.LogPosition()
	return ReplicaPosition{
		Found:           true,
		LastOffset:      lastOffset,
		LastTerm:        lastTerm,
		Epoch:           partition.Epoch(),
		AcceptingWrites: accepting,
	}
}

// partitionOnDisk reports whether this node has a directory for the
// partition, whether or not it is loaded.
func (pm *PartitionManager) partitionOnDisk(partitionID int32) bool {
	info, err := os.Stat(fmt.Sprintf("%s/partitions/%d", pm.config.DataDir, partitionID))
	return err == nil && info.IsDir()
}

// FenceLocalReplica has this node's replica of a partition accept no leader
// of an epoch below epoch any more, and returns where its log ends once that
// holds. It is what an election asks of every replica it counts on.
//
// An election replaces a leader that the node deciding it cannot reach. That
// leader may be running all the same, and reach other replicas: it then goes
// on getting its publishes confirmed by them, and acknowledges them, until
// its successor's first request tells them of the new epoch. What it wrote in
// between is not in the log the election chose, and is removed. Recording the
// epoch here, before the position is read and under the lock that appends
// take, closes that: nothing of an older leader arrives after the answer.
//
// A replica without the partition is given an empty one, so that it has an
// epoch to hold: otherwise the old leader could still fill it and count it.
func (pm *PartitionManager) FenceLocalReplica(partitionID int32, epoch int64) (ReplicaPosition, error) {
	p, err := pm.GetOrCreateInternalPartition(partitionID, fmt.Sprintf("partition-%d", partitionID))
	if err != nil {
		return ReplicaPosition{}, err
	}
	p.ReplicateMu.Lock()
	defer p.ReplicateMu.Unlock()
	if epoch > p.Epoch() {
		if p.IsLeader() {
			// This node is the leader being replaced.
			if err := pm.DemoteFromLeader(partitionID); err != nil {
				return ReplicaPosition{}, fmt.Errorf("step down for epoch %d: %w", epoch, err)
			}
		}
		if err := p.PersistEpoch(epoch); err != nil {
			return ReplicaPosition{}, err
		}
	}
	return pm.LocalReplicaPosition(partitionID), nil
}

// FencedReplicaLogPosition is ReplicaLogPosition for an election: the replica
// first stops accepting leaders of an epoch below fenceEpoch (see
// FenceLocalReplica).
func (pm *PartitionManager) FencedReplicaLogPosition(ctx context.Context, addr string, partitionID int32, fenceEpoch int64) (found bool, lastOffset, lastTerm, epoch int64, acceptingWrites bool, err error) {
	var position ReplicaPosition
	if addr == "" {
		position, err = pm.FenceLocalReplica(partitionID, fenceEpoch)
	} else {
		position, err = pm.remoteReplicaPosition(ctx, addr, partitionID, fenceEpoch)
	}
	if err != nil {
		return false, -1, 0, 0, false, err
	}
	return position.Found, position.LastOffset, position.LastTerm, position.Epoch, position.AcceptingWrites, nil
}

// ReplicaLogPosition returns the log position of the replica on the node at
// addr, or of this node when addr is empty. It is what the cluster uses to
// elect the most complete replica and to check a handoff target.
func (pm *PartitionManager) ReplicaLogPosition(ctx context.Context, addr string, partitionID int32) (found bool, lastOffset, lastTerm, epoch int64, acceptingWrites bool, err error) {
	position := pm.LocalReplicaPosition(partitionID)
	if addr != "" {
		if position, err = pm.RemoteReplicaPosition(ctx, addr, partitionID); err != nil {
			return false, -1, 0, 0, false, err
		}
	}
	return position.Found, position.LastOffset, position.LastTerm, position.Epoch, position.AcceptingWrites, nil
}

// RemoteReplicaPosition asks the node at addr for its log position.
func (pm *PartitionManager) RemoteReplicaPosition(ctx context.Context, addr string, partitionID int32) (ReplicaPosition, error) {
	return pm.remoteReplicaPosition(ctx, addr, partitionID, 0)
}

func (pm *PartitionManager) remoteReplicaPosition(ctx context.Context, addr string, partitionID int32, fenceEpoch int64) (ReplicaPosition, error) {
	resp, err := replication.QueryPosition(ctx, addr, partitionID, fenceEpoch, pm.replicationTLSConfig())
	if err != nil {
		return ReplicaPosition{}, err
	}
	return ReplicaPosition{
		Found:           resp.GetFound(),
		LastOffset:      resp.GetLastOffset(),
		LastTerm:        resp.GetLastTerm(),
		Epoch:           resp.GetEpoch(),
		AcceptingWrites: resp.GetAcceptingWrites(),
	}, nil
}

// GetPartitionEpoch returns the cluster epoch for a partition.
func (pm *PartitionManager) GetPartitionEpoch(partitionID int32) int64 {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	if partition, exists := pm.partitions[partitionID]; exists {
		return partition.Epoch()
	}
	return 0
}

// ReplicationRequired reports whether publish acknowledgement needs an active
// local replication leader. A missing leader must never silently use the RF=1
// WAL fast path when replication is configured.
func (pm *PartitionManager) ReplicationRequired() bool {
	return pm.config.ReplicationFactor > 1 || pm.config.MinInSyncReplicas > 1
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

// GetPartitionInSyncReplicas returns the IDs of replicas currently in the ISR
// for a locally-led partition, including the leader itself.
func (pm *PartitionManager) GetPartitionInSyncReplicas(partitionID int32) []string {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	partition, exists := pm.partitions[partitionID]
	if !exists || partition == nil || partition.ReplLeader == nil {
		return nil
	}

	isr := []string{pm.nodeID}
	for _, id := range partition.ReplLeader.GetInSyncReplicas() {
		if id != pm.nodeID {
			isr = append(isr, id)
		}
	}
	return isr
}

// LeaderReport is what the leader of a partition knows of its replicas.
type LeaderReport struct {
	// PartitionID is the partition the report is about.
	PartitionID int32
	// Epoch is the leadership epoch the partition is held at.
	Epoch int64
	// InSyncReplicas lists the leader and the followers that are connected
	// and in sync.
	InSyncReplicas []string
	// ReplicaOffsets holds the end of the leader's log for the leader, and
	// what each follower has confirmed for the others.
	ReplicaOffsets map[string]int64
}

// NodeID returns the ID of the node this manager holds partitions for.
func (pm *PartitionManager) NodeID() string {
	return pm.nodeID
}

// LeaderReports returns a LeaderReport for every partition this node leads.
// The cluster state records which node leads a partition, but only that node
// knows which of the other replicas keep up with it.
func (pm *PartitionManager) LeaderReports() []LeaderReport {
	pm.mu.RLock()
	var reports []LeaderReport
	for id, p := range pm.partitions {
		if p != nil && p.IsLeader() {
			reports = append(reports, LeaderReport{PartitionID: id, Epoch: p.Epoch()})
		}
	}
	pm.mu.RUnlock()

	for i := range reports {
		report := &reports[i]
		report.ReplicaOffsets = pm.GetPartitionReplicaOffsets(report.PartitionID)
		report.InSyncReplicas = pm.GetPartitionInSyncReplicas(report.PartitionID)
		if len(report.InSyncReplicas) == 0 {
			// A partition without followers, at a replication factor of one.
			report.InSyncReplicas = []string{pm.nodeID}
		}
	}
	sort.Slice(reports, func(i, j int) bool { return reports[i].PartitionID < reports[j].PartitionID })
	return reports
}

// RemoteLeaderReports asks the node at addr for the LeaderReport of every
// partition it leads. It returns the node ID the reports came with.
func (pm *PartitionManager) RemoteLeaderReports(ctx context.Context, addr string) (string, []LeaderReport, error) {
	resp, err := replication.QueryLeaderReport(ctx, addr, pm.replicationTLSConfig())
	if err != nil {
		return "", nil, err
	}
	reports := make([]LeaderReport, 0, len(resp.GetPartitions()))
	for _, p := range resp.GetPartitions() {
		reports = append(reports, LeaderReport{
			PartitionID:    p.GetPartitionId(),
			Epoch:          p.GetEpoch(),
			InSyncReplicas: p.GetInSyncReplicas(),
			ReplicaOffsets: p.GetReplicaOffsets(),
		})
	}
	return resp.GetNodeId(), reports, nil
}

// PartitionLoadStatus exposes backpressure and queue-depth signals for a partition.
type PartitionLoadStatus struct {
	// PartitionID is the partition these metrics describe.
	PartitionID int32
	// ReadyQueueDepth is events waiting in the scheduler ready queue.
	ReadyQueueDepth int64
	// TimingWheelDepth is events still waiting in the timing wheel.
	TimingWheelDepth int64
	// InFlightDeliveries is active delivery attempts not yet acked/failed.
	InFlightDeliveries int64
	// DLQSize is the number of entries in the dead-letter queue.
	DLQSize int64
	// CanAccept is false when admission control would reject a new publish.
	CanAccept bool
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

// NewPartitionManagerWithAccessor creates a partition manager that implements
// PartitionAccessor (same as NewPartitionManager; kept for API clarity).
func NewPartitionManagerWithAccessor(nodeID string, config *types.Config) *PartitionManager {
	return NewPartitionManager(nodeID, config)
}

// GetDataDir returns the configured data directory, or "" if unset.
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

package partition

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/jatin711-debug/cronos_db_golang/internal/lifecycle"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
	"github.com/jatin711-debug/cronos_db_golang/pkg/utils"
)

// LifecycleFormat is the version of the lifecycle record format this build
// writes and reads, and puts in the lifecycle marker of a partition.
const LifecycleFormat = 1

// SetLifecycleRequirement gives the manager the cluster's record of which
// partitions require the lifecycle format.
func (pm *PartitionManager) SetLifecycleRequirement(required func(partitionID int32) bool) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	pm.lifecycleRequired = required
}

// LifecycleRequired reports whether the cluster has committed, as this node
// knows it, that a partition requires the lifecycle format. Lifecycle records
// are taken from replication, in appends and in snapshots, only then.
func (pm *PartitionManager) LifecycleRequired(partitionID int32) bool {
	pm.mu.RLock()
	required := pm.lifecycleRequired
	pm.mu.RUnlock()
	return required != nil && required(partitionID)
}

// rebuildLifecycleIndex rebuilds a partition's lifecycle index from its WAL,
// after a snapshot has replaced the WAL. Nothing to do for a partition not in
// lifecycle mode. The caller holds p.ReplicateMu.
func (p *Partition) rebuildLifecycleIndex() error {
	index := p.eventIndex()
	if index == nil {
		return nil
	}
	p.lifecycleMu.Lock()
	defer p.lifecycleMu.Unlock()
	return index.Rebuild(context.Background(), p.Wal)
}

// replicatedLifecycleAllowed reports why a partition of this node may not be
// in lifecycle mode in a cluster, or nil. Change feeds and experimental
// features have no contract for lifecycle records yet.
func (pm *PartitionManager) replicatedLifecycleAllowed() error {
	switch {
	case !pm.config.ClusterEnabled:
		return fmt.Errorf("replicated lifecycle mode needs cluster mode")
	case pm.changeFeed != nil:
		return fmt.Errorf("lifecycle records are not supported with a change feed")
	case pm.config.ExperimentalFeatures:
		return fmt.Errorf("lifecycle records are not supported with experimental features")
	}
	return nil
}

// EnableReplicatedLifecycle puts a partition of a cluster into lifecycle
// mode: from now on its WAL may hold lifecycle records, which an index keeps
// track of. It never switches back.
//
// The format marker is written durably first, so that the partition opens in
// lifecycle mode after a restart even if the switch goes no further; then the
// index is built from the WAL and published. Lifecycle records reach a
// partition of a cluster only after it has switched, so the index of the log
// it holds is empty; building it still reads the whole retained log once.
//
// The caller holds p.ReplicateMu, so that no replicated append runs while the
// partition switches.
func (pm *PartitionManager) EnableReplicatedLifecycle(p *Partition) error {
	if p.eventIndex() != nil {
		return nil
	}
	if err := pm.replicatedLifecycleAllowed(); err != nil {
		return err
	}
	marker := filepath.Join(p.DataDir, lifecycle.Marker)
	if err := utils.AtomicWriteFile(marker, []byte(fmt.Sprintf("%d\n", LifecycleFormat)), 0600); err != nil {
		return fmt.Errorf("write lifecycle marker: %w", err)
	}
	index, err := lifecycle.Open(p.DataDir, pm.pebbleCache)
	if err != nil {
		return fmt.Errorf("open lifecycle index: %w", err)
	}
	if err := index.Rebuild(context.Background(), p.Wal); err != nil {
		index.Close()
		return fmt.Errorf("build lifecycle index: %w", err)
	}
	p.lifecycleMu.Lock()
	p.lifecycleIdx.Store(index)
	p.lifecycleMu.Unlock()
	if leader := p.replQuorum.Load(); leader != nil {
		leader.SetLifecycleFormat(LifecycleFormat)
	}
	return nil
}

// IndexReplicatedLifecycle records in the index the lifecycle records of a
// replicated batch that has just been appended, after making the batch
// durable: a leader counts a cancellation or a reschedule as done only once a
// quorum holds it on disk, whatever the fsync mode of ordinary events. The
// records are tentative here, as every record on a follower is; what the
// leader accepted is settled when one of them leads. The caller holds
// p.ReplicateMu.
func (p *Partition) IndexReplicatedLifecycle(events []*types.Event) error {
	var records []*types.Event
	for _, e := range events {
		if lifecycle.HasReservedMeta(e) {
			records = append(records, e)
		}
	}
	if len(records) == 0 {
		return nil
	}
	index := p.eventIndex()
	if index == nil {
		err := fmt.Errorf("lifecycle records appended to partition %d, which is not in lifecycle mode", p.ID)
		p.Wal.Fail(err)
		return err
	}
	if err := p.Wal.Flush(); err != nil {
		return err
	}
	p.lifecycleMu.Lock()
	defer p.lifecycleMu.Unlock()
	for _, e := range records {
		if err := index.Apply(e); err != nil {
			p.Wal.Fail(fmt.Errorf("index replicated lifecycle record at offset %d: %w", e.Offset, err))
			return err
		}
	}
	return nil
}

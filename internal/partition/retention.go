package partition

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/jatin711-debug/cronos_db_golang/internal/storage"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

// PruneWAL is the common safety boundary for background and operator pruning.
// Replicated pruning is deferred until completion state and handoff watermarks
// are durable cluster-wide. Keeping data is safer than guessing that boundary.
func (p *Partition) PruneWAL(ctx context.Context, opts storage.PruneOptions) (int, error) {
	p.ReplicateMu.Lock()
	defer p.ReplicateMu.Unlock()
	if p.retentionBlocked || p.ReplLeader != nil {
		return 0, fmt.Errorf("WAL pruning is disabled for replicated partitions until durable completion and replica watermarks are available")
	}
	if p.ConsumerGroup == nil || p.Wal == nil {
		return 0, nil
	}
	return p.ConsumerGroup.WithRetentionCheck(func(eligible func(*types.Event) bool) (int, error) {
		return p.Wal.Prune(ctx, opts, eligible)
	})
}

// RemoveRetainedSegment lets the policy planner request deletion without ever
// unlinking files behind a live WAL. Unloaded partitions are preserved.
func (pm *PartitionManager) RemoveRetainedSegment(ctx context.Context, path string) (bool, error) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	abs, err := filepath.Abs(path)
	if err != nil {
		return false, err
	}
	for _, p := range pm.partitions {
		candidate, err := filepath.Abs(filepath.Join(p.DataDir, "segments", filepath.Base(path)))
		if err != nil {
			return false, err
		}
		if candidate != abs {
			continue
		}
		n, err := p.PruneWAL(ctx, storage.PruneOptions{Filename: filepath.Base(path)})
		return n > 0, err
	}
	return false, nil
}

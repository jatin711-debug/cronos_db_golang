package partition

import (
	"context"
	"fmt"
	"log"
	"path/filepath"

	"github.com/jatin711-debug/cronos_db_golang/internal/metrics"
	"github.com/jatin711-debug/cronos_db_golang/internal/storage"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

// How a partition gets rid of log entries.
//
// A segment may go when nothing needs any entry in it any more: every entry
// is due, at least one consumer group takes it and every group that does has
// finished it, and the change feed has exported it. That holds for a partition
// with one replica and with several.
//
// With several replicas two more things are settled here.
//
// Who decides. The leader does, from its own records. A follower's copy of
// the consumer progress trails the leader's, so a follower deciding for
// itself would keep entries the leader has dropped, and the logs of a
// partition would start at different places for no reason. A follower removes
// what the leader tells it to: every append names the offset the leader's log
// starts at (FollowLogStart).
//
// What may go. Only the oldest segments, so that the log stays one unbroken
// range of offsets. Replicas are compared, caught up and elected by where a
// log ends, which means nothing unless everything before the end is there.
// The price is that one entry that cannot go yet, an event scheduled far
// ahead for instance, keeps every later segment as well.
//
// A replica whose log ends before the leader's begins cannot be sent what it
// is missing. It does not need it either: those entries were finished before
// they were removed. It restarts its log at the leader's start. That is also
// how a replica that lost its disk gets back in.

// PruneWAL is the common safety boundary for background and operator pruning.
// It returns how many segments were removed.
func (p *Partition) PruneWAL(ctx context.Context, opts storage.PruneOptions) (int, error) {
	// Not ReplicateMu: publishes replicate under it, and a pass reads whole
	// segments.
	p.pruneMu.Lock()
	defer p.pruneMu.Unlock()
	if p.ConsumerGroup == nil || p.Wal == nil {
		return 0, nil
	}
	if p.replicated {
		if !p.IsLeader() {
			return 0, fmt.Errorf("partition %d is led by another node, which decides what is removed from its log", p.ID)
		}
		opts.PrefixOnly = true
	}
	// The change feed is one more reader that has to be done with an event
	// before it may go. Compaction removes a segment as soon as every consumer
	// group has finished it, which can be minutes after it was written; a sink
	// that was down for that long would otherwise never see those events.
	exported, feeding := p.ChangeFeedPosition()
	// Nothing above the accepted watermark goes: a replicated entry is only
	// known to be the partition's once it is on enough replicas.
	accepted := p.AcceptedThrough()

	deleted, err := p.ConsumerGroup.WithRetentionCheck(func(eligible func(*types.Event) bool) (int, error) {
		check := func(event *types.Event) bool {
			return (!p.replicated || event.Offset <= accepted) && (!feeding || event.Offset <= exported) && eligible(event)
		}
		if opts.PrefixOnly && p.pruneWaitsAt >= p.Wal.GetFirstOffset() {
			// The entry that stopped the last attempt is asked first. Without
			// this, a log that cannot be pruned yet is read up to that entry
			// again on every attempt.
			if waiting, err := p.Wal.ReadEvent(p.pruneWaitsAt); err == nil && !check(waiting) {
				return 0, nil
			}
		}
		p.pruneWaitsAt = -1
		return p.Wal.Prune(ctx, opts, func(event *types.Event) bool {
			if check(event) {
				return true
			}
			p.pruneWaitsAt = event.Offset
			return false
		})
	})
	if deleted > 0 {
		metrics.AddSegmentsRemoved(p.ID, deleted)
		if startErr := p.ConsumerGroup.SetLogStart(p.ID, p.Wal.GetFirstOffset()); startErr != nil && err == nil {
			err = startErr
		}
	}
	return deleted, err
}

// FollowLogStart brings this replica's log in line with where its leader's
// starts. The caller holds ReplicateMu and has accepted the sender as leader.
//
// Entries below start are removed, whole segments at a time. If this log ends
// before start, it is emptied and restarted there: the entries in between
// cannot be had any more, and were finished before the leader removed them.
// Either way they count as complete for every consumer group from here on,
// which is what keeps this replica from looking for them if it leads next.
func (p *Partition) FollowLogStart(start int64) error {
	if start <= 0 || start <= p.logStartSeen.Load() || p.Wal == nil {
		return nil
	}
	if p.IsLeader() {
		return nil // a leader answers to nobody about its own log
	}
	if next := p.Wal.GetNextOffset(); next < start {
		log.Printf("[Partition %d] This replica's log ends at offset %d and the leader's starts at %d; restarting it there", p.ID, next-1, start)
		if err := p.Wal.ResetTo(start); err != nil {
			return err
		}
		p.dropHeld()
	} else if dropped, err := p.Wal.DropBelow(start); err != nil {
		return err
	} else if dropped > 0 {
		metrics.AddSegmentsRemoved(p.ID, dropped)
		log.Printf("[Partition %d] Removed %d log segments below offset %d, which the leader has released", p.ID, dropped, start)
	}
	if p.ConsumerGroup != nil {
		if err := p.ConsumerGroup.SetLogStart(p.ID, start); err != nil {
			return err
		}
	}
	p.logStartSeen.Store(start)
	return nil
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
		if p.replicated && !p.IsLeader() {
			return false, nil // the leader decides; this replica follows it
		}
		n, err := p.PruneWAL(ctx, storage.PruneOptions{Filename: filepath.Base(path)})
		return n > 0, err
	}
	return false, nil
}

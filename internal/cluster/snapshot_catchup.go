package cluster

import (
	"context"
	"log"
	"slices"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// A follower that is far behind its leader is caught up by the leader, entry
// by entry, from where the follower's log ends; one whose log ends before
// the leader's log starts (it was away while the leader removed what it had
// delivered) first restarts its log there. Each batch is verified and synced
// on the way. A snapshot sends the leader's log as files instead: the sealed
// segments, the consumer progress and the dead letters, verified per file
// (Follower.InstallSnapshot). It sends the whole of the leader's log, though,
// so it pays only when the follower lacks most of it.
//
// Up to v0.13.1 nothing chose a snapshot for a follower that fell behind:
// joins, handoffs and moves used them, and --snapshot-catchup-threshold was
// read by nothing.

const (
	// snapshotCheckTicks is how many of leaderTasks' 5-second ticks pass
	// between two checks for followers that are better caught up from a
	// snapshot.
	snapshotCheckTicks = 6
	// snapshotRetryAfter is how long after a snapshot copy of a partition,
	// finished or failed, this node waits before it considers another.
	snapshotRetryAfter = 5 * time.Minute
)

var snapshotCatchups = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "cronos_replication_snapshot_catchups_total",
	Help: "Partitions this node, as a follower far behind, copied from their leader as a snapshot, by outcome",
}, []string{"outcome"})

// replicaLogRanger reports where the log of a replica starts and ends ("" is
// this node). The partition manager implements it.
type replicaLogRanger interface {
	ReplicaLogRange(ctx context.Context, addr string, partitionID int32) (found bool, start, last int64, err error)
}

// needsSnapshot decides whether a follower whose log ends at localLast copies
// its leader's log, which holds the offsets from leaderStart to leaderLast,
// instead of replaying what it lacks. Replay sends the entries after
// localLast, or all of the leader's log when the follower's ends before the
// leader's starts. A snapshot sends all of it. So the follower must lack at
// least threshold entries and at least half of the leader's log: a snapshot
// then sends at most twice what replay would, and sends it as files.
func needsSnapshot(localLast, leaderStart, leaderLast, threshold int64) bool {
	if threshold <= 0 || leaderLast < 0 {
		return false
	}
	leaderStart = max(leaderStart, 0)
	held := leaderLast - leaderStart + 1
	missing := leaderLast - max(localLast, leaderStart-1)
	return missing >= threshold && 2*missing >= held
}

// catchUpFromSnapshots has this node copy, as a snapshot, each partition it
// follows whose log is far behind its leader's (see needsSnapshot). At most
// one copy of a partition runs at a time, and another is considered only
// snapshotRetryAfter after the last.
func (m *Manager) catchUpFromSnapshots() {
	if !m.metadataCurrent() {
		return
	}
	m.mu.RLock()
	pa, rt, positioner := m.partitionAccessor, m.router, m.positions
	nodeID, threshold := m.config.NodeID, m.config.SnapshotCatchupThreshold
	replicated := m.config.ReplicationFactor > 1
	m.mu.RUnlock()
	ranger, ok := positioner.(replicaLogRanger)
	if !ok || pa == nil || rt == nil || !replicated || threshold <= 0 {
		return
	}
	alive := m.aliveNodes()
	for partitionID := range rt.DesiredAssignments() {
		committed, isCommitted := m.committed(partitionID)
		if !isCommitted || committed.LeaderID == nodeID || committed.TransferTo == nodeID || !slices.Contains(committed.Replicas, nodeID) {
			continue
		}
		leaderAddr := alive[committed.LeaderID]
		if leaderAddr == "" || !m.snapshotDue(partitionID) {
			continue
		}
		ctx, cancel := context.WithTimeout(m.ctx, positionQueryTimeout)
		found, _, localLast, err := ranger.ReplicaLogRange(ctx, "", partitionID)
		if err == nil && !found {
			localLast = -1
		}
		var leaderFound bool
		var leaderStart, leaderLast int64
		if err == nil {
			leaderFound, leaderStart, leaderLast, err = ranger.ReplicaLogRange(ctx, leaderAddr, partitionID)
		}
		cancel()
		if err != nil || !leaderFound || !needsSnapshot(localLast, leaderStart, leaderLast, threshold) {
			continue
		}
		log.Printf("[CLUSTER] catch-up: partition %d ends at offset %d here and its leader %s holds %d to %d; copying the leader's log as a snapshot",
			partitionID, localLast, committed.LeaderID, leaderStart, leaderLast)
		m.pullSnapshot(partitionID, leaderAddr)
	}
}

// snapshotDue reports whether no copy of the partition is running and the
// last one is at least snapshotRetryAfter ago.
func (m *Manager) snapshotDue(partitionID int32) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return !m.pulling[partitionID] && time.Since(m.snapshotAt[partitionID]) >= snapshotRetryAfter
}

// pullSnapshot copies a partition from its leader to this node in the
// background. Appends from the leader wait while the copy is installed, and
// resume from where the copy ends.
func (m *Manager) pullSnapshot(partitionID int32, leaderAddr string) {
	m.mu.Lock()
	if m.pulling == nil {
		m.pulling = make(map[int32]bool)
	}
	if m.pulling[partitionID] {
		m.mu.Unlock()
		return
	}
	m.pulling[partitionID] = true
	if m.snapshotAt == nil {
		m.snapshotAt = make(map[int32]time.Time)
	}
	m.snapshotAt[partitionID] = time.Now()
	pa := m.partitionAccessor
	m.mu.Unlock()

	go func() {
		defer func() {
			m.mu.Lock()
			delete(m.pulling, partitionID)
			m.mu.Unlock()
		}()
		start := time.Now()
		if err := pa.GetOrCreatePartition(partitionID); err != nil {
			snapshotCatchups.WithLabelValues("failed").Inc()
			log.Printf("[CLUSTER] catch-up: create partition %d: %v", partitionID, err)
			return
		}
		if err := pa.SyncPartitionFromLeader(partitionID, leaderAddr); err != nil {
			snapshotCatchups.WithLabelValues("failed").Inc()
			log.Printf("[CLUSTER] catch-up: copy partition %d from %s failed after %s: %v", partitionID, leaderAddr, time.Since(start).Round(time.Millisecond), err)
			return
		}
		snapshotCatchups.WithLabelValues("installed").Inc()
		log.Printf("[CLUSTER] catch-up: partition %d copied from %s in %s", partitionID, leaderAddr, time.Since(start).Round(time.Millisecond))
	}()
}

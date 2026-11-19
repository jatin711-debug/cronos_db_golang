package cluster

import (
	"context"
	"log"
	"slices"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/internal/replication"
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

// snapshotCatchUpRegistrar is implemented by a partition accessor whose
// replication handler can ask for a snapshot when an append shows a
// partition far behind (see requestSnapshot).
type snapshotCatchUpRegistrar interface {
	SetSnapshotCatchUp(threshold int64, request func(partitionID int32, leaderID string) bool)
}

// replicaLogRanger reports where the log of a replica starts and ends ("" is
// this node). The partition manager implements it.
type replicaLogRanger interface {
	ReplicaLogRange(ctx context.Context, addr string, partitionID int32) (found bool, start, last int64, err error)
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
		if err != nil || !leaderFound || !replication.NeedsSnapshot(localLast, leaderStart, leaderLast, threshold) {
			continue
		}
		log.Printf("[CLUSTER] catch-up: partition %d ends at offset %d here and its leader %s holds %d to %d; copying the leader's log as a snapshot",
			partitionID, localLast, committed.LeaderID, leaderStart, leaderLast)
		m.pullSnapshot(partitionID, leaderAddr)
	}
}

// checkSnapshotsInBackground runs catchUpFromSnapshots without holding up
// the caller: it asks the leaders where their logs are, which can take a
// while when one does not answer. One check runs at a time.
func (m *Manager) checkSnapshotsInBackground() {
	if !m.snapshotChecking.CompareAndSwap(false, true) {
		return
	}
	go func() {
		defer m.snapshotChecking.Store(false)
		m.catchUpFromSnapshots()
	}()
}

// requestSnapshot is called by the replication handler when an append from
// leaderID shows that this node's copy of the partition is far behind (see
// replication.NeedsSnapshot). It starts a copy unless one ran less than
// snapshotRetryAfter ago, and reports whether a copy is now running, in
// which case the handler turns the append away so that replay does not race
// the copy.
//
// The leader's address comes from membership, not from the committed
// assignments: those are not current yet right after a restart, which is
// when a node is most often far behind. The handler has already accepted
// leaderID as the partition's leader at a term this node has seen, and the
// copy is refused if its source is at an older epoch.
func (m *Manager) requestSnapshot(partitionID int32, leaderID string) bool {
	m.mu.RLock()
	running := m.pulling[partitionID]
	nodeID := m.config.NodeID
	m.mu.RUnlock()
	if running {
		return true
	}
	if leaderID == "" || leaderID == nodeID || !m.snapshotDue(partitionID) {
		return false
	}
	addr := m.aliveNodes()[leaderID]
	if addr == "" {
		return false
	}
	log.Printf("[CLUSTER] catch-up: an append from %s shows partition %d far behind here; copying the leader's log as a snapshot", leaderID, partitionID)
	m.pullSnapshot(partitionID, addr)
	return true
}

// snapshotDue reports whether no copy of the partition is running and the
// last one is at least snapshotRetryAfter ago.
func (m *Manager) snapshotDue(partitionID int32) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return !m.pulling[partitionID] && time.Since(m.snapshotAt[partitionID]) >= snapshotRetryAfter
}

// pullSnapshot copies a partition from its leader to this node in the
// background. SyncPartitionFromLeader holds the partition's replication lock
// from before it asks for the snapshot until the snapshot is installed, so no
// append lands meanwhile, and the leader cuts the snapshot after the lock is
// taken: the installed log ends no earlier than the one it replaces, and no
// entry this node acknowledged is lost. Appends resume from where it ends.
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

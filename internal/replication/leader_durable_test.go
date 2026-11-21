package replication

import (
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/internal/storage"
)

// A publish sends its entries to the followers while the leader's own disk
// still works on them, so a follower can acknowledge an entry first. The
// leader is one of the replicas a quorum counts: until the entry is durable in
// its own log, one acknowledgement is one replica, not two, and the entry is
// neither on a quorum nor to be handed to consumers. If it were, and the
// leader then lost power, the follower alone would hold an entry consumers had
// been given, and an election between the other two replicas would drop it.
func TestLeader_QuorumCountsTheLeaderOnlyUpToItsDurableLog(t *testing.T) {
	for _, mode := range []string{"every_event", "batch"} {
		t.Run(mode, func(t *testing.T) {
			wal, err := storage.NewWAL(t.TempDir(), 0, &storage.WALConfig{SegmentSizeBytes: 1 << 20, IndexInterval: 1, FsyncMode: mode}, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer wal.Close()
			follower, addr := startGatedReplServer(t, true)

			// Three replicas, two required: the leader and one follower.
			l := NewLeader(0, 500, time.Hour, wal, 2, "leader", nil)
			defer l.Stop()
			if err := l.AddFollower("follower", addr); err != nil {
				t.Fatal(err)
			}

			// Offsets 0-1, durable on both.
			first := batchAt(0, 2)
			for _, e := range first {
				e.ScheduleTs = 1
			}
			if err := wal.AppendBatch(first); err != nil {
				t.Fatal(err)
			}
			if err := l.Replicate(first); err != nil {
				t.Fatal(err)
			}
			if got := l.QuorumOffset(); got != 1 {
				t.Fatalf("QuorumOffset = %d with offsets 0-1 durable on both, want 1", got)
			}

			// Offsets 2-4: in the leader's log, acknowledged by the follower,
			// and not yet durable on the leader.
			second := batchAt(2, 3)
			for _, e := range second {
				e.ScheduleTs = 1
			}
			pending, err := wal.AppendBatchDeferred(second)
			if err != nil {
				t.Fatal(err)
			}
			if err := l.Replicate(second); err != nil {
				t.Fatal(err)
			}
			if offsets, _ := follower.snapshot(); len(offsets) != 5 {
				t.Fatalf("follower holds %v, want offsets 0-4", offsets)
			}
			if got := l.QuorumOffset(); got != 1 {
				t.Fatalf("QuorumOffset = %d while offsets 2-4 are durable on the follower alone, want 1", got)
			}
			if got := l.ReplicatedThrough(); got != 1 {
				t.Fatalf("ReplicatedThrough = %d while offsets 2-4 are durable on the follower alone, want 1", got)
			}

			if err := pending.Wait(); err != nil {
				t.Fatal(err)
			}
			if got := l.QuorumOffset(); got != 4 {
				t.Fatalf("QuorumOffset = %d once the leader's log is durable too, want 4", got)
			}
			if got := l.ReplicatedThrough(); got != 4 {
				t.Fatalf("ReplicatedThrough = %d once the leader's log is durable too, want 4", got)
			}
		})
	}
}

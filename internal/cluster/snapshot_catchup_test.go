package cluster

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/internal/replication"
)

func (f *fakePositions) ReplicaLogRange(_ context.Context, addr string, _ int32) (bool, int64, int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	node := addr
	if addr == "" {
		node = f.local
	}
	if f.unreachable[node] {
		return false, 0, -1, fmt.Errorf("node %s unreachable", node)
	}
	p, ok := f.byNode[node]
	if !ok {
		return false, 0, -1, nil
	}
	return p.Found, f.starts[node], p.LastOffset, nil
}

func TestNeedsSnapshot(t *testing.T) {
	for _, tc := range []struct {
		name                                    string
		localLast, leaderStart, leaderLast, thr int64
		want                                    bool
	}{
		{"caught up", 999, 0, 999, 100, false},
		{"a little behind", 900, 0, 999, 100, false},
		{"behind by less than half the leader's log", 600, 0, 999, 100, false},
		{"behind by half of it", 499, 0, 999, 100, true},
		{"empty follower", -1, 0, 999, 100, true},
		{"empty follower, leader log under the threshold", -1, 0, 50, 100, false},
		{"log ends before the leader's starts", 100, 5000, 5999, 100, true},
		{"ahead of the leader", 1200, 0, 999, 100, false},
		{"disabled", -1, 0, 999, 0, false},
		{"empty leader", -1, 0, -1, 100, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := replication.NeedsSnapshot(tc.localLast, tc.leaderStart, tc.leaderLast, tc.thr); got != tc.want {
				t.Fatalf("NeedsSnapshot(%d, %d, %d, %d) = %v, want %v", tc.localLast, tc.leaderStart, tc.leaderLast, tc.thr, got, tc.want)
			}
		})
	}
}

// waitPulls waits until no copy of partition 0 runs on the fixture's node.
func (f *leadershipFixture) waitPulls(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		f.manager.mu.RLock()
		pulling := f.manager.pulling[0]
		f.manager.mu.RUnlock()
		if !pulling {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("the copy did not finish")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// A follower whose log ends before its leader's starts copies the leader's
// log, once; a follower only a little behind is left to replay.
func TestFollowerFarBehindCopiesItsLeadersLog(t *testing.T) {
	f := newLeadershipFixture(t, "node-2", "node-1", 3, 2, "node-1", "node-2", "node-3")
	f.manager.config.SnapshotCatchupThreshold = 1000
	f.positions.starts = map[string]int64{"node-1": 50_000}
	f.positions.set("node-1", ReplicaPosition{LastOffset: 59_999, AcceptingWrites: true})

	f.positions.set("node-2", ReplicaPosition{LastOffset: 59_500})
	f.manager.catchUpFromSnapshots()
	f.waitPulls(t)
	if _, copied := f.accessor.syncCalls[0]; copied {
		t.Fatal("a follower 500 entries behind copied its leader's log")
	}

	f.positions.set("node-2", ReplicaPosition{LastOffset: 1_000})
	f.manager.catchUpFromSnapshots()
	f.waitPulls(t)
	if addr := f.accessor.syncCalls[0]; addr != "node-1" {
		t.Fatalf("a follower whose log ends before its leader's starts copied from %q, want node-1", addr)
	}

	// Not again before snapshotRetryAfter, whatever its position says.
	delete(f.accessor.syncCalls, 0)
	f.manager.catchUpFromSnapshots()
	f.waitPulls(t)
	if _, copied := f.accessor.syncCalls[0]; copied {
		t.Fatal("copied again right after the last copy")
	}
}

// The leader, a node that holds no replica, and a node with the threshold at
// 0 copy nothing.
func TestOnlyFollowersCopyFromSnapshots(t *testing.T) {
	for name, setup := range map[string]func(f *leadershipFixture){
		"leader": func(f *leadershipFixture) {},
		"threshold 0": func(f *leadershipFixture) {
			f.manager.config.SnapshotCatchupThreshold = 0
		},
	} {
		t.Run(name, func(t *testing.T) {
			self := "node-2"
			if name == "leader" {
				self = "node-1"
			}
			f := newLeadershipFixture(t, self, "node-1", 3, 2, "node-1", "node-2", "node-3")
			f.manager.config.SnapshotCatchupThreshold = 1000
			f.positions.starts = map[string]int64{"node-1": 50_000}
			f.positions.set("node-1", ReplicaPosition{LastOffset: 59_999, AcceptingWrites: true})
			f.positions.set("node-2", ReplicaPosition{LastOffset: -1})
			setup(f)
			f.manager.catchUpFromSnapshots()
			f.waitPulls(t)
			if _, copied := f.accessor.syncCalls[0]; copied {
				t.Fatalf("%s copied partition 0", name)
			}
		})
	}
}

// An append that shows a partition far behind asks for a copy from the node
// that sent it, once: a second request while the copy runs, or within
// snapshotRetryAfter of it, starts none.
func TestAppendFarBehindRequestsACopy(t *testing.T) {
	f := newLeadershipFixture(t, "node-2", "node-1", 3, 2, "node-1", "node-2", "node-3")
	f.manager.config.SnapshotCatchupThreshold = 1000
	if !f.manager.requestSnapshot(0, "node-1") {
		t.Fatal("no copy started for a far-behind partition")
	}
	f.waitPulls(t)
	if addr := f.accessor.syncCalls[0]; addr != "node-1" {
		t.Fatalf("copied from %q, want node-1", addr)
	}
	delete(f.accessor.syncCalls, 0)
	if f.manager.requestSnapshot(0, "node-1") {
		t.Fatal("a copy started again right after the last one")
	}
	if f.manager.requestSnapshot(1, "node-2") || f.manager.requestSnapshot(2, "") {
		t.Fatal("a copy started from this node itself or from no node")
	}
}

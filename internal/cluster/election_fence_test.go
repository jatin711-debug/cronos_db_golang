package cluster

import (
	"context"
	"testing"
)

// fencingPositions answers position queries as fakePositions does and
// records which replicas an election fenced, at which epoch, and what was
// committed for the partition when the first of them was asked.
type fencingPositions struct {
	*fakePositions
	fenced       map[string]int64
	committedAt  func() (PartitionInfo, bool)
	whenFirstAsk *PartitionInfo
}

func (f *fencingPositions) FencedReplicaLogPosition(ctx context.Context, addr string, partitionID int32, fenceEpoch int64) (bool, int64, int64, int64, bool, error) {
	found, lastOffset, lastTerm, epoch, accepting, err := f.fakePositions.ReplicaLogPosition(ctx, addr, partitionID)
	if err != nil {
		return found, lastOffset, lastTerm, epoch, accepting, err
	}
	node := addr
	if addr == "" {
		node = f.local
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.whenFirstAsk == nil {
		if info, ok := f.committedAt(); ok {
			f.whenFirstAsk = &info
		}
	}
	f.fenced[node] = fenceEpoch
	// A fenced replica holds the fence's epoch from then on.
	return found, lastOffset, lastTerm, max(epoch, fenceEpoch), accepting, nil
}

func withFencing(f *leadershipFixture) *fencingPositions {
	fencing := &fencingPositions{
		fakePositions: f.positions,
		fenced:        map[string]int64{},
		committedAt:   func() (PartitionInfo, bool) { return f.store.Partition(0) },
	}
	f.manager.positions = fencing
	return fencing
}

// The node that decides an election cannot reach the leader it replaces, and
// that says nothing about the other replicas: the leader may be running and
// still have its writes confirmed by them. An election therefore records
// first that the partition has no leader, at a new epoch, and has every
// replica it asks stop accepting leaders below that epoch before it answers.
// The leader it then commits holds a higher one.
//
// Without this, what the old leader had confirmed between a replica's answer
// and the new leader's first request was acknowledged and then removed.
func TestElection_FencesTheReplicasItAsksBeforeItChooses(t *testing.T) {
	f := newLeadershipFixture(t, "node-2", "node-1", 3, 2, "node-1", "node-2", "node-3")
	fencing := withFencing(f)
	f.positions.set("node-2", ReplicaPosition{LastTerm: 1, LastOffset: 10, Epoch: 1})
	f.positions.set("node-3", ReplicaPosition{LastTerm: 1, LastOffset: 12, Epoch: 1})
	delete(f.membership.nodes, "node-1") // unreachable from here; perhaps not from node-3
	before := f.committed(t)

	f.manager.checkPartitionHealth()

	if fencing.whenFirstAsk == nil {
		t.Fatal("no replica was asked through the fencing query")
	}
	if at := *fencing.whenFirstAsk; at.LeaderID != "" || at.Epoch <= before.Epoch {
		t.Fatalf("when the first replica was asked, leader %q at epoch %d was committed; want no leader at an epoch above %d",
			at.LeaderID, at.Epoch, before.Epoch)
	}
	vacancy := fencing.whenFirstAsk.Epoch
	for _, node := range []string{"node-2", "node-3"} {
		if got := fencing.fenced[node]; got != vacancy {
			t.Errorf("%s was fenced at epoch %d, want %d, the epoch the partition has no leader at", node, got, vacancy)
		}
	}
	if _, asked := fencing.fenced["node-1"]; asked {
		t.Error("the leader that is counted dead was asked")
	}

	after := f.committed(t)
	if after.LeaderID != "node-3" {
		t.Fatalf("elected %q, want node-3, whose log is the longest", after.LeaderID)
	}
	if after.Epoch <= vacancy {
		t.Fatalf("the new leader holds epoch %d, which the replicas were fenced at; it needs a higher one than %d", after.Epoch, vacancy)
	}
}

// Once a partition is recorded as having no leader, an election is owed until
// one is committed: on the next round, by whichever node decides then, and
// whether or not the old leader is counted alive again. Left to "the leader
// is alive, nothing to do", a partition whose replicas had been fenced would
// have stayed with a leader that none of them accepts.
func TestElection_IsOwedUntilItIsDecided(t *testing.T) {
	f := newLeadershipFixture(t, "node-2", "node-1", 3, 2, "node-1", "node-2", "node-3")
	fencing := withFencing(f)
	f.positions.set("node-2", ReplicaPosition{LastTerm: 1, LastOffset: 10, Epoch: 1})
	f.positions.unreachable["node-3"] = true
	oldLeader := f.membership.nodes["node-1"]
	delete(f.membership.nodes, "node-1")
	before := f.committed(t)

	// One answer of the two needed: nobody is elected, and it is on record.
	f.manager.checkPartitionHealth()
	vacant := f.committed(t)
	if vacant.LeaderID != "" || vacant.Epoch <= before.Epoch {
		t.Fatalf("after an election that could not be decided: leader %q at epoch %d; want no leader at an epoch above %d",
			vacant.LeaderID, vacant.Epoch, before.Epoch)
	}
	if f.manager.PartitionHasLeader(0) {
		t.Fatal("the partition is reported to have a leader")
	}
	if got := fencing.fenced["node-2"]; got != vacant.Epoch {
		t.Fatalf("node-2 was fenced at epoch %d, want %d", got, vacant.Epoch)
	}

	// The old leader is back, with the longest log; node-3 still is not.
	f.membership.nodes["node-1"] = oldLeader
	f.positions.set("node-1", ReplicaPosition{LastTerm: 1, LastOffset: 15, Epoch: 1})
	proposals := f.store.proposals

	f.manager.checkPartitionHealth()

	after := f.committed(t)
	if after.LeaderID != "node-1" {
		t.Fatalf("leader = %q, want node-1: two replicas answered and its log is the longest", after.LeaderID)
	}
	if after.Epoch <= vacant.Epoch {
		t.Fatalf("epoch %d did not rise above %d, at which the replicas were fenced", after.Epoch, vacant.Epoch)
	}
	if got := fencing.fenced["node-1"]; got != vacant.Epoch {
		t.Fatalf("the returning leader was fenced at epoch %d, want %d: it is a candidate like the others", got, vacant.Epoch)
	}
	if got := f.store.proposals - proposals; got != 1 {
		t.Fatalf("%d proposals to fill a recorded vacancy, want 1", got)
	}

	// Decided: later rounds leave it alone.
	proposals = f.store.proposals
	f.manager.checkPartitionHealth()
	if got := f.store.proposals - proposals; got != 0 {
		t.Fatalf("%d proposals for a partition whose leader is alive", got)
	}
}

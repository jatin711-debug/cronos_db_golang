package cluster

import (
	"fmt"
	"slices"
	"testing"
)

// joinNodes adds alive nodes to the fixture's cluster and has the ring place
// partition 0 on replicas, with node-1 leading.
func joinNodes(f *leadershipFixture, replicas []string, nodes ...string) {
	for _, id := range nodes {
		f.membership.nodes[id] = &Node{ID: id, Address: id, State: NodeStateAlive}
	}
	f.manager.router.UpdatePartitionAssignment(0, "node-1", replicas, nil)
}

// A node joins and the ring moves a replica from node-3 to it. Writes 91..100
// were acknowledged by the leader and node-3 (minISR=2), and node-2 is behind.
// Placement used to drop node-3 at once, and when the leader then failed the
// election asked only node-2 and the new, empty node-4, and elected node-2.
func TestNodeJoinKeepsTheDroppedReplicaInElections(t *testing.T) {
	f := newLeadershipFixture(t, "node-2", "node-1", 3, 2, "node-1", "node-2", "node-3")
	joinNodes(f, []string{"node-1", "node-2", "node-4"}, "node-4")
	f.positions.set("node-1", ReplicaPosition{LastTerm: 1, LastOffset: 100, Epoch: 1, AcceptingWrites: true})
	f.positions.set("node-3", ReplicaPosition{LastTerm: 1, LastOffset: 100, Epoch: 1})
	f.positions.set("node-2", ReplicaPosition{LastTerm: 1, LastOffset: 90, Epoch: 1})
	f.positions.set("node-4", ReplicaPosition{LastTerm: 0, LastOffset: -1})

	f.manager.syncClusterState()
	got := f.committed(t)
	if fmt.Sprint(got.Replicas) != "[node-1 node-2 node-4]" || fmt.Sprint(got.Retiring) != "[node-3]" {
		t.Fatalf("after the join: replicas %v, retiring %v; want [node-1 node-2 node-4] and [node-3]", got.Replicas, got.Retiring)
	}

	delete(f.membership.nodes, "node-1")
	f.manager.checkPartitionHealth()
	after := f.committed(t)
	if after.LeaderID != "node-3" {
		t.Fatalf("elected %q; node-3 holds the acknowledged offsets 91..100", after.LeaderID)
	}
	if !slices.Contains(after.Replicas, "node-3") || slices.Contains(after.Retiring, "node-3") {
		t.Fatalf("the elected node is not a replica: replicas %v, retiring %v", after.Replicas, after.Retiring)
	}
}

// Two nodes join and the ring moves two of the three replicas. The election
// used to choose between the two new, empty replicas and lose the whole log.
func TestTwoJoinsDoNotElectAnEmptyReplica(t *testing.T) {
	f := newLeadershipFixture(t, "node-2", "node-1", 3, 2, "node-1", "node-2", "node-3")
	joinNodes(f, []string{"node-1", "node-4", "node-5"}, "node-4", "node-5")
	for _, id := range []string{"node-1", "node-2", "node-3"} {
		f.positions.set(id, ReplicaPosition{LastTerm: 1, LastOffset: 100, Epoch: 1})
	}
	f.positions.set("node-4", ReplicaPosition{LastTerm: 0, LastOffset: -1})
	f.positions.set("node-5", ReplicaPosition{LastTerm: 0, LastOffset: -1})

	f.manager.syncClusterState()
	if got := f.committed(t); fmt.Sprint(got.Retiring) != "[node-2 node-3]" {
		t.Fatalf("retiring %v, want [node-2 node-3]", got.Retiring)
	}
	delete(f.membership.nodes, "node-1")
	f.manager.checkPartitionHealth()
	if leader := f.committed(t).LeaderID; leader != "node-2" && leader != "node-3" {
		t.Fatalf("elected %q while node-2 and node-3 hold offsets 0..100", leader)
	}
}

// With the leader and a retiring replica both gone, the replicas left cannot
// show that none of them lacks an acknowledged write, and nobody is elected.
func TestNoElectionWithoutEnoughOfTheRetiringReplicas(t *testing.T) {
	f := newLeadershipFixture(t, "node-2", "node-1", 3, 2, "node-1", "node-2", "node-3")
	joinNodes(f, []string{"node-1", "node-2", "node-4"}, "node-4")
	f.positions.set("node-1", ReplicaPosition{LastTerm: 1, LastOffset: 100, Epoch: 1})
	f.positions.set("node-2", ReplicaPosition{LastTerm: 1, LastOffset: 90, Epoch: 1})
	f.positions.set("node-4", ReplicaPosition{LastTerm: 0, LastOffset: -1})
	f.manager.syncClusterState()

	delete(f.membership.nodes, "node-1")
	delete(f.membership.nodes, "node-3")
	f.manager.checkPartitionHealth()
	if leader := f.committed(t).LeaderID; leader != "" {
		t.Fatalf("elected %q although node-3, which may hold acknowledged writes, did not answer", leader)
	}
}

// A retiring replica is released once the leader reports the assignment that
// dropped it, and the replicas that stay hold what the leader held then.
func TestRetiredReplicaIsReleasedOnceTheReplicasHoldTheLog(t *testing.T) {
	f := newLeadershipFixture(t, "node-2", "node-1", 3, 2, "node-1", "node-2", "node-3")
	joinNodes(f, []string{"node-1", "node-2", "node-4"}, "node-4")
	f.positions.set("node-1", ReplicaPosition{LastTerm: 1, LastOffset: 100, Epoch: 1, AcceptingWrites: true})
	f.positions.set("node-2", ReplicaPosition{LastTerm: 1, LastOffset: 90, Epoch: 1})
	f.positions.set("node-3", ReplicaPosition{LastTerm: 1, LastOffset: 100, Epoch: 1})
	f.positions.set("node-4", ReplicaPosition{LastTerm: 0, LastOffset: -1})
	f.manager.syncClusterState()
	epoch := f.committed(t).Epoch

	// The leader has not taken up the new assignment: it may still count
	// node-3's acknowledgements.
	f.positions.set("node-2", ReplicaPosition{LastTerm: 1, LastOffset: 200, Epoch: epoch})
	f.manager.syncClusterState()
	f.manager.syncClusterState()
	if got := f.committed(t); len(got.Retiring) == 0 {
		t.Fatal("node-3 was released before the leader had stopped replicating to it")
	}

	// It has, at offset 120. node-2 is behind that, then reaches it.
	f.positions.set("node-1", ReplicaPosition{LastTerm: 1, LastOffset: 120, Epoch: epoch, AcceptingWrites: true})
	f.positions.set("node-2", ReplicaPosition{LastTerm: 1, LastOffset: 110, Epoch: epoch})
	f.manager.syncClusterState() // takes the leader's position
	f.manager.syncClusterState()
	if got := f.committed(t); len(got.Retiring) == 0 {
		t.Fatal("node-3 was released before a replica held the leader's log")
	}
	f.positions.set("node-2", ReplicaPosition{LastTerm: 1, LastOffset: 125, Epoch: epoch})
	f.manager.syncClusterState()
	if got := f.committed(t); len(got.Retiring) != 0 {
		t.Fatalf("node-3 is still retiring once node-2 holds the leader's log: %v", got.Retiring)
	}
}

type pruningAccessor struct {
	*MockPartitionAccessor
	calls []string
}

func (a *pruningAccessor) RetainFollowers(partitionID int32, keep []string) error {
	a.calls = append(a.calls, fmt.Sprintf("retain %v", keep))
	return nil
}

func (a *pruningAccessor) PromoteToLeader(partitionID int32, epoch int64) error {
	a.calls = append(a.calls, fmt.Sprintf("promote %d", epoch))
	return a.MockPartitionAccessor.PromoteToLeader(partitionID, epoch)
}

// The leader stops replicating to former replicas before it takes up the
// assignment that dropped them.
func TestLeaderDropsFormerFollowersBeforeTakingUpTheEpoch(t *testing.T) {
	f := newLeadershipFixture(t, "node-1", "node-1", 3, 2, "node-1", "node-2", "node-3")
	accessor := &pruningAccessor{MockPartitionAccessor: f.accessor}
	f.manager.partitionAccessor = accessor
	joinNodes(f, []string{"node-1", "node-2", "node-4"}, "node-4")
	for _, id := range []string{"node-1", "node-2", "node-3", "node-4"} {
		f.positions.set(id, ReplicaPosition{LastTerm: 1, LastOffset: 10, Epoch: 1})
	}
	f.manager.syncClusterState()
	epoch := f.committed(t).Epoch

	f.manager.reconcileLocalLeadership()
	want := []string{"retain [node-1 node-2 node-4]", fmt.Sprintf("promote %d", epoch)}
	if !slices.Equal(accessor.calls, want) {
		t.Fatalf("calls %v, want %v", accessor.calls, want)
	}
}

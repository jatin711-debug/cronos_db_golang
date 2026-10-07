package cluster

import (
	"testing"
	"time"
)

func contains(nodes []string, id string) bool {
	for _, node := range nodes {
		if node == id {
			return true
		}
	}
	return false
}

// An election changes who leads a partition. It must not change where the
// ring wants the partition's replicas.
//
// It used to write the replica list it had started from over the ring's
// entry. When a node had come back in the meantime, the ring's entry was the
// only place that said the node belongs to the partition again, and with it
// gone nothing ever added the node back: the partition ran on one replica,
// and refused publishes for want of a second, until some other node joined or
// left.
func TestElection_KeepsWhereTheRingWantsTheReplicas(t *testing.T) {
	f := newLeadershipFixture(t, "node-3", "node-1", 3, 2, "node-1", "node-2", "node-3")
	// node-2 was away, and the partition was committed without it.
	without := f.committed(t)
	without.Replicas, without.ISR = []string{"node-1", "node-3"}, []string{"node-1", "node-3"}
	if err := f.store.ProposePartition(&without, false); err != nil {
		t.Fatal(err)
	}
	// It is back, and the ring places the partition on all three again.
	f.manager.router.UpdatePartitionAssignment(0, "node-2", []string{"node-2", "node-3", "node-1"}, nil)
	f.positions.set("node-2", ReplicaPosition{LastTerm: 1, LastOffset: 40})
	f.positions.set("node-3", ReplicaPosition{LastTerm: 1, LastOffset: 90})

	delete(f.membership.nodes, "node-1") // the leader dies before the placement is committed
	f.manager.checkPartitionHealth()
	if got := f.committed(t); got.LeaderID != "node-3" {
		t.Fatalf("elected %q, want node-3, the one replica left", got.LeaderID)
	}
	if wanted := f.manager.router.DesiredAssignments()[0].Replicas; !contains(wanted, "node-2") {
		t.Fatalf("after the election the ring wants the replicas on %v: node-2 is forgotten", wanted)
	}

	f.manager.syncClusterState()
	if got := f.committed(t); !contains(got.Replicas, "node-2") {
		t.Fatalf("the partition stays on %v: the node that came back is never made a replica again", got.Replicas)
	}
	f.manager.reconcileLocalLeadership()
	if f.accessor.followerCalls[0] == "" {
		t.Fatal("the new leader does not replicate to the node that came back")
	}
	// The election stands: the leader it chose is not handed off at once to
	// the node the ring would have preferred.
	if got := f.committed(t); got.LeaderID != "node-3" || got.TransferTo != "" {
		t.Fatalf("leader %q, handing off to %q; want node-3 to keep the partition", got.LeaderID, got.TransferTo)
	}
}

// A node that was counted as failed and then announces itself again is back,
// and those who were told that it failed must be told that too. The router
// takes a failed node out of the ring; it used to hear nothing of the return
// and went on without the node until its next periodic comparison, up to ten
// seconds later. A new Raft leader in that time committed every partition
// without the node.
func TestMembership_ReturningNodeIsPutBackInTheRing(t *testing.T) {
	m, err := NewMembership(&ClusterConfig{NodeID: "node-a", BindAddr: "node-a:1", HeartbeatInterval: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	router := NewRouter(m, 1, 2, 16, nil)
	router.Start()
	inRing := func() bool { return contains(router.DesiredAssignments()[0].Replicas, "node-b") }
	announce := func() {
		if err := m.Join(&Node{ID: "node-b", Address: "node-b:2", GossipAddr: "node-b:1", State: NodeStateAlive, UpdatedAt: time.Now()}); err != nil {
			t.Fatal(err)
		}
	}

	announce()
	eventually(t, "the new node is in the ring", inRing)
	if err := m.MarkDead("node-b"); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the failed node is out of the ring", func() bool { return !inRing() })

	announce()
	// Well within the periodic comparison, which would hide a missing event.
	deadline := time.Now().Add(ringReconcileInterval / 4)
	for !inRing() {
		if time.Now().After(deadline) {
			t.Fatal("the node that came back is not in the ring")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

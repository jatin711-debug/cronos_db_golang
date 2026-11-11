package cluster

import "testing"

// A node that lost its disk and was started again under its name, before the
// others had seen it go, is still the committed leader of what it led. It
// must not lead with its empty log: its followers would remove everything
// they hold to match it.
func TestTakeUp_NodeThatLostItsLogDoesNotLead(t *testing.T) {
	f := newLeadershipFixture(t, "node-1", "node-1", 3, 2, "node-1", "node-2", "node-3")
	f.positions.set("node-2", ReplicaPosition{LastTerm: 3, LastOffset: 100, Epoch: 3})
	f.positions.set("node-3", ReplicaPosition{LastTerm: 3, LastOffset: 98, Epoch: 3})

	f.manager.reconcileLocalLeadership()
	if epoch, promoted := f.accessor.promoteCalls[0]; promoted {
		t.Fatalf("an empty node took up the leadership at epoch %d while its followers hold the log", epoch)
	}
	if follower := f.accessor.followerCalls[0]; follower != "" {
		t.Fatalf("an empty node started replicating to %s", follower)
	}
	if ready, detail := f.manager.LeadershipReady(); ready {
		t.Fatalf("ready (%s) without leading the partition committed to it", detail)
	}
}

// A node whose log ends before a replica's does not lead either: it may have
// lost the end of its log, and the replica may hold acknowledged events.
func TestTakeUp_NodeBehindAReplicaDoesNotLead(t *testing.T) {
	f := newLeadershipFixture(t, "node-1", "node-1", 3, 2, "node-1", "node-2", "node-3")
	f.positions.set("node-1", ReplicaPosition{LastTerm: 3, LastOffset: 60, Epoch: 3})
	f.positions.set("node-2", ReplicaPosition{LastTerm: 3, LastOffset: 100, Epoch: 3})
	f.positions.set("node-3", ReplicaPosition{LastTerm: 3, LastOffset: 60, Epoch: 3})

	f.manager.reconcileLocalLeadership()
	if _, promoted := f.accessor.promoteCalls[0]; promoted {
		t.Fatal("took up the leadership while node-2 holds more of the log")
	}
}

// In a new cluster every replica is empty, and the first leader leads.
func TestTakeUp_FirstLeaderOfANewClusterLeads(t *testing.T) {
	f := newLeadershipFixture(t, "node-1", "node-1", 3, 2, "node-1", "node-2", "node-3")

	f.manager.reconcileLocalLeadership()
	if epoch, promoted := f.accessor.promoteCalls[0]; !promoted || epoch != f.committed(t).Epoch {
		t.Fatalf("promoted=%v at epoch %d; want the first leader to lead at the committed epoch %d", promoted, epoch, f.committed(t).Epoch)
	}
}

// A node that has never held the partition cannot tell an empty partition
// from a lost log by itself. It waits until another replica has answered.
func TestTakeUp_NodeThatNeverHeldThePartitionWaitsForAnotherReplica(t *testing.T) {
	f := newLeadershipFixture(t, "node-1", "node-1", 3, 2, "node-1", "node-2", "node-3")
	f.positions.unreachable["node-2"] = true
	f.positions.unreachable["node-3"] = true

	f.manager.reconcileLocalLeadership()
	if _, promoted := f.accessor.promoteCalls[0]; promoted {
		t.Fatal("a node that never held the partition took it up without hearing from any other replica")
	}

	delete(f.positions.unreachable, "node-2") // node-2 answers: it holds nothing either
	f.manager.reconcileLocalLeadership()
	if _, promoted := f.accessor.promoteCalls[0]; !promoted {
		t.Fatal("did not take up the partition once another replica said it holds nothing")
	}
}

// A node that holds the partition takes it up on what it hears. A failover
// must not wait for replicas that are down.
func TestTakeUp_NodeThatHoldsThePartitionLeadsWithoutAnswers(t *testing.T) {
	f := newLeadershipFixture(t, "node-1", "node-1", 3, 2, "node-1", "node-2", "node-3")
	f.positions.set("node-1", ReplicaPosition{LastTerm: 2, LastOffset: 50, Epoch: 2})
	f.positions.unreachable["node-2"] = true
	f.positions.unreachable["node-3"] = true

	f.manager.reconcileLocalLeadership()
	if _, promoted := f.accessor.promoteCalls[0]; !promoted {
		t.Fatal("a node holding the partition did not take it up while the other replicas were unreachable")
	}
}

// The check is made when a partition is taken up, not on every round while it
// is led; once the node stops leading it, the next take-up is checked again.
func TestTakeUp_CheckedWhenTakingUp(t *testing.T) {
	f := newLeadershipFixture(t, "node-1", "node-1", 3, 2, "node-1", "node-2", "node-3")
	f.positions.set("node-1", ReplicaPosition{LastTerm: 1, LastOffset: 10, Epoch: 1})
	f.manager.reconcileLocalLeadership()
	if _, promoted := f.accessor.promoteCalls[0]; !promoted {
		t.Fatal("did not take up the partition")
	}

	// While it leads, what the others report does not stop it.
	f.positions.set("node-2", ReplicaPosition{LastTerm: 1, LastOffset: 20, Epoch: 1})
	delete(f.accessor.promoteCalls, 0)
	f.manager.reconcileLocalLeadership()
	if _, promoted := f.accessor.promoteCalls[0]; !promoted {
		t.Fatal("a leader stopped refreshing its leadership on a later round")
	}

	// It is replaced, and named leader again: now it checks.
	commit := func(leader string) {
		t.Helper()
		info := f.committed(t)
		info.LeaderID = leader
		if err := f.store.ProposePartition(&info, false); err != nil {
			t.Fatal(err)
		}
		f.manager.reconcileLocalLeadership()
	}
	commit("node-2")
	delete(f.accessor.promoteCalls, 0)
	commit("node-1")
	if _, promoted := f.accessor.promoteCalls[0]; promoted {
		t.Fatal("took the partition up again while node-2 holds more of the log")
	}
}

// The coordinator elects another leader in place of one that is alive and
// does not lead while another replica holds more of the log. Without that a
// node that came back empty would hold the partition without a leader for as
// long as it runs: elections are otherwise held only for dead leaders.
func TestCoordinator_ReplacesALeaderThatCameBackWithoutItsLog(t *testing.T) {
	f := newLeadershipFixture(t, "node-2", "node-1", 3, 2, "node-1", "node-2", "node-3")
	// node-1 holds nothing and does not accept writes.
	f.positions.set("node-2", ReplicaPosition{LastTerm: 3, LastOffset: 100, Epoch: 3})
	f.positions.set("node-3", ReplicaPosition{LastTerm: 3, LastOffset: 98, Epoch: 3})
	before := f.committed(t)

	f.manager.replaceLeadersThatDoNotLead()
	after := f.committed(t)
	if after.LeaderID != "node-2" {
		t.Fatalf("leader %q, want node-2, the replica with the most complete log", after.LeaderID)
	}
	if after.Epoch <= before.Epoch {
		t.Fatalf("epoch %d did not rise above %d", after.Epoch, before.Epoch)
	}
}

// A leader that does not accept writes yet, because it is still taking the
// partition up, holds the most complete log and is left alone.
func TestCoordinator_LeavesALeaderThatIsTakingUp(t *testing.T) {
	f := newLeadershipFixture(t, "node-2", "node-1", 3, 2, "node-1", "node-2", "node-3")
	f.positions.set("node-1", ReplicaPosition{LastTerm: 3, LastOffset: 100, Epoch: 2})
	f.positions.set("node-2", ReplicaPosition{LastTerm: 3, LastOffset: 90, Epoch: 2})
	f.positions.set("node-3", ReplicaPosition{LastTerm: 3, LastOffset: 100, Epoch: 2})
	before := f.committed(t)

	f.manager.replaceLeadersThatDoNotLead()
	if after := f.committed(t); after.LeaderID != before.LeaderID || after.Epoch != before.Epoch {
		t.Fatalf("leader %s at epoch %d, want %s left at %d", after.LeaderID, after.Epoch, before.LeaderID, before.Epoch)
	}
	if f.store.proposals != 1 {
		t.Fatalf("%d proposals, want only the fixture's", f.store.proposals)
	}
}

// A leader that serves the partition is never second-guessed.
func TestCoordinator_LeavesALeaderThatLeads(t *testing.T) {
	f := newLeadershipFixture(t, "node-2", "node-1", 3, 2, "node-1", "node-2", "node-3")
	f.positions.set("node-1", ReplicaPosition{LastTerm: 3, LastOffset: 100, Epoch: 3, AcceptingWrites: true})
	f.positions.set("node-2", ReplicaPosition{LastTerm: 3, LastOffset: 100, Epoch: 3})
	before := f.committed(t)

	f.manager.replaceLeadersThatDoNotLead()
	if after := f.committed(t); after.LeaderID != before.LeaderID || after.Epoch != before.Epoch {
		t.Fatalf("leader %s at epoch %d, want %s left at %d", after.LeaderID, after.Epoch, before.LeaderID, before.Epoch)
	}
}

package cluster

import (
	"context"
	"fmt"
	"testing"
)

func (f *fakePositions) LeaderProgressFollowers(_ context.Context, addr string, _ int32) (bool, []string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	node := addr
	if addr == "" {
		node = f.local
	}
	if f.unreachable[node] {
		return false, nil, fmt.Errorf("node %s unreachable", node)
	}
	followers, reported := f.progress[node]
	return reported, followers, nil
}

func (f *fakePositions) holdProgress(leader string, followers ...string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.progress == nil {
		f.progress = make(map[string][]string)
	}
	f.progress[leader] = followers
}

// A handoff does not start while its target lacks the leader's record of what
// the consumer groups have finished, although its log has caught up: leading
// without it, the target would deliver again what was finished while it was
// away. The leader goes on taking publishes meanwhile.
func TestHandoff_WaitsForTheTargetToHoldTheConsumerProgress(t *testing.T) {
	f := newLeadershipFixture(t, "node-1", "node-1", 3, 2, "node-1", "node-2", "node-3")
	nodes := []string{"node-2", "node-1", "node-3"}
	f.manager.router.UpdatePartitionAssignment(0, "node-2", nodes, nodes) // the ring now prefers node-2
	f.positions.set("node-1", ReplicaPosition{LastTerm: 1, LastOffset: 100, AcceptingWrites: true})
	f.positions.set("node-2", ReplicaPosition{LastTerm: 1, LastOffset: 100})
	f.positions.holdProgress("node-1", "node-3") // node-2 has just come back

	f.manager.syncClusterState()
	if got := f.committed(t); got.TransferTo != "" || got.LeaderID != "node-1" || !f.manager.IsPartitionWritable(0) {
		t.Fatalf("handoff started before the target held the consumer progress: leader=%s transfer_to=%q writable=%v",
			got.LeaderID, got.TransferTo, f.manager.IsPartitionWritable(0))
	}

	f.positions.holdProgress("node-1", "node-2", "node-3")
	f.manager.syncClusterState()
	if got := f.committed(t); got.TransferTo != "node-2" {
		t.Fatalf("handoff did not start once the target held the consumer progress (transfer_to=%q)", got.TransferTo)
	}
}

// A handoff under way does not complete while the target is being sent the
// consumer progress again, and completes once it holds it.
func TestHandoff_CompletesOnlyWhenTheTargetHoldsTheConsumerProgress(t *testing.T) {
	f := newLeadershipFixture(t, "node-1", "node-1", 3, 2, "node-1", "node-2", "node-3")
	nodes := []string{"node-2", "node-1", "node-3"}
	f.manager.router.UpdatePartitionAssignment(0, "node-2", nodes, nodes)
	f.positions.set("node-1", ReplicaPosition{LastTerm: 1, LastOffset: 100, AcceptingWrites: true})
	f.positions.set("node-2", ReplicaPosition{LastTerm: 1, LastOffset: 100})
	f.positions.holdProgress("node-1", "node-2", "node-3")
	f.manager.syncClusterState()
	if got := f.committed(t); got.TransferTo != "node-2" {
		t.Fatalf("handoff did not start (transfer_to=%q)", got.TransferTo)
	}

	// The leader has stopped and the logs are equal, but the target's
	// connection was made again and it is being sent the full copy.
	f.positions.set("node-1", ReplicaPosition{LastTerm: 1, LastOffset: 100})
	f.positions.holdProgress("node-1", "node-3")
	f.manager.advanceTransfers()
	if got := f.committed(t); got.LeaderID != "node-1" || got.TransferTo != "node-2" {
		t.Fatalf("leadership moved to a target without the consumer progress (leader=%s transfer_to=%q)", got.LeaderID, got.TransferTo)
	}

	f.positions.holdProgress("node-1", "node-2", "node-3")
	f.manager.advanceTransfers()
	if got := f.committed(t); got.LeaderID != "node-2" || got.TransferTo != "" {
		t.Fatalf("handoff did not complete once the target held the consumer progress (leader=%s transfer_to=%q)", got.LeaderID, got.TransferTo)
	}
}

// A leader of a build that does not say which followers hold its consumer
// progress hands over as before: when the logs are equal.
func TestHandoff_ProceedsWhenTheLeaderDoesNotReportProgress(t *testing.T) {
	f := newLeadershipFixture(t, "node-1", "node-1", 3, 2, "node-1", "node-2", "node-3")
	nodes := []string{"node-2", "node-1", "node-3"}
	f.manager.router.UpdatePartitionAssignment(0, "node-2", nodes, nodes)
	f.positions.set("node-1", ReplicaPosition{LastTerm: 1, LastOffset: 100, AcceptingWrites: true})
	f.positions.set("node-2", ReplicaPosition{LastTerm: 1, LastOffset: 100})

	f.manager.syncClusterState()
	if got := f.committed(t); got.TransferTo != "node-2" {
		t.Fatalf("handoff did not start (transfer_to=%q)", got.TransferTo)
	}
	f.positions.set("node-1", ReplicaPosition{LastTerm: 1, LastOffset: 100})
	f.manager.advanceTransfers()
	if got := f.committed(t); got.LeaderID != "node-2" {
		t.Fatalf("handoff did not complete (leader=%s transfer_to=%q)", got.LeaderID, got.TransferTo)
	}
}

package api

import (
	"context"
	"testing"

	"github.com/jatin711-debug/cronos_db_golang/internal/partition"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

func (r *replicaLog) position(fenceEpoch int64) *types.ReplicationPositionResponse {
	r.t.Helper()
	resp, err := r.h.Position(context.Background(), &types.ReplicationPositionRequest{PartitionId: 0, FenceEpoch: fenceEpoch})
	if err != nil {
		r.t.Fatalf("position with fence epoch %d: %v", fenceEpoch, err)
	}
	return resp
}

// An election replaces a leader that the node deciding it cannot reach. That
// leader may still be running and still reach this replica. The election
// asks where the replica's log ends and chooses by the answers; what the old
// leader wrote here after the answer was confirmed to it, acknowledged to
// its publisher, and then removed, because the replica chosen never had it.
//
// Asked for an election, a replica first stops taking anything from a leader
// below the election's epoch. Its answer is then final as far as that leader
// goes.
func TestReplication_ReplicaAskedForAnElectionRefusesTheOldLeader(t *testing.T) {
	r := newReplicaLog(t, "follower")
	if resp := r.send("old-leader", 5, entries("old", 5, 0, 2)); !resp.GetSuccess() {
		t.Fatalf("setup: %s", resp.GetError())
	}

	// A plain question changes nothing: the leader goes on.
	if got := r.position(0); got.GetLastOffset() != 2 || got.GetEpoch() != 5 {
		t.Fatalf("position = offset %d epoch %d, want 2 and 5", got.GetLastOffset(), got.GetEpoch())
	}
	if resp := r.send("old-leader", 5, entries("old", 5, 3, 3)); !resp.GetSuccess() {
		t.Fatalf("the leader was refused after a question that fences nothing: %s", resp.GetError())
	}

	// The election's question, at epoch 6.
	got := r.position(6)
	if got.GetLastOffset() != 3 || got.GetEpoch() != 6 {
		t.Fatalf("fenced position = offset %d epoch %d, want 3 and 6", got.GetLastOffset(), got.GetEpoch())
	}
	resp := r.send("old-leader", 5, entries("old", 5, 4, 4))
	if resp.GetSuccess() {
		t.Fatal("the leader that is being replaced wrote to a replica after it had answered the election")
	}
	if resp.GetTerm() != 6 {
		t.Fatalf("the refusal names term %d, want 6: it is what tells the old leader to stop", resp.GetTerm())
	}
	if got := len(r.log()); got != 4 {
		t.Fatalf("the log has %d entries, want the 4 it had when it answered", got)
	}

	// It survives a restart of the replica, and the elected leader is taken.
	if got := r.position(0); got.GetEpoch() != 6 {
		t.Fatalf("epoch after the fence = %d, want 6", got.GetEpoch())
	}
	if resp := r.send("new-leader", 7, entries("new", 7, 4, 4)); !resp.GetSuccess() {
		t.Fatalf("the elected leader was refused: %s", resp.GetError())
	}
	// A fence below what the replica holds changes nothing.
	if got := r.position(3); got.GetEpoch() != 7 || got.GetLastOffset() != 4 {
		t.Fatalf("after an older fence: offset %d epoch %d, want 4 and 7", got.GetLastOffset(), got.GetEpoch())
	}
}

// A replica that holds nothing of the partition yet is asked all the same,
// and must hold the epoch too: empty and unfenced, the old leader could fill
// it afterwards and count it among those that confirmed its writes.
func TestReplication_ReplicaWithoutThePartitionIsFencedToo(t *testing.T) {
	cfg := &types.Config{DataDir: t.TempDir(), PartitionCount: 1, ReplicationFactor: 3, MinInSyncReplicas: 2, TickMS: 10, WheelSize: 100, SegmentSizeBytes: 1 << 20, IndexInterval: 1, FsyncMode: "periodic", FlushIntervalMS: 50, DedupTTLHours: 24, BloomCapacity: 1000}
	pm := partition.NewPartitionManager("empty", cfg)
	t.Cleanup(func() { pm.Close() })
	h := NewReplicationServiceHandler(pm)

	got, err := h.Position(context.Background(), &types.ReplicationPositionRequest{PartitionId: 0, FenceEpoch: 6})
	if err != nil {
		t.Fatal(err)
	}
	if got.GetLastOffset() != -1 || got.GetEpoch() != 6 {
		t.Fatalf("fenced position of an empty replica = offset %d epoch %d, want -1 and 6", got.GetLastOffset(), got.GetEpoch())
	}
	resp, err := h.Append(context.Background(), &types.ReplicationAppendRequest{PartitionId: 0, Term: 5, LeaderId: "old-leader", Events: entries("old", 5, 0, 0)})
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetSuccess() {
		t.Fatal("the leader that is being replaced filled a replica that had answered the election")
	}
}

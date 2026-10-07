package api

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jatin711-debug/cronos_db_golang/internal/dedup"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

// A replica that was brought up by snapshot holds only the log. Everything
// else it needs to lead is rebuilt from that log when it is promoted: the
// timers, and the record of which message IDs were already published.
func TestSnapshot_InstalledReplicaCanTakeOver(t *testing.T) {
	// The leader is at epoch 4 and holds twelve events.
	leader := newPublisher(t)
	if err := leader.pm.PromoteToLeader(0, 4); err != nil {
		t.Fatal(err)
	}
	batch := orders("order", 12)
	if err := leader.p.Wal.AppendBatch(batch); err != nil {
		t.Fatal(err)
	}

	replica := newReplicaLog(t, "node-b")
	if err := replica.pm.SyncPartitionFromLeader(0, leader.serveReplication()); err != nil {
		t.Fatalf("install snapshot: %v", err)
	}
	if got := len(replica.log()); got != 12 {
		t.Fatalf("replica log has %d entries after the snapshot, want 12", got)
	}
	// The replica is fenced at the epoch of the log it installed, durably.
	if replica.p.Epoch() != 4 {
		t.Fatalf("replica epoch = %d after installing a snapshot written at epoch 4", replica.p.Epoch())
	}
	if stale := replica.send("node-z", 3, entries("stale", 3, 12, 12)); stale.GetSuccess() {
		t.Fatal("replica accepted an append from epoch 3 after installing a snapshot from epoch 4")
	}
	if _, err := os.Stat(filepath.Join(replica.p.Wal.GetDataDir(), "snapshot-staging")); !os.IsNotExist(err) {
		t.Fatalf("snapshot staging directory left behind (err=%v)", err)
	}

	// The leader is lost; the replica takes over.
	if err := replica.pm.PromoteToLeader(0, 5); err != nil {
		t.Fatal(err)
	}
	if got := replica.p.Scheduler.GetTimingWheelDepth(); got != 12 {
		t.Fatalf("%d timers on the promoted replica, want 12", got)
	}
	outcome, offset, found, err := replica.p.DedupStore.Outcome("order-7")
	if err != nil || !found || outcome != dedup.Appended || offset != 7 {
		t.Fatalf("promoted replica's record of order-7: %v at %d (found=%v err=%v), want appended at 7", outcome, offset, found, err)
	}

	// A client that never got its answer retries against the new leader. The
	// events are there already; nothing is appended or scheduled twice.
	promoted := &publisher{replicaLog: replica, h: NewEventServiceHandler(replica.pm, replica.p.DedupStore, replica.p.ConsumerGroup)}
	other := newReplicaLog(t, "node-c")
	if err := replica.pm.AddFollower(0, "node-c", other.serveReplication()); err != nil {
		t.Fatal(err)
	}
	retry := promoted.publish(orders("order", 12))
	if !retry.GetSuccess() || retry.GetDuplicateCount() != 12 || retry.GetPublishedCount() != 0 {
		t.Fatalf("retry against the promoted replica: %+v", retry)
	}
	if log, replicated, timers := len(replica.log()), len(other.log()), replica.p.Scheduler.GetTimingWheelDepth(); log != 12 || replicated != 12 || timers != 12 {
		t.Fatalf("after the retry: %d log entries, %d on the new follower, %d timers; want 12 each", log, replicated, timers)
	}
}

// A node that was superseded as leader may still answer a snapshot request.
// Its log can lack events the cluster acknowledged since, so a replica that
// has accepted a newer epoch must not replace its own log with it.
func TestSnapshot_FromSupersededLeaderIsRefused(t *testing.T) {
	old := newReplicaLog(t, "node-a")
	if err := old.pm.PromoteToLeader(0, 2); err != nil {
		t.Fatal(err)
	}
	if err := old.p.Wal.AppendBatch(orders("old", 3)); err != nil {
		t.Fatal(err)
	}

	// The replica follows node-c at epoch 5 and holds six of its entries.
	replica := newReplicaLog(t, "node-b")
	if resp := replica.send("node-c", 5, entries("new", 5, 0, 5)); !resp.GetSuccess() {
		t.Fatalf("append from the current leader: %s", resp.GetError())
	}

	err := replica.pm.SyncPartitionFromLeader(0, old.serveReplication())
	if err == nil || !strings.Contains(err.Error(), "epoch") {
		t.Fatalf("snapshot from a leader at epoch 2 was not refused by a replica at epoch 5: %v", err)
	}
	log := replica.log()
	if len(log) != 6 {
		t.Fatalf("replica log has %d entries after the refused snapshot, want its own 6", len(log))
	}
	for _, event := range log {
		if !strings.HasPrefix(event.MessageId, "new-") {
			t.Fatalf("replica log holds %q from the superseded leader", event.MessageId)
		}
	}
	// It keeps working as a follower of the current leader.
	if resp := replica.send("node-c", 5, entries("new", 5, 6, 6)); !resp.GetSuccess() {
		t.Fatalf("append after the refused snapshot: %s", resp.GetError())
	}
	if _, err := replica.h.Position(context.Background(), &types.ReplicationPositionRequest{PartitionId: 0}); err != nil {
		t.Fatal(err)
	}
}

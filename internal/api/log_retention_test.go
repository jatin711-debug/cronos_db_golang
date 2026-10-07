package api

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/internal/partition"
	"github.com/jatin711-debug/cronos_db_golang/internal/storage"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

// newPrunableReplica is a replica of a clustered partition whose segments hold
// one event each, so that every finished event can be removed by itself.
func newPrunableReplica(t *testing.T, nodeID string) *replicaLog {
	t.Helper()
	cfg := &types.Config{DataDir: t.TempDir(), ClusterEnabled: true, PartitionCount: 1, ReplicationFactor: 3, MinInSyncReplicas: 2, TickMS: 10, WheelSize: 100, SegmentSizeBytes: 1024, IndexInterval: 1, FsyncMode: "periodic", FlushIntervalMS: 50, DedupTTLHours: 24, BloomCapacity: 1000}
	pm := partition.NewPartitionManager(nodeID, cfg)
	t.Cleanup(func() { pm.Close() })
	if err := pm.CreatePartition(0, "orders"); err != nil {
		t.Fatal(err)
	}
	p, err := pm.GetInternalPartition(0)
	if err != nil {
		t.Fatal(err)
	}
	return &replicaLog{t: t, pm: pm, h: NewReplicationServiceHandler(pm), p: p}
}

// bigOrders builds n publishable events that are already due and fill a
// segment each.
func bigOrders(tag string, n int) []*types.Event {
	due := time.Now().Add(-time.Second).UnixMilli()
	out := make([]*types.Event, n)
	for i := range out {
		out[i] = &types.Event{MessageId: fmt.Sprintf("%s-%d", tag, i), Topic: "orders", Payload: make([]byte, 2048), ScheduleTs: due}
	}
	return out
}

func (r *replicaLog) offsets() string {
	r.t.Helper()
	events, err := r.p.Wal.ReadEvents(0, r.p.Wal.GetLastOffset())
	if err != nil {
		r.t.Fatal(err)
	}
	out := make([]int64, len(events))
	for i, event := range events {
		out[i] = event.Offset
	}
	return fmt.Sprint(out)
}

func (r *replicaLog) waitOffsets(what, want string) {
	r.t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for r.offsets() != want {
		if time.Now().After(deadline) {
			r.t.Fatalf("%s: log holds offsets %s, want %s", what, r.offsets(), want)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// finish records, on the partition's leader, that the group has finished the
// events at these offsets.
func (r *replicaLog) finish(group string, offsets ...int64) {
	r.t.Helper()
	events := make([]*types.Event, 0, len(offsets))
	for _, offset := range offsets {
		event, err := r.p.Wal.ReadEvent(offset)
		if err != nil {
			r.t.Fatal(err)
		}
		events = append(events, event)
	}
	if err := r.p.ConsumerGroup.CommitDelivery(group, 0, events); err != nil {
		r.t.Fatal(err)
	}
}

func (r *replicaLog) prune() int {
	r.t.Helper()
	n, err := r.p.PruneWAL(context.Background(), storage.PruneOptions{AllCompleted: true})
	if err != nil {
		r.t.Fatal(err)
	}
	return n
}

// prunedCluster is a partition led by node-a at term 1 with followers node-b
// and node-c, six events at offsets 0-5 on all three, of which a consumer
// group has finished 0-3, and the leader's log pruned accordingly.
func prunedCluster(t *testing.T) (a, b, c *replicaLog, publish func([]*types.Event)) {
	t.Helper()
	a, b, c = newPrunableReplica(t, "node-a"), newPrunableReplica(t, "node-b"), newPrunableReplica(t, "node-c")
	if err := a.pm.PromoteToLeader(0, 1); err != nil {
		t.Fatal(err)
	}
	for id, follower := range map[string]*replicaLog{"node-b": b, "node-c": c} {
		if err := a.pm.AddFollower(0, id, follower.serveReplication()); err != nil {
			t.Fatal(err)
		}
	}
	handler := NewEventServiceHandler(a.pm, a.p.DedupStore, a.p.ConsumerGroup)
	// One publish per event: a batch goes into one segment as a whole.
	publish = func(events []*types.Event) {
		t.Helper()
		for _, event := range events {
			resp, err := handler.PublishBatch(context.Background(), &types.PublishBatchRequest{Events: []*types.Event{event}})
			if err != nil || !resp.GetSuccess() {
				t.Fatalf("publish: %+v %v", resp, err)
			}
		}
	}
	if err := a.p.ConsumerGroup.CreateGroup("workers", "orders", []int32{0}); err != nil {
		t.Fatal(err)
	}
	publish(bigOrders("order", 6))
	b.waitOffsets("node-b after the publish", "[0 1 2 3 4 5]")
	c.waitOffsets("node-c after the publish", "[0 1 2 3 4 5]")

	a.finish("workers", 0, 1, 2, 3)
	if n := a.prune(); n != 4 {
		t.Fatalf("the leader removed %d segments, want the 4 that are finished", n)
	}
	if got := a.offsets(); got != "[4 5]" {
		t.Fatalf("the leader's log holds offsets %s, want [4 5]", got)
	}
	return a, b, c, publish
}

// What the leader removes from the start of its log, the followers remove
// too, without anyone publishing, and a follower that takes over afterwards
// knows that what was removed is finished.
func TestRetention_FollowersFollowTheLeadersLogStart(t *testing.T) {
	a, b, c, publish := prunedCluster(t)

	b.waitOffsets("node-b after the leader pruned", "[4 5]")
	c.waitOffsets("node-c after the leader pruned", "[4 5]")
	for _, follower := range []*replicaLog{b, c} {
		if n, err := follower.p.PruneWAL(context.Background(), storage.PruneOptions{AllCompleted: true}); err == nil || n != 0 {
			t.Fatalf("a follower pruned by its own decision: %d segments, err %v", n, err)
		}
	}

	// The partition goes on as before.
	publish(bigOrders("more", 2))
	b.waitOffsets("node-b after a publish to the pruned log", "[4 5 6 7]")
	c.waitOffsets("node-c after a publish to the pruned log", "[4 5 6 7]")
	if got := a.offsets(); got != "[4 5 6 7]" {
		t.Fatalf("the leader's log holds offsets %s", got)
	}

	// node-b takes over. Nothing below the start of its log is left to do
	// for the group, and what is in the log and unfinished still is.
	if err := a.pm.DemoteFromLeader(0); err != nil {
		t.Fatal(err)
	}
	if err := b.pm.PromoteToLeader(0, 2); err != nil {
		t.Fatal(err)
	}
	for offset := int64(0); offset < 4; offset++ {
		if !b.p.ConsumerGroup.IsCompleted("workers", 0, offset) {
			t.Fatalf("the new leader counts offset %d, which was removed, as unfinished", offset)
		}
	}
	if b.p.ConsumerGroup.IsCompleted("workers", 0, 4) {
		t.Fatal("the new leader counts offset 4, which nobody acknowledged, as finished")
	}

	// It prunes in its turn, and the others follow it.
	for id, follower := range map[string]*replicaLog{"node-a": a, "node-c": c} {
		if err := b.pm.AddFollower(0, id, follower.serveReplication()); err != nil {
			t.Fatal(err)
		}
	}
	// The group is there already if the old leader's progress arrived in time.
	_ = b.p.ConsumerGroup.CreateGroup("workers", "orders", []int32{0})
	b.finish("workers", 4, 5)
	// A new leader removes nothing before it knows what a quorum holds.
	if n := b.prune(); n != 0 && b.p.AcceptedThrough() < 5 {
		t.Fatalf("the new leader removed %d segments before it had heard from its followers", n)
	}
	deadline := time.Now().Add(20 * time.Second)
	for b.p.AcceptedThrough() < 7 {
		if time.Now().After(deadline) {
			t.Fatalf("the new leader knows offsets through %d to be on a quorum, want 7", b.p.AcceptedThrough())
		}
		time.Sleep(20 * time.Millisecond)
	}
	b.prune()
	if got := b.offsets(); got != "[6 7]" {
		t.Fatalf("the new leader's log holds offsets %s after pruning, want [6 7]", got)
	}
	a.waitOffsets("node-a under the new leader", "[6 7]")
	c.waitOffsets("node-c under the new leader", "[6 7]")
}

// A replica whose log ends before the leader's begins cannot be sent what it
// is missing, and does not need it. It restarts its log where the leader's
// starts. That is how a replica with an empty disk joins a partition that has
// already removed entries, and how one that was away for long comes back.
func TestRetention_ReplicaBehindTheLogStartRestartsThere(t *testing.T) {
	a, b, _, publish := prunedCluster(t)
	b.waitOffsets("node-b after the leader pruned", "[4 5]")

	// A replica that holds nothing.
	empty := newPrunableReplica(t, "node-d")
	if err := a.pm.AddFollower(0, "node-d", empty.serveReplication()); err != nil {
		t.Fatal(err)
	}
	empty.waitOffsets("a replica that started empty", "[4 5]")
	if start := empty.p.Wal.GetFirstOffset(); start != 4 {
		t.Fatalf("its log starts at offset %d, want 4", start)
	}

	// A replica that holds the first two entries and nothing after them.
	behind := newPrunableReplica(t, "node-e")
	if resp := behind.send("node-a", 1, a.entriesLike(0, 1)); !resp.GetSuccess() {
		t.Fatalf("setup: %s", resp.GetError())
	}
	if err := a.pm.AddFollower(0, "node-e", behind.serveReplication()); err != nil {
		t.Fatal(err)
	}
	behind.waitOffsets("a replica that ended before the leader's log starts", "[4 5]")

	// Both take part from here on.
	publish(bigOrders("more", 1))
	for name, replica := range map[string]*replicaLog{"the replica that started empty": empty, "the replica that was behind": behind} {
		replica.waitOffsets(name+" after another publish", "[4 5 6]")
		if !replica.p.ConsumerGroup.IsCompleted("workers", 0, 3) || replica.p.ConsumerGroup.IsCompleted("workers", 0, 4) {
			t.Fatalf("%s does not count exactly what lies below the log start as finished", name)
		}
	}
}

// entriesLike builds entries at the given offsets that match what a leader at
// term 1 would have sent: this replica's own entries cannot be read once they
// are pruned.
func (r *replicaLog) entriesLike(from, to int64) []*types.Event {
	out := make([]*types.Event, 0, to-from+1)
	for offset := from; offset <= to; offset++ {
		out = append(out, &types.Event{MessageId: fmt.Sprintf("order-%d", offset), Topic: "orders", Offset: offset, Term: 1, ScheduleTs: 1, Payload: make([]byte, 2048)})
	}
	return out
}

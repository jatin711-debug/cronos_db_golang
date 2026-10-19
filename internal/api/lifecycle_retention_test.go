package api

import (
	"context"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/internal/lifecycle"
	"github.com/jatin711-debug/cronos_db_golang/internal/storage"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
	"google.golang.org/grpc/codes"
)

func TestLifecycleRetentionWaitsForReplicatedRelocationAndKeepsPendingIdentity(t *testing.T) {
	c := newReplicatedLifecycleConfig(t, true, func(cfg *types.Config) {
		cfg.SegmentSizeBytes, cfg.WALRelocationBytes = 512, 4096
	})
	publish := func(id string, due time.Time, size int) *types.EventReference {
		t.Helper()
		got, err := c.events.Publish(context.Background(), &types.PublishRequest{Event: &types.Event{MessageId: id, Topic: "orders", Payload: make([]byte, size), ScheduleTs: due.UnixMilli()}})
		if err != nil || !got.GetSuccess() {
			t.Fatalf("publish: %v %v", got, err)
		}
		return &types.EventReference{PartitionId: 0, Offset: got.Offset, Term: got.Term, MessageId: id, Topic: "orders"}
	}
	ref := publish("future", time.Now().Add(time.Hour), 700)
	due := publish("done", time.Now(), 700)
	publish("tail", time.Now(), 16)
	if _, err := c.state(ref); err != nil {
		t.Fatal(err)
	}
	if err := c.leader.p.ConsumerGroup.CreateGroup("workers", "orders", []int32{0}); err != nil {
		t.Fatal(err)
	}
	e, err := c.leader.p.Wal.ReadEvent(due.Offset)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.leader.p.ConsumerGroup.CommitDelivery("workers", 0, []*types.Event{e}); err != nil {
		t.Fatal(err)
	}
	c.gate.refuse(true)
	c.leader.p.ReplLeader.SetReplicateTimeout(100 * time.Millisecond)
	before := c.leader.p.Wal.GetLastOffset()
	if n, err := c.leader.p.PruneWAL(context.Background(), storage.PruneOptions{AllCompleted: true}); err == nil || n != 0 || c.leader.p.Wal.GetFirstOffset() != 0 {
		t.Fatalf("unreplicated relocation released prefix: %d %v", n, err)
	}
	if c.leader.p.Wal.GetLastOffset() != before+1 {
		t.Fatal("evacuation did not stage one bounded replacement")
	}
	_, err = c.state(ref)
	wantCode(t, err, codes.Unavailable, "unaccepted relocated state")
	c.gate.refuse(false)
	awaitLifecycle(t, "relocation never reached quorum", func() bool { return !c.leader.p.HasUnaccepted() })
	if n, err := c.leader.p.PruneWAL(context.Background(), storage.PruneOptions{AllCompleted: true}); err != nil || n == 0 || c.leader.p.Wal.GetFirstOffset() <= ref.Offset {
		t.Fatalf("accepted replacement kept prefix pinned: %d %v", n, err)
	}
	state, err := c.state(ref)
	if err != nil || state.CurrentOffset != before+1 || state.RevisionOffset != ref.Offset || c.leader.p.ConsumerGroup.IsCompleted("workers", 0, state.CurrentOffset) {
		t.Fatalf("relocation lost identity or inherited completion: %v %v", state, err)
	}
	awaitLifecycle(t, "follower did not follow retained prefix", func() bool { return c.follower.p.Wal.GetFirstOffset() > ref.Offset })
	copy, err := c.follower.p.Wal.ReadEvent(state.CurrentOffset)
	if err != nil || !c.follower.p.ActiveEvent(copy) {
		t.Fatalf("follower lost pending replacement: %v", err)
	}
	root, err := lifecycle.Root(copy)
	if err != nil || !lifecycle.SameReference(root, ref) {
		t.Fatalf("follower changed logical identity: %v %v", root, err)
	}
	if err := c.leader.pm.DemoteFromLeader(0); err != nil {
		t.Fatal(err)
	}
	if err := c.follower.pm.PromoteToLeader(0, 2); err != nil {
		t.Fatal(err)
	}
	if err := c.follower.pm.AddFollower(0, "leader", c.leader.serveReplication()); err != nil {
		t.Fatal(err)
	}
	awaitLifecycle(t, "retained replacement could not be committed after failover", func() bool { return c.follower.p.DeliverableThrough() > state.CurrentOffset })
	api := NewEventServiceHandler(c.follower.pm, c.follower.p.DedupStore, c.follower.p.ConsumerGroup)
	got, err := api.GetEventState(context.Background(), &types.EventStateRequest{Reference: ref})
	if err != nil || got.CurrentOffset != state.CurrentOffset {
		t.Fatalf("promotion lost pruned-root state: %v %v", got, err)
	}
}

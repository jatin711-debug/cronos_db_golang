package api

import (
	"context"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/internal/lifecycle"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func requireLifecycle(r *replicaLog) {
	r.pm.SetLifecycleRequirement(func(int32) bool { return true })
	r.pm.SetTermStartRequirement(func(int32) bool { return true })
}

func awaitLifecycle(t *testing.T, why string, done func() bool) {
	t.Helper()
	until := time.Now().Add(12 * time.Second)
	for !done() {
		if time.Now().After(until) {
			t.Fatal(why)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestLifecycleRecoveryKeepsTheClusterEpochAndSupportsElectionFences(t *testing.T) {
	dir := t.TempDir()
	r := newNamedClusterReplica(t, dir, "recovering")
	requireLifecycle(r)
	resp, err := r.h.AppendTermStart(context.Background(), &types.ReplicationTermAppendRequest{TermStartFormat: 1, LifecycleFormat: 1,
		Append: &types.ReplicationAppendRequest{PartitionId: 0, Term: 7, LeaderId: "leader", Events: cancelBatch(t, 7)}})
	if err != nil || !resp.GetSuccess() {
		t.Fatalf("seed lifecycle log: %v %v", resp, err)
	}
	r.stop()
	r = newNamedClusterReplica(t, dir, "recovering")
	requireLifecycle(r)
	if r.p.Epoch() != 7 {
		t.Fatalf("restart invented an unassigned cluster epoch: %d", r.p.Epoch())
	}
	pos, err := r.h.Position(context.Background(), &types.ReplicationPositionRequest{PartitionId: 0, FenceEpoch: 8})
	if err != nil || !pos.GetFound() || pos.GetEpoch() != 8 || pos.GetLastTerm() != 7 {
		t.Fatalf("lifecycle replica refused an election fence: %v %v", pos, err)
	}
	progress, err := r.h.SyncConsumerProgress(context.Background(), &types.ReplicationProgressRequest{
		PartitionId: 0, Term: 8, LeaderId: "next", Groups: []*types.ConsumerGroupProgress{{GroupId: "workers", Topic: "orders", CommittedOffset: 1}},
	})
	if err != nil || !progress.GetSuccess() || !r.p.ConsumerGroup.IsCompleted("workers", 0, 0) {
		t.Fatalf("lifecycle follower lost consumer progress: %v %v", progress, err)
	}
}

func TestLifecycleRecoveryReplacesAnUnreplicatedMutationBeforeOffsetReuse(t *testing.T) {
	c := newReplicatedLifecycle(t, true)
	ref := c.publish("order")
	rev, err := c.state(ref)
	if err != nil {
		t.Fatal(err)
	}
	c.gate.refuse(true)
	c.leader.p.ReplLeader.SetReplicateTimeout(100 * time.Millisecond)
	_, err = c.cancel(ref, "lost-cancel", rev)
	wantCode(t, err, codes.Unavailable, "unreplicated cancellation")
	stale := c.leader.log()[c.leader.p.Wal.GetLastOffset()]
	if err := c.leader.pm.DemoteFromLeader(0); err != nil {
		t.Fatal(err)
	}
	third := newNamedClusterReplica(t, t.TempDir(), "third")
	requireLifecycle(third)
	gate := &lifecycleGate{gatedReplication{ReplicationServiceServer: third.h}}
	gate.refuse(true)
	if err := c.follower.pm.PromoteToLeader(0, 2); err != nil {
		t.Fatal(err)
	}
	if err := c.follower.pm.AddFollower(0, "third", serveReplicationService(t, gate)); err != nil {
		t.Fatal(err)
	}
	api := NewEventServiceHandler(c.follower.pm, c.follower.p.DedupStore, c.follower.p.ConsumerGroup)
	_, err = api.GetEventState(context.Background(), &types.EventStateRequest{Reference: ref})
	wantCode(t, err, codes.Unavailable, "new leader without current-term proof")
	gate.refuse(false)
	awaitLifecycle(t, "new leader never committed its term", func() bool { return c.follower.p.DeliverableThrough() >= 2 })
	state, err := api.GetEventState(context.Background(), &types.EventStateRequest{Reference: ref})
	if err != nil || state.GetCanceled() || state.GetCurrentOffset() != ref.Offset {
		t.Fatalf("lost mutation changed surviving state: %v %v", state, err)
	}
	if err := c.follower.pm.AddFollower(0, "leader", c.leader.serveReplication()); err != nil {
		t.Fatal(err)
	}
	awaitLifecycle(t, "returning leader retained its canceled head", func() bool {
		e, err := c.leader.p.Wal.ReadEvent(stale.Offset)
		return err == nil && e.Term == 2 && c.leader.p.ActiveEvent(c.leader.log()[ref.Offset])
	})
	if c.leader.p.ActiveEvent(stale) || c.leader.p.Wal.Failure() != nil {
		t.Fatal("cut mutation survived in a cached reader or corrupted the rebuilt index")
	}
	state, err = api.CancelEvent(context.Background(), &types.EventMutationRequest{Reference: ref, RequestId: "lost-cancel", ExpectedRevisionOffset: rev.RevisionOffset, ExpectedRevisionTerm: rev.RevisionTerm})
	if err != nil || !state.GetCanceled() || state.CurrentOffset <= stale.Offset {
		t.Fatalf("retry after suffix reuse did not append in new term: %v %v", state, err)
	}
}

type lostLifecycleReply struct {
	types.ReplicationServiceServer
	lose   atomic.Bool
	stored atomic.Int32
}

func (s *lostLifecycleReply) AppendTermStart(ctx context.Context, req *types.ReplicationTermAppendRequest) (*types.ReplicationAppendResponse, error) {
	resp, err := s.ReplicationServiceServer.AppendTermStart(ctx, req)
	if err == nil && resp.GetSuccess() && s.lose.Load() {
		for _, e := range req.GetAppend().GetEvents() {
			if r, _ := lifecycle.Decode(e); r != nil {
				s.stored.Add(1)
			}
		}
		return nil, status.Error(codes.Unavailable, "reply lost after durable lifecycle append")
	}
	return resp, err
}

func TestLifecycleRecoveryLostSuccessReplyAndPromotionPreserveOneActiveVersion(t *testing.T) {
	for _, kind := range []string{lifecycle.Cancel, lifecycle.Reschedule} {
		t.Run(kind, func(t *testing.T) {
			c := newReplicatedLifecycle(t, true)
			ref := c.publish("order")
			rev, err := c.state(ref)
			if err != nil {
				t.Fatal(err)
			}
			if err := c.leader.p.ReplLeader.RemoveFollower("follower"); err != nil {
				t.Fatal(err)
			}
			gate := &lostLifecycleReply{ReplicationServiceServer: c.follower.h}
			gate.lose.Store(true)
			if err := c.leader.pm.AddFollower(0, "follower", serveReplicationService(t, gate)); err != nil {
				t.Fatal(err)
			}
			c.leader.p.ReplLeader.SetReplicateTimeout(100 * time.Millisecond)
			req := &types.EventMutationRequest{Reference: ref, RequestId: "change", ExpectedRevisionOffset: rev.RevisionOffset, ExpectedRevisionTerm: rev.RevisionTerm}
			if kind == lifecycle.Reschedule {
				req.ScheduleTs = time.Now().Add(2 * time.Hour).UnixMilli()
			}
			if kind == lifecycle.Cancel {
				_, err = c.events.CancelEvent(context.Background(), req)
			} else {
				_, err = c.events.RescheduleEvent(context.Background(), req)
			}
			wantCode(t, err, codes.Unavailable, "mutation with lost reply")
			awaitLifecycle(t, "follower never durably stored mutation", func() bool { return gate.stored.Load() > 0 })
			if err := c.leader.pm.DemoteFromLeader(0); err != nil {
				t.Fatal(err)
			}
			// Lose the derived index and timers too, not just the old leader.
			dir := filepath.Dir(filepath.Dir(c.follower.p.DataDir))
			c.follower.stop()
			c.follower = newNamedClusterReplica(t, dir, "follower")
			requireLifecycle(c.follower)
			if err := c.follower.pm.PromoteToLeader(0, 2); err != nil {
				t.Fatal(err)
			}
			if err := c.follower.pm.AddFollower(0, "leader", c.leader.serveReplication()); err != nil {
				t.Fatal(err)
			}
			awaitLifecycle(t, "promoted lifecycle log never committed", func() bool { return c.follower.p.DeliverableThrough() >= 3 })
			api := NewEventServiceHandler(c.follower.pm, c.follower.p.DedupStore, c.follower.p.ConsumerGroup)
			end := c.follower.p.Wal.GetLastOffset()
			var got *types.EventStateResponse
			if kind == lifecycle.Cancel {
				got, err = api.CancelEvent(context.Background(), req)
			} else {
				got, err = api.RescheduleEvent(context.Background(), req)
			}
			if err != nil || got.GetCanceled() != (kind == lifecycle.Cancel) || c.follower.p.Wal.GetLastOffset() != end {
				t.Fatalf("recovered retry changed outcome or duplicated mutation: %v %v", got, err)
			}
			active := 0
			for _, e := range c.follower.log() {
				if err := c.follower.p.WithActiveEvent(e, func(*types.Event) error { active++; return nil }); err != nil {
					t.Fatal(err)
				}
			}
			want := 1
			if kind == lifecycle.Cancel {
				want = 0
			}
			if active != want {
				t.Fatalf("recovery exposed %d active versions; want %d", active, want)
			}
		})
	}
}

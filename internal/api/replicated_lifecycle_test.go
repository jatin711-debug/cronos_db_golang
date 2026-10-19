package api

import (
	"context"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/internal/logcontrol"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// lifecycleGate is gatedReplication for a follower that also takes the
// versioned append.
type lifecycleGate struct{ gatedReplication }

func (g *lifecycleGate) AppendTermStart(ctx context.Context, req *types.ReplicationTermAppendRequest) (*types.ReplicationAppendResponse, error) {
	if g.refusing.Load() {
		return nil, status.Error(codes.Unavailable, "follower unreachable")
	}
	return g.ReplicationServiceServer.AppendTermStart(ctx, req)
}

// These assertions count logical records; term-start records only prove the
// leader's term and never stand for a publish or a lifecycle operation.
func (c *replicatedLifecycle) records(r *replicaLog) []*types.Event {
	var records []*types.Event
	for _, e := range r.log() {
		if !logcontrol.IsTermStart(e) {
			records = append(records, e)
		}
	}
	return records
}

func (g *lifecycleGate) AppendLifecycle(ctx context.Context, req *types.ReplicationLifecycleAppendRequest) (*types.ReplicationAppendResponse, error) {
	if g.refusing.Load() {
		return nil, status.Error(codes.Unavailable, "follower unreachable")
	}
	return g.ReplicationServiceServer.AppendLifecycle(ctx, req)
}

// replicatedLifecycle is a partition led by one node with one follower, both
// of a cluster that has committed the partition's lifecycle requirement; two
// replicas must hold an entry before it is accepted.
type replicatedLifecycle struct {
	t        *testing.T
	leader   *replicaLog
	follower *replicaLog
	gate     *lifecycleGate
	events   *EventServiceHandler
}

func newReplicatedLifecycle(t *testing.T, required bool) *replicatedLifecycle {
	return newReplicatedLifecycleConfig(t, required, nil)
}

func newReplicatedLifecycleConfig(t *testing.T, required bool, configure func(*types.Config)) *replicatedLifecycle {
	t.Helper()
	leader := newConfiguredClusterReplica(t, t.TempDir(), "leader", configure)
	follower := newConfiguredClusterReplica(t, t.TempDir(), "follower", configure)
	for _, r := range []*replicaLog{leader, follower} {
		r.pm.SetLifecycleRequirement(func(int32) bool { return required })
		r.pm.SetTermStartRequirement(func(int32) bool { return required })
	}
	if err := leader.pm.PromoteToLeader(0, 1); err != nil {
		t.Fatal(err)
	}
	gate := &lifecycleGate{gatedReplication{ReplicationServiceServer: follower.h}}
	if err := leader.pm.AddFollower(0, "follower", serveReplicationService(t, gate)); err != nil {
		t.Fatal(err)
	}
	return &replicatedLifecycle{t: t, leader: leader, follower: follower, gate: gate,
		events: NewEventServiceHandler(leader.pm, leader.p.DedupStore, leader.p.ConsumerGroup)}
}

// publish publishes an event due an hour from now and returns its reference.
func (c *replicatedLifecycle) publish(id string) *types.EventReference {
	c.t.Helper()
	resp, err := c.events.Publish(context.Background(), &types.PublishRequest{Event: &types.Event{
		MessageId: id, Topic: "orders", Payload: []byte(id), ScheduleTs: time.Now().Add(time.Hour).UnixMilli()}})
	if err != nil || !resp.GetSuccess() {
		c.t.Fatalf("publish %s: resp=%v err=%v", id, resp, err)
	}
	return &types.EventReference{PartitionId: resp.GetPartitionId(), Offset: resp.GetOffset(), Term: resp.GetTerm(), MessageId: id, Topic: "orders"}
}

func (c *replicatedLifecycle) state(ref *types.EventReference) (*types.EventStateResponse, error) {
	return c.events.GetEventState(context.Background(), &types.EventStateRequest{Reference: ref})
}

func (c *replicatedLifecycle) cancel(ref *types.EventReference, requestID string, rev *types.EventStateResponse) (*types.EventStateResponse, error) {
	return c.events.CancelEvent(context.Background(), &types.EventMutationRequest{Reference: ref, RequestId: requestID,
		ExpectedRevisionOffset: rev.GetRevisionOffset(), ExpectedRevisionTerm: rev.GetRevisionTerm()})
}

// delivered reports whether the leader would hand the event at offset out.
func (c *replicatedLifecycle) delivered(offset int64) bool {
	c.t.Helper()
	e, err := c.leader.p.Wal.ReadEvent(offset)
	if err != nil {
		c.t.Fatal(err)
	}
	sent := false
	if err := c.leader.p.WithActiveEvent(e, func(*types.Event) error { sent = true; return nil }); err != nil {
		c.t.Fatal(err)
	}
	return sent
}

// A cancellation on a replicated leader succeeds once both replicas hold it;
// the follower switches to lifecycle mode with it, and neither replica treats
// the event as one to deliver any more.
func TestReplicatedCancel_AcceptedAtQuorum(t *testing.T) {
	c := newReplicatedLifecycle(t, true)
	ref := c.publish("order-1")
	rev, err := c.state(ref)
	if err != nil {
		t.Fatal(err)
	}
	if !c.leader.p.LifecycleEnabled() {
		t.Fatal("the leader did not switch to lifecycle mode")
	}
	got, err := c.cancel(ref, "cancel-1", rev)
	if err != nil || !got.GetCanceled() {
		t.Fatalf("cancel: resp=%v err=%v", got, err)
	}
	if events := c.records(c.follower); len(events) != 2 || !c.follower.p.LifecycleEnabled() || c.follower.p.ActiveEvent(events[0]) {
		t.Fatalf("follower: %d entries, lifecycle mode=%v; want the event and its cancellation, and the event canceled", len(events), c.follower.p.LifecycleEnabled())
	}
	if c.delivered(ref.Offset) {
		t.Fatal("the leader would still deliver the canceled event")
	}
	if again, err := c.state(ref); err != nil || !again.GetCanceled() {
		t.Fatalf("state after the cancel: %v %v", again, err)
	}
}

// A cancellation that reaches the leader's log but not the follower has an
// unknown outcome. Meanwhile the event is frozen, neither delivered nor
// changed again, and its state is not reported. Once catch-up brings the
// cancellation to the follower, the unchanged retry reports success.
func TestReplicatedCancel_UnknownOutcomeUntilAQuorumHoldsIt(t *testing.T) {
	c := newReplicatedLifecycle(t, true)
	c.leader.p.ReplLeader.SetReplicateTimeout(time.Second)
	ref := c.publish("order-1")
	rev, err := c.state(ref)
	if err != nil {
		t.Fatal(err)
	}

	c.gate.refuse(true)
	_, err = c.cancel(ref, "cancel-1", rev)
	wantCode(t, err, codes.Unavailable, "cancel while the follower refuses appends")
	if n := len(c.records(c.leader)); n != 2 {
		t.Fatalf("leader log has %d entries, want the event and the staged cancellation", n)
	}
	if c.delivered(ref.Offset) {
		t.Fatal("the event is delivered while its cancellation is unresolved")
	}
	_, err = c.state(ref)
	wantCode(t, err, codes.Unavailable, "state while the cancellation is unresolved")
	_, err = c.cancel(ref, "cancel-2", rev)
	wantCode(t, err, codes.Unavailable, "another change while the cancellation is unresolved")
	if n := len(c.records(c.leader)); n != 2 {
		t.Fatalf("leader log has %d entries after a second request, want 2", n)
	}

	c.gate.refuse(false)
	deadline := time.Now().Add(15 * time.Second)
	for {
		got, err := c.cancel(ref, "cancel-1", rev)
		if err == nil {
			if !got.GetCanceled() {
				t.Fatalf("retry reports %v, want canceled", got)
			}
			break
		}
		if status.Code(err) != codes.Unavailable || time.Now().After(deadline) {
			t.Fatalf("retry after the follower returned: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	if n := len(c.records(c.leader)); n != 2 {
		t.Fatalf("leader log has %d entries after the retry: the cancellation was appended again", n)
	}
	if events := c.records(c.follower); len(events) != 2 || c.follower.p.ActiveEvent(events[0]) {
		t.Fatalf("follower holds %d entries, want the event and its cancellation", len(events))
	}
	if c.leader.p.HasUnaccepted() {
		t.Fatal("the cancellation is still held after a quorum took it")
	}
}

// A reschedule replaces the event with a copy due at the new time, which the
// follower holds too; the copy is the one to deliver, the original is not.
func TestReplicatedReschedule_ReplacesTheEvent(t *testing.T) {
	c := newReplicatedLifecycle(t, true)
	ref := c.publish("order-1")
	rev, err := c.state(ref)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Now().Add(2 * time.Hour).UnixMilli()
	got, err := c.events.RescheduleEvent(context.Background(), &types.EventMutationRequest{Reference: ref, RequestId: "move-1",
		ExpectedRevisionOffset: rev.GetRevisionOffset(), ExpectedRevisionTerm: rev.GetRevisionTerm(), ScheduleTs: at})
	if err != nil || got.GetScheduleTs() != at || got.GetCurrentOffset() == ref.Offset {
		t.Fatalf("reschedule: resp=%v err=%v", got, err)
	}
	if c.delivered(ref.Offset) {
		t.Fatal("the original is still one to deliver")
	}
	replacement, err := c.leader.p.Wal.ReadEvent(got.GetCurrentOffset())
	if err != nil || !c.leader.p.ActiveEvent(replacement) {
		t.Fatalf("the replacement is not the active copy (%v)", err)
	}
	if events := c.records(c.follower); len(events) != 2 || events[1].GetScheduleTs() != at {
		t.Fatalf("follower holds %d entries, want the event and its replacement due at %d", len(events), at)
	}
}

// Changes are made on the partition's leader only: a follower refuses them
// and writes nothing.
func TestReplicatedMutation_RefusedOnAFollower(t *testing.T) {
	c := newReplicatedLifecycle(t, true)
	ref := c.publish("order-1")
	rev, err := c.state(ref)
	if err != nil {
		t.Fatal(err)
	}
	onFollower := NewEventServiceHandler(c.follower.pm, c.follower.p.DedupStore, c.follower.p.ConsumerGroup)
	_, err = onFollower.CancelEvent(context.Background(), &types.EventMutationRequest{Reference: ref, RequestId: "cancel-1",
		ExpectedRevisionOffset: rev.GetRevisionOffset(), ExpectedRevisionTerm: rev.GetRevisionTerm()})
	wantCode(t, err, codes.Unavailable, "cancel on a follower")
	if n := len(c.records(c.follower)); n != 1 {
		t.Fatalf("follower log has %d entries, want only the event", n)
	}
}

// Without the cluster's requirement the partition stays out of lifecycle
// mode, and the calls say so.
func TestReplicatedMutation_RefusedWithoutTheRequirement(t *testing.T) {
	c := newReplicatedLifecycle(t, false)
	ref := c.publish("order-1")
	_, err := c.state(ref)
	wantCode(t, err, codes.FailedPrecondition, "state without the requirement")
	if c.leader.p.LifecycleEnabled() {
		t.Fatal("the leader switched to lifecycle mode without the requirement")
	}
}

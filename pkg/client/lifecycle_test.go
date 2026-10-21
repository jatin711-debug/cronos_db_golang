package client

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// lifecycleNode answers lifecycle calls the way a server node does: a node
// that does not lead the partition refuses them, and the leader can be told
// to fail the next calls with a given error.
type lifecycleNode struct {
	types.UnimplementedEventServiceServer
	leads atomic.Bool

	mu       sync.Mutex
	failNext []error
	requests []*types.EventMutationRequest
	states   int
}

func (n *lifecycleNode) answer(req *types.EventMutationRequest) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if req != nil {
		n.requests = append(n.requests, req)
	}
	if !n.leads.Load() {
		return status.Error(codes.Unavailable, "this node does not lead partition 0; retry against its leader")
	}
	if len(n.failNext) > 0 {
		err := n.failNext[0]
		n.failNext = n.failNext[1:]
		return err
	}
	return nil
}

func (n *lifecycleNode) GetEventState(_ context.Context, req *types.EventStateRequest) (*types.EventStateResponse, error) {
	if err := n.answer(nil); err != nil {
		return nil, err
	}
	n.mu.Lock()
	n.states++
	n.mu.Unlock()
	ref := req.GetReference()
	return &types.EventStateResponse{Reference: ref, CurrentOffset: ref.GetOffset(), RevisionOffset: ref.GetOffset(), RevisionTerm: ref.GetTerm(), ScheduleTs: 1234}, nil
}

func (n *lifecycleNode) mutation(req *types.EventMutationRequest, canceled bool) (*types.EventStateResponse, error) {
	if err := n.answer(req); err != nil {
		return nil, err
	}
	return &types.EventStateResponse{Reference: req.GetReference(), CurrentOffset: 9, RevisionOffset: 9, RevisionTerm: 2, ScheduleTs: req.GetScheduleTs(), Canceled: canceled}, nil
}

func (n *lifecycleNode) CancelEvent(_ context.Context, req *types.EventMutationRequest) (*types.EventStateResponse, error) {
	return n.mutation(req, true)
}

func (n *lifecycleNode) RescheduleEvent(_ context.Context, req *types.EventMutationRequest) (*types.EventStateResponse, error) {
	return n.mutation(req, false)
}

func (n *lifecycleNode) failWith(errs ...error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.failNext = append(n.failNext, errs...)
}

func (n *lifecycleNode) seen() []*types.EventMutationRequest {
	n.mu.Lock()
	defer n.mu.Unlock()
	return append([]*types.EventMutationRequest(nil), n.requests...)
}

func lifecycleCluster(t *testing.T) (follower, leader *lifecycleNode, cl *Client) {
	t.Helper()
	follower, leader = &lifecycleNode{}, &lifecycleNode{}
	leader.leads.Store(true)
	addrF := startTestEventServer(t, follower, &testPartitionServer{})
	addrL := startTestEventServer(t, leader, &testPartitionServer{})
	cl, err := Dial(context.Background(), DefaultConfig(addrF, addrL))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cl.Close() })
	return follower, leader, cl
}

var testRef = EventReference{Topic: "orders", PartitionID: 0, Offset: 5, Term: 1, MessageID: "m-5"}

func TestLifecycle_CallsReachThePartitionLeader(t *testing.T) {
	_, leader, cl := lifecycleCluster(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	state, err := cl.GetEventState(ctx, testRef)
	if err != nil {
		t.Fatal(err)
	}
	if state.Reference != testRef || state.Revision != testRef.InitialRevision() || state.ScheduleTS != 1234 {
		t.Fatalf("state = %+v", state)
	}
	at := time.Now().Add(time.Hour)
	state, err = cl.RescheduleEvent(ctx, testRef, EventChange{RequestID: "r1", ExpectedRevision: state.Revision}, at)
	if err != nil {
		t.Fatal(err)
	}
	if state.ScheduleTS != at.UnixMilli() || state.Revision != (Revision{Offset: 9, Term: 2}) {
		t.Fatalf("reschedule state = %+v", state)
	}
	if _, err = cl.CancelEvent(ctx, testRef, EventChange{RequestID: "c1", ExpectedRevision: state.Revision}); err != nil {
		t.Fatal(err)
	}
	reqs := leader.seen()
	if len(reqs) != 2 || reqs[0].GetScheduleTs() != at.UnixMilli() || reqs[1].GetScheduleTs() != 0 ||
		reqs[1].GetExpectedRevisionOffset() != 9 || reqs[1].GetExpectedRevisionTerm() != 2 {
		t.Fatalf("leader saw %v", reqs)
	}
}

func TestLifecycle_UnknownOutcomeIsRetriedUnchanged(t *testing.T) {
	_, leader, cl := lifecycleCluster(t)
	leader.failWith(status.Error(codes.Unavailable, "request c1 is in the leader's log but not yet on the replicas it needs; its outcome is unknown, retry it unchanged"))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	state, err := cl.CancelEvent(ctx, testRef, EventChange{RequestID: "c1", ExpectedRevision: testRef.InitialRevision()})
	if err != nil || !state.Canceled {
		t.Fatalf("cancel = %+v, %v", state, err)
	}
	reqs := leader.seen()
	if len(reqs) != 2 {
		t.Fatalf("leader saw %d requests, want the failed one and its retry", len(reqs))
	}
	if reqs[0].GetRequestId() != reqs[1].GetRequestId() || reqs[0].GetExpectedRevisionOffset() != reqs[1].GetExpectedRevisionOffset() {
		t.Fatalf("retry changed the request: %v then %v", reqs[0], reqs[1])
	}
}

func TestLifecycle_FinalAnswersKeepTheirKind(t *testing.T) {
	cases := []struct {
		err  error
		kind ErrorKind
		code codes.Code
	}{
		{status.Error(codes.Aborted, "event revision changed; read its current state"), ErrorKindConflict, codes.Aborted},
		{status.Error(codes.FailedPrecondition, "event is canceled"), ErrorKindPrecondition, codes.FailedPrecondition},
		{status.Error(codes.FailedPrecondition, "event-lifecycle is disabled"), ErrorKindPrecondition, codes.FailedPrecondition},
		{status.Error(codes.NotFound, "event state is no longer retained"), ErrorKindNotFound, codes.NotFound},
		{status.Error(codes.PermissionDenied, "no publish permission"), ErrorKindAuth, codes.PermissionDenied},
	}
	for _, tc := range cases {
		_, leader, cl := lifecycleCluster(t)
		leader.failWith(tc.err)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		_, err := cl.CancelEvent(ctx, testRef, EventChange{RequestID: "c1", ExpectedRevision: testRef.InitialRevision()})
		cancel()
		var ce *Error
		if !errors.As(err, &ce) || ce.Kind != tc.kind {
			t.Fatalf("%v: got %v, want kind %s", tc.err, err, tc.kind)
		}
		if status.Code(err) != tc.code {
			t.Fatalf("%v: the gRPC status is lost: %v", tc.err, status.Code(err))
		}
		if n := len(leader.seen()); n != 1 {
			t.Fatalf("%v: a final answer was retried (%d requests)", tc.err, n)
		}
	}
}

func TestLifecycle_RefusesIncompleteRequests(t *testing.T) {
	_, leader, cl := lifecycleCluster(t)
	ctx := context.Background()
	check := func(name string, err error) {
		t.Helper()
		var ce *Error
		if !errors.As(err, &ce) || ce.Kind != ErrorKindValidation {
			t.Fatalf("%s: got %v, want a validation error", name, err)
		}
	}
	_, err := cl.CancelEvent(ctx, testRef, EventChange{})
	check("no request ID", err)
	_, err = cl.GetEventState(ctx, EventReference{Topic: "orders", Offset: 1, Term: 1})
	check("no message ID", err)
	if n := len(leader.seen()); n != 0 {
		t.Fatalf("an invalid request reached the server (%d)", n)
	}
}

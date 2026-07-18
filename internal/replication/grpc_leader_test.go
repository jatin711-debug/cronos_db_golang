package replication

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
	"google.golang.org/grpc"
)

// fakeReplServer is an in-process ReplicationService that records appended
// events and enforces offset contiguity like the real follower handler.
type fakeReplServer struct {
	types.UnimplementedReplicationServiceServer
	mu       sync.Mutex
	received []*types.Event
	nextOff  int64
	lastTerm int64
}

func (s *fakeReplServer) Append(_ context.Context, req *types.ReplicationAppendRequest) (*types.ReplicationAppendResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastTerm = req.GetTerm()
	if req.GetExpectedNextOffset() != s.nextOff {
		return &types.ReplicationAppendResponse{
			Success:    false,
			Error:      "offset mismatch",
			NextOffset: s.nextOff,
			LastOffset: s.nextOff - 1,
		}, nil
	}
	for _, e := range req.GetEvents() {
		s.received = append(s.received, e)
		s.nextOff = e.GetOffset() + 1
	}
	return &types.ReplicationAppendResponse{
		Success:    true,
		LastOffset: s.nextOff - 1,
		NextOffset: s.nextOff,
	}, nil
}

func (s *fakeReplServer) term() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastTerm
}

func (s *fakeReplServer) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.received)
}

func startFakeReplServer(t *testing.T) (*fakeReplServer, string, func()) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	fake := &fakeReplServer{}
	srv := grpc.NewServer()
	types.RegisterReplicationServiceServer(srv, fake)
	go func() { _ = srv.Serve(lis) }()
	return fake, lis.Addr().String(), func() { srv.Stop() }
}

// TestLeader_GRPCReplicate_EndToEnd verifies the leader ships events to a
// follower over gRPC and meets the min-insync quorum (leader + 1 follower).
func TestLeader_GRPCReplicate_EndToEnd(t *testing.T) {
	fake, addr, stop := startFakeReplServer(t)
	defer stop()

	l := NewLeader(0, 500, 100*time.Millisecond, nil, 2, "leader", nil)
	if err := l.AddFollower("f1", addr); err != nil {
		t.Fatalf("AddFollower: %v", err)
	}
	defer l.Stop()

	events := []*types.Event{
		{MessageId: "m0", Offset: 0, Payload: []byte("a"), Topic: "t"},
		{MessageId: "m1", Offset: 1, Payload: []byte("b"), Topic: "t"},
		{MessageId: "m2", Offset: 2, Payload: []byte("c"), Topic: "t"},
	}
	if err := l.Replicate(events); err != nil {
		t.Fatalf("Replicate: %v", err)
	}

	if got := fake.count(); got != 3 {
		t.Fatalf("follower received %d events, want 3", got)
	}

	// The follower must now be in-sync so leadership stats reflect quorum.
	if isr := l.GetInSyncReplicas(); len(isr) != 1 {
		t.Fatalf("expected 1 in-sync replica, got %d", len(isr))
	}
}

// TestLeader_SetEpochPropagatesToWire verifies that SetEpoch changes the Term
// carried by outgoing Append RPCs. Before the fix, PromoteToLeader updated the
// partition epoch but not the Leader object, so the wire term stayed at the
// default 1 and the follower's stale-term fence could not distinguish leaders.
func TestLeader_SetEpochPropagatesToWire(t *testing.T) {
	fake, addr, stop := startFakeReplServer(t)
	defer stop()

	l := NewLeader(0, 500, 100*time.Millisecond, nil, 2, "leader", nil)
	if err := l.AddFollower("f1", addr); err != nil {
		t.Fatalf("AddFollower: %v", err)
	}
	defer l.Stop()

	l.SetEpoch(7)

	events := []*types.Event{{MessageId: "m0", Offset: 0, Payload: []byte("a"), Topic: "t"}}
	if err := l.Replicate(events); err != nil {
		t.Fatalf("Replicate: %v", err)
	}
	if got := fake.term(); got != 7 {
		t.Fatalf("follower received term %d, want 7 (epoch not propagated to wire)", got)
	}
}

// TestLeader_GRPCReplicate_QuorumFailsWhenFollowerDown verifies that with
// minISR=2 and no reachable follower, Replicate fails closed (durability not met).
func TestLeader_GRPCReplicate_QuorumFailsWhenFollowerDown(t *testing.T) {
	// A closed port: connection is refused, so the append RPC fails fast.
	lis, _ := net.Listen("tcp", "127.0.0.1:0")
	addr := lis.Addr().String()
	lis.Close() // nothing is listening now

	l := NewLeader(0, 500, time.Hour, nil, 2, "leader", nil) // long maintenance tick so it won't reconnect
	if err := l.AddFollower("f1", addr); err != nil {
		t.Fatalf("AddFollower: %v", err)
	}
	defer l.Stop()

	events := []*types.Event{{MessageId: "m0", Offset: 0, Payload: []byte("a"), Topic: "t"}}
	err := l.Replicate(events)
	if err == nil {
		t.Fatal("expected replication quorum failure when follower is unreachable, got nil")
	}
}

// TestLeader_GRPCReplicate_RF1NoFollowers verifies RF=1 (minISR=1, no followers)
// is a no-op success.
func TestLeader_GRPCReplicate_RF1NoFollowers(t *testing.T) {
	l := NewLeader(0, 500, time.Hour, nil, 1, "leader", nil)
	defer l.Stop()
	if err := l.Replicate([]*types.Event{{Offset: 0, Payload: []byte("x")}}); err != nil {
		t.Fatalf("RF=1 no-follower Replicate should succeed, got: %v", err)
	}
}

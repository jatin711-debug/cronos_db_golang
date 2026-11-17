package replication

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
	"google.golang.org/grpc"
)

// slowFirstFollower takes every append in order and counts the requests. Its
// first append takes 200 ms, so batches queue up behind it.
type slowFirstFollower struct {
	types.UnimplementedReplicationServiceServer
	mu       sync.Mutex
	nextOff  int64
	requests atomic.Int32
	offsets  []int64
}

func (s *slowFirstFollower) Append(_ context.Context, req *types.ReplicationAppendRequest) (*types.ReplicationAppendResponse, error) {
	if s.requests.Add(1) == 1 {
		time.Sleep(200 * time.Millisecond)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if events := req.GetEvents(); len(events) > 0 {
		if events[0].GetOffset() != s.nextOff {
			return &types.ReplicationAppendResponse{Error: "offset mismatch", NextOffset: s.nextOff, LastOffset: s.nextOff - 1}, nil
		}
		for _, e := range events {
			s.offsets = append(s.offsets, e.GetOffset())
		}
		s.nextOff = events[len(events)-1].GetOffset() + 1
	}
	return &types.ReplicationAppendResponse{Success: true, LastOffset: s.nextOff - 1, NextOffset: s.nextOff}, nil
}

func (s *slowFirstFollower) SyncConsumerProgress(context.Context, *types.ReplicationProgressRequest) (*types.ReplicationProgressResponse, error) {
	return &types.ReplicationProgressResponse{Success: true}, nil
}

// Publishes queue their sends in order and wait for the quorum afterwards,
// and the batches queued behind a slow append go to the follower together,
// in order, in one request.
func TestLeader_QueuedSendsGoTogether(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	follower := &slowFirstFollower{}
	srv := grpc.NewServer()
	types.RegisterReplicationServiceServer(srv, follower)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	wal := newLeaderTestWAL(t)
	const batches = 20
	events := loggedAt(t, wal, 0, batches)

	l := NewLeader(0, 500, time.Hour, wal, 2, "leader", nil)
	if err := l.AddFollower("f1", lis.Addr().String()); err != nil {
		t.Fatal(err)
	}
	defer l.Stop()

	// As the publish path does: start each in order, wait afterwards.
	pending := make([]*PendingReplication, 0, batches)
	for i := 0; i < batches; i++ {
		p, err := l.StartReplication(events[i : i+1])
		if err != nil {
			t.Fatal(err)
		}
		pending = append(pending, p)
	}
	for i, p := range pending {
		if err := p.Wait(); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}

	follower.mu.Lock()
	got := append([]int64(nil), follower.offsets...)
	follower.mu.Unlock()
	if len(got) != batches {
		t.Fatalf("the follower holds %d entries, want %d", len(got), batches)
	}
	for i, offset := range got {
		if offset != int64(i) {
			t.Fatalf("entry %d is offset %d: out of order", i, offset)
		}
	}
	if n := follower.requests.Load(); n > 3 {
		t.Fatalf("%d append requests for %d batches; those queued behind the slow one should go together", n, batches)
	}
}

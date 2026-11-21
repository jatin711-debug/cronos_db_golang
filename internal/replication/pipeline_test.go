package replication

import (
	"context"
	"fmt"
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

// steppedFollower takes every append in order, and answers each request that
// carries events only when the test lets it.
type steppedFollower struct {
	types.UnimplementedReplicationServiceServer
	mu      sync.Mutex
	nextOff int64
	arrived chan int      // the number of events in each request, as it arrives
	answer  chan struct{} // one token lets one request answer
}

func (s *steppedFollower) Append(ctx context.Context, req *types.ReplicationAppendRequest) (*types.ReplicationAppendResponse, error) {
	events := req.GetEvents()
	if len(events) > 0 {
		s.arrived <- len(events)
		select {
		case <-s.answer:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(events) > 0 {
		if events[0].GetOffset() != s.nextOff {
			return &types.ReplicationAppendResponse{Error: "offset mismatch", NextOffset: s.nextOff, LastOffset: s.nextOff - 1}, nil
		}
		s.nextOff = events[len(events)-1].GetOffset() + 1
	}
	return &types.ReplicationAppendResponse{Success: true, LastOffset: s.nextOff - 1, NextOffset: s.nextOff}, nil
}

func (s *steppedFollower) SyncConsumerProgress(context.Context, *types.ReplicationProgressRequest) (*types.ReplicationProgressResponse, error) {
	return &types.ReplicationProgressResponse{Success: true}, nil
}

// A publish whose batch went to the follower in an earlier request is told so
// as soon as that request is answered. It used to wait for one more request
// for every send ahead of it in the queue: each of those took whatever had
// been queued since and sent it before it finished.
func TestLeader_ACarriedBatchDoesNotWaitForLaterSends(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	follower := &steppedFollower{arrived: make(chan int, 16), answer: make(chan struct{}, 16)}
	srv := grpc.NewServer()
	types.RegisterReplicationServiceServer(srv, follower)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	wal := newLeaderTestWAL(t)
	events := loggedAt(t, wal, 0, 5)
	l := NewLeader(0, 500, time.Hour, wal, 2, "leader", nil)
	if err := l.AddFollower("f1", lis.Addr().String()); err != nil {
		t.Fatal(err)
	}
	defer l.Stop()

	start := func(i int) *PendingReplication {
		t.Helper()
		p, err := l.StartReplication(events[i : i+1])
		if err != nil {
			t.Fatal(err)
		}
		return p
	}
	arrives := func(want int, what string) {
		t.Helper()
		select {
		case got := <-follower.arrived:
			if got != want {
				t.Fatalf("%s carries %d events, want %d", what, got, want)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("%s never reached the follower", what)
		}
	}
	settles := func(p *PendingReplication, what string) {
		t.Helper()
		done := make(chan error, 1)
		go func() { done <- p.Wait() }()
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("%s: %v", what, err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("%s: still waiting, though the follower has acknowledged it", what)
		}
	}

	// Batch 0 is on its way; 1, 2 and 3 queue behind it.
	pending := []*PendingReplication{start(0)}
	arrives(1, "the first request")
	for i := 1; i <= 3; i++ {
		pending = append(pending, start(i))
	}
	follower.answer <- struct{}{}
	settles(pending[0], "batch 0")

	// They go together. While that request is out, batch 4 is published.
	arrives(3, "the second request")
	pending = append(pending, start(4))
	follower.answer <- struct{}{}

	// The follower now holds 0-3. Batch 4 goes in a third request, which the
	// follower does not answer yet; batches 1-3 do not wait for it.
	arrives(1, "the third request")
	for i := 1; i <= 3; i++ {
		settles(pending[i], fmt.Sprintf("batch %d", i))
	}
	follower.answer <- struct{}{}
	settles(pending[4], "batch 4")
}

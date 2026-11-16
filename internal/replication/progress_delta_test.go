package replication

import (
	"context"
	"net"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
	"google.golang.org/grpc"
)

// progressRecordingServer is a follower that takes every entry and keeps the
// consumer progress requests it is sent.
type progressRecordingServer struct {
	progressCountingServer
	mu       sync.Mutex
	requests []*types.ReplicationProgressRequest
}

func (s *progressRecordingServer) SyncConsumerProgress(_ context.Context, req *types.ReplicationProgressRequest) (*types.ReplicationProgressResponse, error) {
	s.mu.Lock()
	s.requests = append(s.requests, req)
	s.mu.Unlock()
	return &types.ReplicationProgressResponse{Success: true}, nil
}

func (s *progressRecordingServer) sent() []*types.ReplicationProgressRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.requests)
}

// After a follower has taken the full copy of the consumer progress, it is
// sent what changed after the version it took, not the full copy again.
func TestLeader_ProgressGoesAsDeltasAfterTheFullCopy(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	follower := &progressRecordingServer{}
	srv := grpc.NewServer()
	types.RegisterReplicationServiceServer(srv, follower)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	wal := newLeaderTestWAL(t)
	loggedAt(t, wal, 0, 6)

	var version atomic.Uint64
	version.Store(1)
	var pages atomic.Int32
	var mu sync.Mutex
	var sinces []uint64

	l := NewLeader(0, 500, 20*time.Millisecond, wal, 2, "leader", nil)
	l.SetProgressPageSource(version.Load, func(types.ProgressCursor) ([]*types.ConsumerGroupProgress, types.ProgressCursor, bool, error) {
		pages.Add(1)
		return []*types.ConsumerGroupProgress{{GroupId: "g", Topic: "t", CommittedOffset: 0, CompletedOffsets: []int64{1, 2, 3}}}, types.ProgressCursor{}, true, nil
	})
	l.SetProgressDeltaSource(func(since uint64) ([]*types.ConsumerGroupProgress, uint64, bool) {
		mu.Lock()
		sinces = append(sinces, since)
		mu.Unlock()
		v := version.Load()
		return []*types.ConsumerGroupProgress{{GroupId: "g", Topic: "t", CommittedOffset: 0, CompletedOffsets: []int64{int64(v)}}}, v, true
	})
	if err := l.AddFollower("f1", lis.Addr().String()); err != nil {
		t.Fatal(err)
	}
	l.Start()
	defer l.Stop()

	waitFor(t, "the follower to hold the log", func() bool { return l.QuorumOffset() == 5 })
	waitFor(t, "the full copy", func() bool { return pages.Load() >= 1 })
	// A copy sent before the follower held the log goes again once it does.
	time.Sleep(200 * time.Millisecond)
	full := pages.Load()

	version.Store(4)
	waitFor(t, "a delta", func() bool { mu.Lock(); defer mu.Unlock(); return len(sinces) >= 1 })
	time.Sleep(200 * time.Millisecond)
	if got := pages.Load(); got != full {
		t.Fatalf("the full copy was sent again (%d times, was %d) instead of a delta", got, full)
	}
	mu.Lock()
	first := sinces[0]
	mu.Unlock()
	if first != 1 {
		t.Fatalf("the delta was built from version %d, want 1: the version of the full copy", first)
	}
	last := follower.sent()
	if got := last[len(last)-1].GetGroups()[0].GetCompletedOffsets(); !slices.Equal(got, []int64{4}) {
		t.Fatalf("the delta named %v, want [4]", got)
	}

	// The next change goes from the version the delta brought it to.
	version.Store(5)
	waitFor(t, "another delta", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return slices.Contains(sinces, 4)
	})
	if got := pages.Load(); got != full {
		t.Fatalf("the full copy was sent again (%d times, was %d) instead of a delta", got, full)
	}
}

// When no delta can be built, the follower gets the full copy instead.
func TestLeader_ProgressFallsBackToTheFullCopy(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	follower := &progressRecordingServer{}
	srv := grpc.NewServer()
	types.RegisterReplicationServiceServer(srv, follower)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	wal := newLeaderTestWAL(t)
	loggedAt(t, wal, 0, 6)

	var version atomic.Uint64
	version.Store(1)
	var pages, deltas atomic.Int32
	l := NewLeader(0, 500, 20*time.Millisecond, wal, 2, "leader", nil)
	l.SetProgressPageSource(version.Load, func(types.ProgressCursor) ([]*types.ConsumerGroupProgress, types.ProgressCursor, bool, error) {
		pages.Add(1)
		return []*types.ConsumerGroupProgress{{GroupId: "g", Topic: "t"}}, types.ProgressCursor{}, true, nil
	})
	l.SetProgressDeltaSource(func(uint64) ([]*types.ConsumerGroupProgress, uint64, bool) {
		deltas.Add(1)
		return nil, version.Load(), false
	})
	if err := l.AddFollower("f1", lis.Addr().String()); err != nil {
		t.Fatal(err)
	}
	l.Start()
	defer l.Stop()

	waitFor(t, "the follower to hold the log", func() bool { return l.QuorumOffset() == 5 })
	waitFor(t, "the full copy", func() bool { return pages.Load() >= 1 })
	version.Store(2)
	waitFor(t, "the full copy again", func() bool { return deltas.Load() >= 1 && pages.Load() >= 2 })
}

// Completions a delta sent beyond the follower's log end go again with the
// next one, in order, without repeats, and not once the floor covers them.
func TestMergeCarry(t *testing.T) {
	groups := []*types.ConsumerGroupProgress{
		{GroupId: "a", CommittedOffset: 10, CompletedOffsets: []int64{12, 15, 20}},
		{GroupId: "b", CommittedOffset: 0},
	}
	mergeCarry(groups, map[string][]int64{"a": {8, 15, 18, 30}, "b": {7}, "gone": {1}})
	if got, want := groups[0].GetCompletedOffsets(), []int64{12, 15, 18, 20, 30}; !slices.Equal(got, want) {
		t.Fatalf("group a = %v, want %v", got, want)
	}
	if got, want := groups[1].GetCompletedOffsets(), []int64{7}; !slices.Equal(got, want) {
		t.Fatalf("group b = %v, want %v", got, want)
	}
}

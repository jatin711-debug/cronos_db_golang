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

// progressCountingServer is a follower that counts the rounds of consumer
// progress it is sent. While refusing is set it takes no entries, and says
// that its log is empty.
type progressCountingServer struct {
	types.UnimplementedReplicationServiceServer
	refusing atomic.Bool
	rounds   atomic.Int32

	mu      sync.Mutex
	nextOff int64
}

func (s *progressCountingServer) Append(_ context.Context, req *types.ReplicationAppendRequest) (*types.ReplicationAppendResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.refusing.Load() {
		return &types.ReplicationAppendResponse{Error: "not now", NextOffset: s.nextOff, LastOffset: s.nextOff - 1}, nil
	}
	if events := req.GetEvents(); len(events) > 0 {
		if events[0].GetOffset() != s.nextOff {
			return &types.ReplicationAppendResponse{Error: "offset mismatch", NextOffset: s.nextOff, LastOffset: s.nextOff - 1}, nil
		}
		s.nextOff = events[len(events)-1].GetOffset() + 1
	}
	return &types.ReplicationAppendResponse{Success: true, LastOffset: s.nextOff - 1, NextOffset: s.nextOff}, nil
}

func (s *progressCountingServer) SyncConsumerProgress(context.Context, *types.ReplicationProgressRequest) (*types.ReplicationProgressResponse, error) {
	s.rounds.Add(1)
	return &types.ReplicationProgressResponse{Success: true}, nil
}

// A follower takes its leader's consumer progress only as far as its own log
// reaches. If it was behind when the current progress was sent, it has to be
// sent again once it has caught up: the progress may not change for a long
// time, and until the next routine round the follower would lead, if it came
// to that, without knowing what was finished.
func TestLeader_ProgressIsSentAgainToAFollowerThatHasCaughtUp(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	follower := &progressCountingServer{}
	follower.refusing.Store(true)
	srv := grpc.NewServer()
	types.RegisterReplicationServiceServer(srv, follower)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	wal := newLeaderTestWAL(t)
	loggedAt(t, wal, 0, 6)

	l := NewLeader(0, 500, 20*time.Millisecond, wal, 2, "leader", nil)
	// Offsets 0 to 5 are finished, and nothing changes after that.
	l.SetProgressSource(func() (uint64, []*types.ConsumerGroupProgress) {
		return 7, []*types.ConsumerGroupProgress{{GroupId: "g", Topic: "t", CommittedOffset: 6}}
	})
	if err := l.AddFollower("f1", lis.Addr().String()); err != nil {
		t.Fatal(err)
	}
	l.Start()
	defer l.Stop()

	// The follower takes no entries yet, so it holds nothing of the log.
	waitFor(t, "the first round of progress", func() bool { return follower.rounds.Load() >= 1 })
	time.Sleep(300 * time.Millisecond)
	if got := follower.rounds.Load(); got != 1 {
		t.Fatalf("%d rounds of unchanged progress went to a follower that is still behind, want 1", got)
	}

	follower.refusing.Store(false)
	waitFor(t, "the follower to hold the log", func() bool { return l.QuorumOffset() == 5 })
	waitFor(t, "a second round once the follower has caught up", func() bool { return follower.rounds.Load() >= 2 })
	time.Sleep(300 * time.Millisecond)
	if got := follower.rounds.Load(); got != 2 {
		t.Fatalf("%d rounds of unchanged progress, want 2: one while behind and one when caught up", got)
	}
}

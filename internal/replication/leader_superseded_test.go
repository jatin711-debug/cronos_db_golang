package replication

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/internal/storage"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
	"google.golang.org/grpc"
)

// movedOnReplServer is a follower that takes appends until it is told that a
// newer leader exists. From then on it refuses the old leader's, and reports
// the log it has by now: longer than the old leader's, and not the same.
type movedOnReplServer struct {
	types.UnimplementedReplicationServiceServer
	mu      sync.Mutex
	nextOff int64
	term    int64 // of the newer leader, 0 while there is none
	silent  bool  // refuse without naming the newer term
	appends int   // requests that carried entries
}

func (s *movedOnReplServer) Append(_ context.Context, req *types.ReplicationAppendRequest) (*types.ReplicationAppendResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(req.GetEvents()) > 0 {
		s.appends++
	}
	if s.term > req.GetTerm() {
		resp := &types.ReplicationAppendResponse{
			Error:      "invalid or stale term",
			NextOffset: s.nextOff,
			LastOffset: s.nextOff - 1,
			Term:       s.term,
		}
		if s.silent {
			resp.Term = 0
		}
		return resp, nil
	}
	if events := req.GetEvents(); len(events) > 0 {
		if events[0].GetOffset() != s.nextOff {
			return &types.ReplicationAppendResponse{Error: "offset mismatch", NextOffset: s.nextOff, LastOffset: s.nextOff - 1, Term: req.GetTerm()}, nil
		}
		s.nextOff = events[len(events)-1].GetOffset() + 1
	}
	return &types.ReplicationAppendResponse{Success: true, LastOffset: s.nextOff - 1, NextOffset: s.nextOff, Term: req.GetTerm()}, nil
}

// followNewerLeader makes the follower behave as after it accepted a leader of
// term, which wrote grown more entries to it.
func (s *movedOnReplServer) followNewerLeader(term, grown int64) {
	s.mu.Lock()
	s.term = term
	s.nextOff += grown
	s.mu.Unlock()
}

func (s *movedOnReplServer) appendsSeen() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.appends
}

func startMovedOnReplServer(t *testing.T) (*movedOnReplServer, string) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	fake := &movedOnReplServer{}
	srv := grpc.NewServer()
	types.RegisterReplicationServiceServer(srv, fake)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return fake, lis.Addr().String()
}

// loggedAt appends n entries from offset start to the leader's log and returns
// them, as a publish does before it replicates.
func loggedAt(t *testing.T, wal *storage.WAL, start, n int64) []*types.Event {
	t.Helper()
	events := batchAt(start, n)
	for _, e := range events {
		e.ScheduleTs = 1
	}
	if err := wal.AppendBatch(events); err != nil {
		t.Fatal(err)
	}
	return events
}

// A leader that was cut off comes back to followers that follow its successor
// and hold entries the successor wrote. A refused append tells it where such a
// follower's log ends now. That used to be taken for how much of this leader's
// own log the follower holds: the next publish was at an offset the follower
// had passed, so nothing was sent and the follower counted as having confirmed
// it. The publish was acknowledged to its producer, and then removed when this
// node took the successor's log.
//
// The follower here does not say which term it follows, so that the leader
// has nothing to go by but the refusal: where a follower's log ends is no
// evidence of what is in it, whatever the reason it gave.
func TestLeader_FollowerOfANewerLeaderConfirmsNothing(t *testing.T) {
	follower, addr := startMovedOnReplServer(t)
	follower.silent = true
	wal := newLeaderTestWAL(t)

	l := NewLeader(0, 500, time.Hour, wal, 2, "old-leader", nil)
	l.SetEpoch(9)
	defer l.Stop()
	if err := l.AddFollower("f1", addr); err != nil {
		t.Fatal(err)
	}
	if err := l.Replicate(loggedAt(t, wal, 0, 3)); err != nil {
		t.Fatalf("replicate while this is the leader: %v", err)
	}
	if got := l.QuorumOffset(); got != 2 {
		t.Fatalf("QuorumOffset = %d, want 2", got)
	}

	// The follower accepts a leader of term 11, which writes offsets 3 to 5.
	follower.followNewerLeader(11, 3)

	if err := l.Replicate(loggedAt(t, wal, 3, 1)); err == nil {
		t.Fatal("a publish of the old leader counted as replicated to a follower of the new one")
	}
	// The refusal has told the old leader that the follower's log ends at 5.
	if err := l.Replicate(loggedAt(t, wal, 4, 1)); err == nil {
		t.Fatal("a publish of the old leader counted as replicated because the follower's log, written by the new leader, reaches past it")
	}
	if got := l.QuorumOffset(); got != 2 {
		t.Fatalf("the old leader counts offset %d as held by a quorum; the follower confirmed its log up to 2", got)
	}
}

// What a follower answers shows which term it follows. A leader that learns of
// a newer one from it stops: it refuses publishes, counts nothing more as
// replicated, and tells its partition, without waiting for the cluster's
// records to reach this node.
func TestLeader_StepsDownWhenAFollowerNamesANewerTerm(t *testing.T) {
	follower, addr := startMovedOnReplServer(t)
	wal := newLeaderTestWAL(t)

	l := NewLeader(0, 500, time.Hour, wal, 2, "old-leader", nil)
	l.SetEpoch(9)
	defer l.Stop()
	superseded := make(chan int64, 4)
	l.SetSupersededObserver(func(term int64) { superseded <- term })
	if err := l.AddFollower("f1", addr); err != nil {
		t.Fatal(err)
	}
	if err := l.Replicate(loggedAt(t, wal, 0, 3)); err != nil {
		t.Fatalf("replicate while this is the leader: %v", err)
	}
	if got := l.QuorumOffset(); got != 2 {
		t.Fatalf("QuorumOffset = %d, want 2", got)
	}

	follower.followNewerLeader(11, 0)
	held := loggedAt(t, wal, 3, 1)
	if err := l.Replicate(held); err == nil {
		t.Fatal("replicated to a follower of a newer leader")
	}
	select {
	case term := <-superseded:
		if term != 11 {
			t.Fatalf("told of term %d, want 11", term)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the leader did not report that a newer term exists")
	}
	if got := l.NewerTerm(); got != 11 {
		t.Fatalf("NewerTerm = %d, want 11", got)
	}
	if got := l.QuorumOffset(); got != -1 {
		t.Fatalf("a superseded leader counts offset %d as held by a quorum, want -1", got)
	}
	if got := l.ReplicatedThrough(); got != -1 {
		t.Fatalf("a superseded leader counts offset %d as replicated, want -1", got)
	}

	// It does not ask the follower again: a publish is refused at once.
	sent := follower.appendsSeen()
	if err := l.Replicate(held); err == nil {
		t.Fatal("a superseded leader replicated a publish")
	}
	if got := follower.appendsSeen(); got != sent {
		t.Fatalf("a superseded leader sent %d more appends", got-sent)
	}
	select {
	case term := <-superseded:
		t.Fatalf("the partition was told a second time, of term %d", term)
	case <-time.After(50 * time.Millisecond):
	}

	// Leading again under a term at least as new clears it.
	l.SetEpoch(12)
	if got := l.NewerTerm(); got != 0 {
		t.Fatalf("NewerTerm after taking term 12 = %d, want 0", got)
	}
}

// What the required replicas held once is part of the partition's log from
// then on. A leader whose follower goes away knows nothing of what followers
// hold now, and still delivers what was replicated before: otherwise a
// partition that has lost a replica would stop delivering events that every
// later leader has.
func TestLeader_ReplicatedThroughOutlastsTheFollower(t *testing.T) {
	_, addr := startMovedOnReplServer(t)
	wal := newLeaderTestWAL(t)

	l := NewLeader(0, 500, time.Hour, wal, 2, "leader", nil)
	l.SetEpoch(9)
	defer l.Stop()
	if got := l.ReplicatedThrough(); got != -1 {
		t.Fatalf("ReplicatedThrough with no followers = %d, want -1", got)
	}
	if err := l.AddFollower("f1", addr); err != nil {
		t.Fatal(err)
	}
	if err := l.Replicate(loggedAt(t, wal, 0, 3)); err != nil {
		t.Fatal(err)
	}
	if got := l.ReplicatedThrough(); got != 2 {
		t.Fatalf("ReplicatedThrough = %d, want 2", got)
	}

	if err := l.RemoveFollower("f1"); err != nil {
		t.Fatal(err)
	}
	loggedAt(t, wal, 3, 2) // written, and on no other replica
	if got := l.QuorumOffset(); got != -1 {
		t.Fatalf("QuorumOffset with the follower gone = %d, want -1", got)
	}
	if got := l.ReplicatedThrough(); got != 2 {
		t.Fatalf("ReplicatedThrough with the follower gone = %d, want 2: offsets 0 to 2 were replicated", got)
	}
}

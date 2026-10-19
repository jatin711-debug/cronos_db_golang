package replication

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/internal/lifecycle"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
	"google.golang.org/grpc"
)

// lifecycleReplServer is a follower that implements AppendLifecycle as well,
// and counts which of the two appends it was sent.
type lifecycleReplServer struct {
	fakeReplServer
	lmu         sync.Mutex
	plainCalls  int
	lifecycle   int
	lastFormat  uint32
	recordsSeen int
}

func (s *lifecycleReplServer) Append(ctx context.Context, req *types.ReplicationAppendRequest) (*types.ReplicationAppendResponse, error) {
	s.lmu.Lock()
	s.plainCalls++
	s.lmu.Unlock()
	return s.fakeReplServer.Append(ctx, req)
}

func (s *lifecycleReplServer) AppendLifecycle(ctx context.Context, req *types.ReplicationLifecycleAppendRequest) (*types.ReplicationAppendResponse, error) {
	s.lmu.Lock()
	s.lifecycle++
	s.lastFormat = req.GetLifecycleFormat()
	for _, e := range req.GetAppend().GetEvents() {
		if lifecycle.HasReservedMeta(e) {
			s.recordsSeen++
		}
	}
	s.lmu.Unlock()
	return s.fakeReplServer.Append(ctx, req.GetAppend())
}

func (s *lifecycleReplServer) calls() (plain, versioned int, format uint32, records int) {
	s.lmu.Lock()
	defer s.lmu.Unlock()
	return s.plainCalls, s.lifecycle, s.lastFormat, s.recordsSeen
}

func serveRepl(t *testing.T, srv types.ReplicationServiceServer) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	g := grpc.NewServer()
	types.RegisterReplicationServiceServer(g, srv)
	go func() { _ = g.Serve(lis) }()
	t.Cleanup(g.Stop)
	return lis.Addr().String()
}

// withRecord is two ordinary events and, at offset 2, a lifecycle record.
func withRecord(t *testing.T) []*types.Event {
	t.Helper()
	original := &types.Event{MessageId: "m-0", Topic: "t", PartitionId: 0, Offset: 0, Term: 1, ScheduleTs: time.Now().Add(time.Hour).UnixMilli(), Payload: []byte("a")}
	cancel, err := lifecycle.Replacement(original, lifecycle.Cancel, "req-1", 0)
	if err != nil {
		t.Fatal(err)
	}
	cancel.Offset, cancel.Term = 2, 1
	return []*types.Event{original, {MessageId: "m-1", Topic: "t", Offset: 1, Term: 1, Payload: []byte("b")}, cancel}
}

// A leader whose log has a lifecycle format sends everything through
// AppendLifecycle, with the format; the plain append is never used.
func TestLeader_LifecycleLogGoesOnlyThroughAppendLifecycle(t *testing.T) {
	follower := &lifecycleReplServer{}
	l := NewLeader(0, 500, 100*time.Millisecond, nil, 2, "leader", nil)
	l.SetLifecycleFormat(1)
	if err := l.AddFollower("f1", serveRepl(t, follower)); err != nil {
		t.Fatal(err)
	}
	defer l.Stop()

	if err := l.Replicate(withRecord(t)); err != nil {
		t.Fatalf("Replicate: %v", err)
	}
	plain, versioned, format, records := follower.calls()
	if plain != 0 || versioned == 0 || format != 1 || records != 1 {
		t.Fatalf("plain=%d versioned=%d format=%d records=%d; want only versioned appends of format 1 carrying the record", plain, versioned, format, records)
	}
}

// Without a lifecycle format a leader refuses to send a lifecycle record at
// all: through the plain append a follower of an older build would store it
// as an event.
func TestLeader_RefusesLifecycleRecordsWithoutAFormat(t *testing.T) {
	follower := &lifecycleReplServer{}
	l := NewLeader(0, 500, 100*time.Millisecond, nil, 2, "leader", nil)
	if err := l.AddFollower("f1", serveRepl(t, follower)); err != nil {
		t.Fatal(err)
	}
	defer l.Stop()

	if err := l.Replicate(withRecord(t)); err == nil {
		t.Fatal("a batch with a lifecycle record was replicated without a lifecycle format")
	}
	if plain, versioned, _, _ := follower.calls(); plain != 0 || versioned != 0 || follower.count() != 0 {
		t.Fatalf("the follower was sent %d plain and %d versioned appends and holds %d events; want nothing", plain, versioned, follower.count())
	}
}

// A follower of a build without AppendLifecycle answers Unimplemented and is
// sent nothing of a lifecycle log, not even its ordinary events: it neither
// holds a copy nor counts towards a quorum.
func TestLeader_FollowerWithoutAppendLifecycleGetsNothing(t *testing.T) {
	old := &fakeReplServer{}
	l := NewLeader(0, 500, 100*time.Millisecond, nil, 2, "leader", nil)
	l.SetLifecycleFormat(1)
	l.SetReplicateTimeout(2 * time.Second)
	if err := l.AddFollower("old", serveRepl(t, old)); err != nil {
		t.Fatal(err)
	}
	defer l.Stop()

	ordinary := []*types.Event{{MessageId: "m-0", Topic: "t", Offset: 0, Term: 1, Payload: []byte("a")}}
	if err := l.Replicate(ordinary); err == nil {
		t.Fatal("a quorum of two was met with the only follower unable to take the log")
	}
	if old.count() != 0 {
		t.Fatalf("the follower without AppendLifecycle holds %d events", old.count())
	}
	if isr := l.GetInSyncReplicas(); len(isr) != 0 {
		t.Fatalf("in-sync replicas = %v, want none", isr)
	}
}

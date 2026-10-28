package replication

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/internal/logcontrol"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
	"google.golang.org/grpc"
)

// termRetryServer is a follower that holds offsets [0, next) and accepts any
// contiguous AppendTermStart batch that starts at next.
type termRetryServer struct {
	types.UnimplementedReplicationServiceServer
	mu   sync.Mutex
	next int64
	got  []int64
}

func (s *termRetryServer) AppendTermStart(_ context.Context, treq *types.ReplicationTermAppendRequest) (*types.ReplicationAppendResponse, error) {
	req := treq.GetAppend()
	s.mu.Lock()
	defer s.mu.Unlock()
	evs := req.GetEvents()
	if len(evs) > 0 && evs[0].GetOffset() != s.next {
		return &types.ReplicationAppendResponse{Error: "gap", LastOffset: s.next - 1, NextOffset: s.next, Term: req.GetTerm()}, nil
	}
	for _, e := range evs {
		s.got = append(s.got, e.GetOffset())
		s.next = e.GetOffset() + 1
	}
	return &types.ReplicationAppendResponse{Success: true, LastOffset: s.next - 1, NextOffset: s.next, Term: req.GetTerm()}, nil
}

// A publish retried after its leader took over again concerns an entry of an
// earlier term, before this term's start record. Replicate used to send that
// entry alone and report success once a quorum held it, while the leader's
// own commit proof said it was not committed: an entry of an earlier term is
// committed only together with one of the current term.
func TestReplicateOfEarlierTermEntryCommitsIt(t *testing.T) {
	w := newLeaderTestWAL(t)
	l := NewLeader(0, 500, time.Hour, w, 2, "a", nil) // no maintenance loop
	defer l.Stop()

	// Term 1 prefix that every replica holds: offsets 0..2.
	for i := 0; i < 3; i++ {
		if err := w.AppendEvent(&types.Event{MessageId: "base", Payload: []byte("b"), Topic: "t", ScheduleTs: 1}); err != nil {
			t.Fatal(err)
		}
	}
	// Term 2: this node appended y at offset 3; its publish failed.
	l.SetEpoch(2)
	y := &types.Event{MessageId: "y", Payload: []byte("y"), Topic: "t", ScheduleTs: 1}
	if err := w.AppendEvent(y); err != nil {
		t.Fatal(err)
	}
	// Re-promoted in term 4 with term-start mode: barrier at offset 4.
	l.SetEpoch(4)
	l.SetTermStartFormat(logcontrol.TermStartFormat)
	barrier := logcontrol.NewTermStart(0)
	if err := w.AppendEvent(barrier); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	l.SetTermStartOffset(barrier.Offset, 4)

	// Follower C holds 0..2 (term 1).
	srv := &termRetryServer{next: 3}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	g := grpc.NewServer()
	types.RegisterReplicationServiceServer(g, srv)
	go func() { _ = g.Serve(lis) }()
	defer g.Stop()
	if err := l.AddFollower("c", lis.Addr().String()); err != nil {
		t.Fatal(err)
	}

	// The producer retries y: finishPriorPublishes calls Replicate([y]).
	logged, err := w.ReadEvent(3)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("y at offset %d term %d; barrier at offset %d term %d", logged.Offset, logged.Term, barrier.Offset, barrier.Term)
	if err := l.Replicate([]*types.Event{logged}); err != nil {
		t.Fatalf("Replicate: %v", err)
	}
	srv.mu.Lock()
	sent := append([]int64(nil), srv.got...)
	srv.mu.Unlock()
	if fmt.Sprint(sent) != "[3 4]" {
		t.Fatalf("the follower received offsets %v, want [3 4]: the entry and this term's start record", sent)
	}
	if committed := l.QuorumOffset(); committed < logged.Offset {
		t.Fatalf("Replicate reported success for offset %d while the commit proof is %d", logged.Offset, committed)
	}
}

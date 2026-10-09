package replication

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
	"google.golang.org/grpc"
)

// sizedReplServer accepts appends in order, as a follower does, and records how
// many payload bytes each request carried, so a test can see how large the
// requests a catch-up sends are.
type sizedReplServer struct {
	types.UnimplementedReplicationServiceServer
	mu       sync.Mutex
	nextOff  int64
	offsets  []int64
	requests []int // payload bytes of each append that carried events
}

func (s *sizedReplServer) Append(_ context.Context, req *types.ReplicationAppendRequest) (*types.ReplicationAppendResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if req.GetExpectedNextOffset() != s.nextOff {
		return &types.ReplicationAppendResponse{Error: "offset mismatch", NextOffset: s.nextOff, LastOffset: s.nextOff - 1}, nil
	}
	size := 0
	for _, e := range req.GetEvents() {
		size += len(e.GetPayload())
		s.offsets = append(s.offsets, e.GetOffset())
		s.nextOff = e.GetOffset() + 1
	}
	if len(req.GetEvents()) > 0 {
		s.requests = append(s.requests, size)
	}
	return &types.ReplicationAppendResponse{Success: true, LastOffset: s.nextOff - 1, NextOffset: s.nextOff}, nil
}

// startSizedReplServer serves the fake follower with the message limit the
// replication transport uses between nodes.
func startSizedReplServer(t *testing.T) (*sizedReplServer, string) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	fake := &sizedReplServer{}
	srv := grpc.NewServer(grpc.MaxRecvMsgSize(64 << 20))
	types.RegisterReplicationServiceServer(srv, fake)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return fake, lis.Addr().String()
}

// A follower that starts empty is caught up from the leader's log, and that
// catch-up sends requests of at most catchUpBytes, plus one event that is not
// split. Events of a megabyte each, five hundred to a request, would be over
// the transport's limit, and a follower that cannot take a request from its
// leader never catches up.
func TestLeader_CatchUpSendsRequestsBoundedByBytes(t *testing.T) {
	wal := newLeaderTestWAL(t)
	const count, size = 40, 1 << 20 // 40 MiB of log
	events := make([]*types.Event, count)
	for i := range events {
		events[i] = &types.Event{MessageId: fmt.Sprintf("m%d", i), Topic: "t", ScheduleTs: 1, Payload: bytes.Repeat([]byte{byte(i)}, size)}
	}
	if err := wal.AppendBatch(events); err != nil {
		t.Fatal(err)
	}

	follower, addr := startSizedReplServer(t)
	l := NewLeader(0, 500, time.Hour, wal, 2, "leader", nil)
	defer l.Stop()
	if err := l.AddFollower("late", addr); err != nil {
		t.Fatal(err)
	}
	if err := l.Replicate(events[count-1:]); err != nil {
		t.Fatalf("replicate to a follower that starts empty: %v", err)
	}

	follower.mu.Lock()
	defer follower.mu.Unlock()
	if len(follower.offsets) != count {
		t.Fatalf("follower holds %d events, want %d", len(follower.offsets), count)
	}
	for i, offset := range follower.offsets {
		if offset != int64(i) {
			t.Fatalf("follower log is out of order: %v", follower.offsets)
		}
	}
	if len(follower.requests) < 2 {
		t.Fatalf("catch-up of %d MiB went in %d request; the bound of %d MiB never applied", count, len(follower.requests), catchUpBytes>>20)
	}
	for i, got := range follower.requests {
		if got > catchUpBytes+size {
			t.Fatalf("request %d carried %d bytes, over the bound of %d plus one event", i, got, catchUpBytes)
		}
	}
}

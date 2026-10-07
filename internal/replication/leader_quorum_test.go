package replication

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/internal/storage"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
	"google.golang.org/grpc"
)

// gatedReplServer behaves like fakeReplServer but holds every Append until its
// gate is opened, modelling a follower that is slow or stalled.
type gatedReplServer struct {
	types.UnimplementedReplicationServiceServer
	mu       sync.Mutex
	gate     chan struct{}
	offsets  []int64
	nextOff  int64
	rejected int
}

func (s *gatedReplServer) Append(ctx context.Context, req *types.ReplicationAppendRequest) (*types.ReplicationAppendResponse, error) {
	select {
	case <-s.gate:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if req.GetExpectedNextOffset() != s.nextOff {
		s.rejected++
		return &types.ReplicationAppendResponse{Error: "offset mismatch", NextOffset: s.nextOff, LastOffset: s.nextOff - 1}, nil
	}
	for _, e := range req.GetEvents() {
		s.offsets = append(s.offsets, e.GetOffset())
		s.nextOff = e.GetOffset() + 1
	}
	return &types.ReplicationAppendResponse{Success: true, LastOffset: s.nextOff - 1, NextOffset: s.nextOff}, nil
}

func (s *gatedReplServer) snapshot() (offsets []int64, rejected int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]int64(nil), s.offsets...), s.rejected
}

func startGatedReplServer(t *testing.T, open bool) (*gatedReplServer, string) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	fake := &gatedReplServer{gate: make(chan struct{})}
	if open {
		close(fake.gate)
	}
	srv := grpc.NewServer()
	types.RegisterReplicationServiceServer(srv, fake)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return fake, lis.Addr().String()
}

func batchAt(start, n int64) []*types.Event {
	events := make([]*types.Event, n)
	for i := range events {
		offset := start + int64(i)
		events[i] = &types.Event{MessageId: fmt.Sprintf("m%d", offset), Offset: offset, Payload: []byte("payload"), Topic: "t"}
	}
	return events
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// A stalled follower must not hold a publish once the other replica has acked,
// and it must still receive every batch, in order, when it recovers.
func TestLeader_ReplicateReturnsOnQuorumWithSlowFollower(t *testing.T) {
	_, fastAddr := startGatedReplServer(t, true)
	slow, slowAddr := startGatedReplServer(t, false)

	l := NewLeader(0, 500, time.Hour, nil, 2, "leader", nil)
	defer l.Stop()
	if err := l.AddFollower("fast", fastAddr); err != nil {
		t.Fatal(err)
	}
	if err := l.AddFollower("slow", slowAddr); err != nil {
		t.Fatal(err)
	}

	for batch := int64(0); batch < 3; batch++ {
		done := make(chan error, 1)
		go func() { done <- l.Replicate(batchAt(batch*2, 2)) }()
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("batch %d: quorum of leader+fast follower should succeed: %v", batch, err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("batch %d: Replicate waited for the stalled follower", batch)
		}
	}

	close(slow.gate)
	waitFor(t, "stalled follower to receive every batch", func() bool {
		offsets, _ := slow.snapshot()
		return len(offsets) == 6
	})
	offsets, rejected := slow.snapshot()
	for i, offset := range offsets {
		if offset != int64(i) {
			t.Fatalf("stalled follower received offsets out of order: %v", offsets)
		}
	}
	if rejected != 0 {
		t.Fatalf("queued batches should arrive in order without rejections, got %d", rejected)
	}
}

// A follower that falls further behind than the pending-send bound is skipped,
// then repaired from the leader's WAL by a later send.
func TestLeader_SkippedFollowerCatchesUpFromWAL(t *testing.T) {
	wal, err := storage.NewWAL(t.TempDir(), 0, &storage.WALConfig{SegmentSizeBytes: 1 << 20, IndexInterval: 1, FsyncMode: "periodic", FlushIntervalMS: 10}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()

	_, fastAddr := startGatedReplServer(t, true)
	slow, slowAddr := startGatedReplServer(t, false)

	l := NewLeader(0, 500, time.Hour, wal, 2, "leader", nil)
	defer l.Stop()
	if err := l.AddFollower("fast", fastAddr); err != nil {
		t.Fatal(err)
	}
	if err := l.AddFollower("slow", slowAddr); err != nil {
		t.Fatal(err)
	}

	const batches = maxPendingSendsPerFollower + 3
	publish := func(batch int64) {
		t.Helper()
		events := batchAt(batch*2, 2)
		for _, e := range events {
			e.ScheduleTs = 1
		}
		if err := wal.AppendBatch(events); err != nil {
			t.Fatalf("append batch %d: %v", batch, err)
		}
		if err := l.Replicate(events); err != nil {
			t.Fatalf("replicate batch %d: %v", batch, err)
		}
	}
	for batch := int64(0); batch < batches; batch++ {
		publish(batch)
	}

	// The slow follower holds maxPendingSendsPerFollower batches; the rest were
	// skipped. Once it drains, the next batch must fill the gap from the WAL.
	close(slow.gate)
	waitFor(t, "queued sends to drain", func() bool {
		offsets, _ := slow.snapshot()
		return len(offsets) == maxPendingSendsPerFollower*2
	})
	publish(batches)

	total := int((batches + 1) * 2)
	waitFor(t, "skipped batches to be caught up", func() bool {
		offsets, _ := slow.snapshot()
		return len(offsets) == total
	})
	offsets, rejected := slow.snapshot()
	for i, offset := range offsets {
		if offset != int64(i) {
			t.Fatalf("follower log has a gap or reordering after catch-up: %v", offsets)
		}
	}
	if rejected != 0 {
		t.Fatalf("leader should catch a known-behind follower up before appending, got %d rejected appends", rejected)
	}
}

// Replicate returns on quorum, so a follower that fell behind must not depend
// on the next publish to recover: the maintenance loop brings an idle follower
// up to date, and sends queued behind that catch-up do not resend its work.
func TestLeader_IdleLaggingFollowerIsCaughtUp(t *testing.T) {
	wal, err := storage.NewWAL(t.TempDir(), 0, &storage.WALConfig{SegmentSizeBytes: 1 << 20, IndexInterval: 1, FsyncMode: "periodic", FlushIntervalMS: 10}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()

	_, fastAddr := startGatedReplServer(t, true)
	slow, slowAddr := startGatedReplServer(t, false)

	l := NewLeader(0, 500, 20*time.Millisecond, wal, 2, "leader", nil)
	l.Start()
	defer l.Stop()
	if err := l.AddFollower("fast", fastAddr); err != nil {
		t.Fatal(err)
	}
	if err := l.AddFollower("slow", slowAddr); err != nil {
		t.Fatal(err)
	}

	publish := func(batch int64) {
		t.Helper()
		events := batchAt(batch*2, 2)
		for _, e := range events {
			e.ScheduleTs = 1
		}
		if err := wal.AppendBatch(events); err != nil {
			t.Fatalf("append batch %d: %v", batch, err)
		}
		if err := l.Replicate(events); err != nil {
			t.Fatalf("replicate batch %d: %v", batch, err)
		}
	}
	const batches = maxPendingSendsPerFollower + 3
	for batch := int64(0); batch < batches; batch++ {
		publish(batch)
	}

	// With no further publish, only the maintenance loop can deliver the
	// batches that were skipped while the follower was stalled.
	close(slow.gate)
	waitFor(t, "idle follower to be caught up", func() bool {
		offsets, _ := slow.snapshot()
		return len(offsets) == batches*2
	})

	// Later publishes continue from there without resending anything.
	for batch := int64(batches); batch < batches+3; batch++ {
		publish(batch)
	}
	total := int((batches + 3) * 2)
	waitFor(t, "follower to receive the later batches", func() bool {
		offsets, _ := slow.snapshot()
		return len(offsets) == total
	})
	offsets, rejected := slow.snapshot()
	for i, offset := range offsets {
		if offset != int64(i) {
			t.Fatalf("follower log has a gap or reordering after catch-up: %v", offsets)
		}
	}
	if rejected != 0 {
		t.Fatalf("leader resent offsets the follower already had: %d rejected appends", rejected)
	}
}

// The configured replication timeout bounds a quorum wait on an unresponsive
// follower instead of the previous fixed ten seconds.
func TestLeader_ReplicateTimeoutIsConfigurable(t *testing.T) {
	_, addr := startGatedReplServer(t, false) // never answers

	l := NewLeader(0, 500, time.Hour, nil, 2, "leader", nil)
	defer l.Stop()
	l.SetReplicateTimeout(200 * time.Millisecond)
	if err := l.AddFollower("stalled", addr); err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	err := l.Replicate(batchAt(0, 1))
	if err == nil {
		t.Fatal("expected quorum failure when the only follower never answers")
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("Replicate ignored the configured timeout: took %v", elapsed)
	}
}

// With minISR equal to the full replica set, one failed follower makes quorum
// impossible; Replicate must report that without waiting on the others.
func TestLeader_ReplicateFailsFastWhenQuorumImpossible(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	deadAddr := lis.Addr().String()
	lis.Close()
	_, stalledAddr := startGatedReplServer(t, false)

	l := NewLeader(0, 500, time.Hour, nil, 3, "leader", nil)
	defer l.Stop()
	if err := l.AddFollower("dead", deadAddr); err != nil {
		t.Fatal(err)
	}
	if err := l.AddFollower("stalled", stalledAddr); err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() { done <- l.Replicate(batchAt(0, 1)) }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected quorum failure")
		}
	case <-time.After(8 * time.Second):
		t.Fatal("Replicate kept waiting on a follower after quorum became impossible")
	}
}

func newLeaderTestWAL(t *testing.T) *storage.WAL {
	t.Helper()
	wal, err := storage.NewWAL(t.TempDir(), 0, &storage.WALConfig{SegmentSizeBytes: 1 << 20, IndexInterval: 1, FsyncMode: "periodic", FlushIntervalMS: 10}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { wal.Close() })
	return wal
}

// QuorumOffset is what the partition relies on to finish a publish whose
// replication failed at first: an offset counts once enough followers, besides
// the leader, have acknowledged the log up to it.
func TestLeader_QuorumOffset(t *testing.T) {
	wal := newLeaderTestWAL(t)
	fast, fastAddr := startGatedReplServer(t, true)
	slow, slowAddr := startGatedReplServer(t, false)

	// Three replicas, all three required.
	l := NewLeader(0, 500, time.Hour, wal, 3, "leader", nil)
	defer l.Stop()
	if got := l.QuorumOffset(); got != -1 {
		t.Fatalf("QuorumOffset with no followers = %d, want -1", got)
	}
	if err := l.AddFollower("fast", fastAddr); err != nil {
		t.Fatal(err)
	}
	if err := l.AddFollower("slow", slowAddr); err != nil {
		t.Fatal(err)
	}

	events := batchAt(0, 4)
	for _, e := range events {
		e.ScheduleTs = 1
	}
	if err := wal.AppendBatch(events); err != nil {
		t.Fatal(err)
	}
	replicated := make(chan error, 1)
	go func() { replicated <- l.Replicate(events) }()

	// One follower holds the batch, the other is stalled: not a quorum of three.
	waitFor(t, "the fast follower to hold the batch", func() bool {
		offsets, _ := fast.snapshot()
		return len(offsets) == 4
	})
	if got := l.QuorumOffset(); got != -1 {
		t.Fatalf("QuorumOffset = %d while a required follower has acknowledged nothing, want -1", got)
	}

	close(slow.gate)
	if err := <-replicated; err != nil {
		t.Fatalf("replicate once the stalled follower answers: %v", err)
	}
	if got := l.QuorumOffset(); got != 3 {
		t.Fatalf("QuorumOffset = %d once every follower holds offsets 0-3, want 3", got)
	}

	// With one follower enough, the one that answers decides.
	two := NewLeader(0, 500, time.Hour, wal, 2, "leader", nil)
	defer two.Stop()
	_, aheadAddr := startGatedReplServer(t, true)
	_, behindAddr := startGatedReplServer(t, false)
	if err := two.AddFollower("ahead", aheadAddr); err != nil {
		t.Fatal(err)
	}
	if err := two.AddFollower("behind", behindAddr); err != nil {
		t.Fatal(err)
	}
	if err := two.Replicate(events[3:]); err != nil {
		t.Fatalf("replicate with one of two followers answering: %v", err)
	}
	if got := two.QuorumOffset(); got != 3 {
		t.Fatalf("QuorumOffset = %d with one follower at offset 3 and min-insync 2, want 3", got)
	}
}

// A follower with an empty log reports next offset 0. The leader must take
// that at face value and ship the whole log, not keep assuming the follower is
// where the leader was when it was added.
func TestLeader_EmptyFollowerIsCaughtUpFromTheStart(t *testing.T) {
	wal := newLeaderTestWAL(t)
	events := batchAt(0, 6)
	for _, e := range events {
		e.ScheduleTs = 1
	}
	if err := wal.AppendBatch(events); err != nil {
		t.Fatal(err)
	}

	// The follower joins after the leader already holds six entries.
	follower, addr := startGatedReplServer(t, true)
	l := NewLeader(0, 500, time.Hour, wal, 2, "leader", nil)
	defer l.Stop()
	if err := l.AddFollower("late", addr); err != nil {
		t.Fatal(err)
	}
	if err := l.Replicate(events[5:]); err != nil {
		t.Fatalf("replicate to a follower that starts empty: %v", err)
	}
	offsets, _ := follower.snapshot()
	if len(offsets) != 6 {
		t.Fatalf("follower holds offsets %v, want 0-5", offsets)
	}
	for i, offset := range offsets {
		if offset != int64(i) {
			t.Fatalf("follower log is out of order: %v", offsets)
		}
	}
}

// A follower that has never answered is asked where its log ends. Without
// that, an idle partition, after a failover for instance, would neither catch
// the follower up nor learn what is on a quorum until somebody published.
func TestLeader_IdleFollowerThatNeverAnsweredIsProbed(t *testing.T) {
	wal := newLeaderTestWAL(t)
	events := batchAt(0, 6)
	for _, e := range events {
		e.ScheduleTs = 1
	}
	if err := wal.AppendBatch(events); err != nil {
		t.Fatal(err)
	}

	follower, addr := startGatedReplServer(t, true)
	l := NewLeader(0, 500, 20*time.Millisecond, wal, 2, "leader", nil)
	defer l.Stop()
	if err := l.AddFollower("idle", addr); err != nil {
		t.Fatal(err)
	}
	if got := l.QuorumOffset(); got != -1 {
		t.Fatalf("QuorumOffset before the follower answered = %d, want -1", got)
	}

	// Nothing is published. The maintenance loop alone has to find out.
	l.Start()
	waitFor(t, "the idle follower to be caught up", func() bool {
		offsets, _ := follower.snapshot()
		return len(offsets) == 6
	})
	waitFor(t, "the quorum offset to reach the end of the log", func() bool { return l.QuorumOffset() == 5 })
	if offsets, rejected := follower.snapshot(); rejected != 0 || offsets[0] != 0 || offsets[5] != 5 {
		t.Fatalf("follower holds %v after %d rejections, want 0-5 with none", offsets, rejected)
	}
}

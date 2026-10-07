package partition

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/internal/delivery"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
	"google.golang.org/grpc"
)

// termFollower is a follower that takes every append, until it is told to
// answer for a newer term.
type termFollower struct {
	types.UnimplementedReplicationServiceServer
	mu      sync.Mutex
	nextOff int64
	term    int64
	stalled bool
}

func (f *termFollower) Append(ctx context.Context, req *types.ReplicationAppendRequest) (*types.ReplicationAppendResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.stalled {
		return &types.ReplicationAppendResponse{Error: "not now", NextOffset: f.nextOff, LastOffset: f.nextOff - 1, Term: req.GetTerm()}, nil
	}
	if f.term > req.GetTerm() {
		return &types.ReplicationAppendResponse{Error: "invalid or stale term", NextOffset: f.nextOff, LastOffset: f.nextOff - 1, Term: f.term}, nil
	}
	if events := req.GetEvents(); len(events) > 0 {
		f.nextOff = events[len(events)-1].GetOffset() + 1
	}
	return &types.ReplicationAppendResponse{Success: true, LastOffset: f.nextOff - 1, NextOffset: f.nextOff, Term: req.GetTerm()}, nil
}

func (f *termFollower) SyncConsumerProgress(context.Context, *types.ReplicationProgressRequest) (*types.ReplicationProgressResponse, error) {
	return &types.ReplicationProgressResponse{Success: true}, nil
}

func (f *termFollower) set(change func(*termFollower)) {
	f.mu.Lock()
	change(f)
	f.mu.Unlock()
}

func startTermFollower(t *testing.T) (*termFollower, string) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	follower := &termFollower{}
	srv := grpc.NewServer()
	types.RegisterReplicationServiceServer(srv, follower)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return follower, lis.Addr().String()
}

// replicatedTestConfig is for a partition that needs one follower's
// acknowledgement besides its leader's.
func replicatedTestConfig(t *testing.T) *types.Config {
	cfg := unacceptedTestConfig(t)
	cfg.ClusterEnabled = true
	cfg.ReplicationFactor = 2
	cfg.MinInSyncReplicas = 2
	cfg.ReplicationTimeout = 2 * time.Second
	return cfg
}

func wantDeliverable(t *testing.T, p *Partition, want int64, when string) {
	t.Helper()
	if got := p.DeliverableThrough(); got != want {
		t.Fatalf("%s: deliverable through offset %d, want %d", when, got, want)
	}
}

func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// collectingStream records the offsets a subscriber is sent.
type collectingStream struct {
	mu      sync.Mutex
	offsets []int64
}

func (s *collectingStream) Send(msg *delivery.DeliveryMessage) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if msg.Event != nil {
		s.offsets = append(s.offsets, msg.Event.Offset)
	}
	for _, event := range msg.Batch {
		s.offsets = append(s.offsets, event.Offset)
	}
	return nil
}

func (s *collectingStream) Recv() (*delivery.Control, error) { return nil, nil }
func (s *collectingStream) Context() context.Context         { return context.Background() }

func (s *collectingStream) got() []int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]int64(nil), s.offsets...)
}

// A leader hands its consumers only what the partition's followers hold as
// well. What only the leader holds can still leave the log, and a consumer
// that has taken an event cannot give it back.
func TestDeliverableThrough_FollowsTheReplicatedLog(t *testing.T) {
	pm := NewPartitionManager("node-1", replicatedTestConfig(t))
	defer pm.StopAllPartitions()
	follower, addr := startTermFollower(t)

	if err := pm.CreatePartition(0, "orders"); err != nil {
		t.Fatal(err)
	}
	p, err := pm.GetInternalPartition(0)
	if err != nil {
		t.Fatal(err)
	}
	appendOrders(t, p, 3) // in the log from before this node led
	wantDeliverable(t, p, -1, "before this node leads")

	if err := pm.PromoteToLeader(0, 5); err != nil {
		t.Fatal(err)
	}
	wantDeliverable(t, p, -1, "leading, with nothing known of any follower")

	if err := pm.AddFollower(0, "node-2", addr); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, "the follower to be brought up to date", func() bool { return p.DeliverableThrough() == 2 })

	// A publish that the follower does not confirm stays with the leader.
	follower.set(func(f *termFollower) { f.stalled = true })
	unconfirmed := appendOrders(t, p, 2)
	if err := p.ReplLeader.Replicate(unconfirmed); err == nil {
		t.Fatal("replicated to a follower that refuses")
	}
	p.HoldUnaccepted(unconfirmed, false)
	wantDeliverable(t, p, 2, "with two entries the follower has not confirmed")

	// Consumers get the first three and not those two, on whatever path.
	stream := &collectingStream{}
	sub := &delivery.Subscription{ID: "g:0:c1", ConsumerGroup: "g", Topic: "orders", Partition: &types.Partition{ID: 0}, MaxCredits: 100, Stream: stream}
	if err := p.Dispatcher.Subscribe(sub); err != nil {
		t.Fatal(err)
	}
	defer p.Dispatcher.Unsubscribe(sub.ID) // nothing is acknowledged here; stopping would wait for it
	all, err := p.Wal.ReadEvents(0, 4)
	if err != nil {
		t.Fatal(err)
	}
	if dispatched, blocked := p.Dispatcher.RedriveGroup("g", all); dispatched != 3 || !blocked {
		t.Fatalf("read from the log: %d events handed over, held back=%v; want 3 and true", dispatched, blocked)
	}
	if err := p.Dispatcher.DispatchBatch(all); err != nil {
		t.Fatal(err)
	}
	if got := stream.got(); len(got) != 3 {
		t.Fatalf("the consumer was sent offsets %v, want 0 to 2", got)
	}

	// Once the follower has them they are delivered like the others.
	follower.set(func(f *termFollower) { f.stalled = false })
	waitUntil(t, "the follower to confirm the rest", func() bool { return p.DeliverableThrough() == 4 })
	if dispatched, _ := p.Dispatcher.RedriveGroup("g", all); dispatched != 2 {
		t.Fatalf("%d events handed over once the follower holds them, want 2", dispatched)
	}

	if err := pm.DemoteFromLeader(0); err != nil {
		t.Fatal(err)
	}
	wantDeliverable(t, p, -1, "after this node stopped leading")
}

// A follower that answers for a newer term has a newer leader. The node that
// hears it stops leading the partition by itself: it records the term, so
// that it cannot be made leader again under its old one, and takes no part in
// delivering until it leads anew.
func TestLeaderStepsDownWhenAFollowerHasANewerLeader(t *testing.T) {
	pm := NewPartitionManager("node-1", replicatedTestConfig(t))
	defer pm.StopAllPartitions()
	follower, addr := startTermFollower(t)

	if err := pm.CreatePartition(0, "orders"); err != nil {
		t.Fatal(err)
	}
	p, err := pm.GetInternalPartition(0)
	if err != nil {
		t.Fatal(err)
	}
	if err := pm.PromoteToLeader(0, 5); err != nil {
		t.Fatal(err)
	}
	if err := pm.AddFollower(0, "node-2", addr); err != nil {
		t.Fatal(err)
	}
	first := appendOrders(t, p, 1)
	if err := p.ReplLeader.Replicate(first); err != nil {
		t.Fatal(err)
	}

	follower.set(func(f *termFollower) { f.term = 7 })
	stale := appendOrders(t, p, 1)
	if err := p.ReplLeader.Replicate(stale); err == nil {
		t.Fatal("replicated to a follower of a newer leader")
	}
	waitUntil(t, "this node to stop leading", func() bool { return !p.IsLeader() })
	waitUntil(t, "the replication leader to be gone", func() bool {
		pm.mu.RLock()
		defer pm.mu.RUnlock()
		return p.ReplLeader == nil
	})
	if got := p.Epoch(); got != 7 {
		t.Fatalf("recorded term %d, want 7", got)
	}
	wantDeliverable(t, p, -1, "after stepping down")
	if err := pm.PromoteToLeader(0, 5); err == nil {
		t.Fatal("promoted again under the term a newer leader has replaced")
	}
	if err := pm.PromoteToLeader(0, 8); err != nil {
		t.Fatalf("promotion under a newer term: %v", err)
	}
}

// When the leader's log replaces the end of this replica's, what consumer
// groups had recorded for the entries that go must go with them: the offsets
// are taken by other events.
func TestCutLogForgetsWhatWasFinishedThere(t *testing.T) {
	pm := NewPartitionManager("node-1", replicatedTestConfig(t))
	defer pm.StopAllPartitions()
	if err := pm.CreatePartition(0, "orders"); err != nil {
		t.Fatal(err)
	}
	p, err := pm.GetInternalPartition(0)
	if err != nil {
		t.Fatal(err)
	}
	events := appendOrders(t, p, 6)
	if err := p.ConsumerGroup.CreateGroup("g", "orders", []int32{0}); err != nil {
		t.Fatal(err)
	}
	if err := p.ConsumerGroup.CommitDelivery("g", 0, events); err != nil {
		t.Fatal(err)
	}

	removed, err := p.CutLog(4)
	if err != nil || removed != 2 {
		t.Fatalf("CutLog(4) removed %d entries, %v; want 2, nil", removed, err)
	}
	for offset, want := range map[int64]bool{3: true, 4: false, 5: false} {
		if got := p.ConsumerGroup.IsCompleted("g", 0, offset); got != want {
			t.Errorf("after the cut IsCompleted(%d) = %v, want %v", offset, got, want)
		}
	}
	if committed, _ := p.ConsumerGroup.GetCommittedOffset("g", 0); committed != 4 {
		t.Fatalf("committed offset after the cut = %d, want 4", committed)
	}
	if removed, err := p.CutLog(4); err != nil || removed != 0 {
		t.Fatalf("second CutLog(4) removed %d entries, %v; want 0, nil", removed, err)
	}
}

// A replica can hold consumer progress that reaches past its log: taken from
// a leader whose last entries it never got, or recorded before a crash that
// lost the end of the log. When it leads, the offsets past its log go to new
// events, and those are not finished.
func TestPromotionForgetsProgressBeyondTheLog(t *testing.T) {
	pm := NewPartitionManager("node-1", replicatedTestConfig(t))
	defer pm.StopAllPartitions()
	if err := pm.CreatePartition(0, "orders"); err != nil {
		t.Fatal(err)
	}
	p, err := pm.GetInternalPartition(0)
	if err != nil {
		t.Fatal(err)
	}
	appendOrders(t, p, 3)
	if err := p.ConsumerGroup.CreateGroup("g", "orders", []int32{0}); err != nil {
		t.Fatal(err)
	}
	// As if recorded for a log that reached offset 5.
	if err := p.ConsumerGroup.CommitDelivery("g", 0, []*types.Event{
		{Topic: "orders", Offset: 0}, {Topic: "orders", Offset: 1}, {Topic: "orders", Offset: 2},
		{Topic: "orders", Offset: 3}, {Topic: "orders", Offset: 5},
	}); err != nil {
		t.Fatal(err)
	}

	if err := pm.PromoteToLeader(0, 5); err != nil {
		t.Fatal(err)
	}
	for offset, want := range map[int64]bool{2: true, 3: false, 5: false} {
		if got := p.ConsumerGroup.IsCompleted("g", 0, offset); got != want {
			t.Errorf("after promotion IsCompleted(%d) = %v, want %v", offset, got, want)
		}
	}
}

// What is scheduled belongs to the log it was read from. A node that stops
// leading drops it, and schedules the log anew when it leads again: the log
// may have been cut and written again in between, and a timer kept from
// before would fire with the event that was removed, at the offset of the
// event that replaced it.
func TestScheduleIsRebuiltFromTheLogOnEveryPromotion(t *testing.T) {
	cfg := replicatedTestConfig(t)
	cfg.ReplicationFactor, cfg.MinInSyncReplicas = 1, 1 // leads alone
	pm := NewPartitionManager("node-1", cfg)
	defer pm.StopAllPartitions()
	if err := pm.CreatePartition(0, "orders"); err != nil {
		t.Fatal(err)
	}
	if err := pm.StartPartition(0); err != nil {
		t.Fatal(err)
	}
	p, err := pm.GetInternalPartition(0)
	if err != nil {
		t.Fatal(err)
	}
	if err := pm.PromoteToLeader(0, 1); err != nil {
		t.Fatal(err)
	}

	// Two events an hour ahead, scheduled as a publish does.
	removed := appendOrders(t, p, 2)
	if err := p.Scheduler.ScheduleBatch(removed); err != nil {
		t.Fatal(err)
	}
	if got := p.Scheduler.GetTimingWheelDepth(); got != 2 {
		t.Fatalf("%d timers, want 2", got)
	}

	if err := pm.DemoteFromLeader(0); err != nil {
		t.Fatal(err)
	}
	if got := p.Scheduler.GetTimingWheelDepth(); got != 0 {
		t.Fatalf("%d timers kept by a node that stopped leading, want 0", got)
	}

	// The new leader's log has one other event where those two were.
	if _, err := p.CutLog(0); err != nil {
		t.Fatal(err)
	}
	replacement := appendOrders(t, p, 1)

	if err := pm.PromoteToLeader(0, 2); err != nil {
		t.Fatal(err)
	}
	if got := p.Scheduler.GetTimingWheelDepth(); got != 1 {
		t.Fatalf("%d timers after leading again, want 1: the log holds one event", got)
	}
	// And promoting over what is already scheduled does not add to it.
	if err := pm.DemoteFromLeader(0); err != nil {
		t.Fatal(err)
	}
	if err := pm.PromoteToLeader(0, 3); err != nil {
		t.Fatal(err)
	}
	if got := p.Scheduler.GetTimingWheelDepth(); got != 1 {
		t.Fatalf("%d timers after leading a third time, want 1", got)
	}
	_ = replacement
}

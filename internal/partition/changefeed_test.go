package partition

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

// feedSink collects what change feeds hand over, per partition.
type feedSink struct {
	refuse atomic.Bool
	calls  atomic.Int64

	mu      sync.Mutex
	offsets map[int32][]int64
}

func (s *feedSink) take(_ context.Context, partitionID int32, events []*types.Event) error {
	s.calls.Add(1)
	if s.refuse.Load() {
		return errors.New("sink is down")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.offsets == nil {
		s.offsets = make(map[int32][]int64)
	}
	for _, event := range events {
		if event.PartitionId != partitionID {
			return fmt.Errorf("event of partition %d handed over for partition %d", event.PartitionId, partitionID)
		}
		s.offsets[partitionID] = append(s.offsets[partitionID], event.Offset)
	}
	return nil
}

func (s *feedSink) got(partitionID int32) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return fmt.Sprint(s.offsets[partitionID])
}

// expect waits until the sink holds exactly want for the partition, then makes
// sure nothing more arrives.
func (s *feedSink) expect(t *testing.T, partitionID int32, want string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for s.got(partitionID) != want {
		if time.Now().After(deadline) {
			t.Fatalf("change feed of partition %d handed over %s, want %s", partitionID, s.got(partitionID), want)
		}
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(150 * time.Millisecond)
	if got := s.got(partitionID); got != want {
		t.Fatalf("change feed of partition %d went on to hand over %s, want %s", partitionID, got, want)
	}
}

func feedManager(t *testing.T, cfg *types.Config, sink *feedSink) *PartitionManager {
	t.Helper()
	pm := NewPartitionManager("node-1", cfg)
	if sink != nil {
		pm.SetChangeFeed(sink.take)
	}
	t.Cleanup(func() { pm.Close() })
	return pm
}

func startedPartition(t *testing.T, pm *PartitionManager, id int32) *Partition {
	t.Helper()
	if err := pm.CreatePartition(id, "orders"); err != nil {
		t.Fatal(err)
	}
	if err := pm.StartPartition(id); err != nil {
		t.Fatal(err)
	}
	p, err := pm.GetInternalPartition(id)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

var feedEventSeq atomic.Int64

// appendOrders appends n events to the log, as the middle of a publish does.
func appendOrders(t *testing.T, p *Partition, n int) []*types.Event {
	t.Helper()
	events := make([]*types.Event, n)
	for i := range events {
		events[i] = &types.Event{
			MessageId:  fmt.Sprintf("order-%d", feedEventSeq.Add(1)),
			Topic:      "orders",
			Payload:    []byte("p"),
			ScheduleTs: time.Now().Add(time.Hour).UnixMilli(),
		}
	}
	if err := p.Wal.AppendBatch(events); err != nil {
		t.Fatal(err)
	}
	return events
}

// publish runs a whole accepted publish of n events.
func publish(t *testing.T, p *Partition, n int) {
	t.Helper()
	token := p.BeginPublish()
	appendOrders(t, p, n)
	p.EndPublish(token)
}

func wantWatermark(t *testing.T, p *Partition, want int64, when string) {
	t.Helper()
	if got := p.AcceptedThrough(); got != want {
		t.Fatalf("%s: accepted through offset %d, want %d", when, got, want)
	}
}

// The feed hands over an event only once its publish is accepted, and never
// out of log order: an undecided publish holds back every later one, however
// long ago those were accepted.
func TestChangeFeed_OnlyAcceptedEventsInLogOrder(t *testing.T) {
	sink := &feedSink{}
	p := startedPartition(t, feedManager(t, unacceptedTestConfig(t), sink), 0)

	publish(t, p, 2)
	wantWatermark(t, p, 1, "after an accepted publish")
	sink.expect(t, 0, "[0 1]")

	// A publish appends offsets 2 and 3 and is still waiting for its replicas.
	slow := p.BeginPublish()
	undecided := appendOrders(t, p, 2)
	wantWatermark(t, p, 1, "while a publish is in flight")

	// A later publish is accepted meanwhile. It is behind the undecided one.
	publish(t, p, 2)
	wantWatermark(t, p, 1, "while an earlier publish is in flight")
	sink.expect(t, 0, "[0 1]")

	// The slow publish fails: its events stay in the log, held.
	p.HoldUnaccepted(undecided, false)
	p.EndPublish(slow)
	wantWatermark(t, p, 1, "while a failed publish is held")
	sink.expect(t, 0, "[0 1]")

	// Its events turn out to be replicated after all.
	if err := p.AcceptThrough(3); err != nil {
		t.Fatal(err)
	}
	wantWatermark(t, p, 5, "after the held publish was accepted")
	sink.expect(t, 0, "[0 1 2 3 4 5]")
}

// A consumer that fails delays nothing but the feed: publishes carry on, and
// when it recovers it gets everything, in order, once.
func TestChangeFeed_FailingConsumerLosesNothing(t *testing.T) {
	sink := &feedSink{}
	p := startedPartition(t, feedManager(t, unacceptedTestConfig(t), sink), 0)

	sink.refuse.Store(true)
	for i := 0; i < 5; i++ {
		publish(t, p, 3)
	}
	wantWatermark(t, p, 14, "with the consumer down")
	deadline := time.Now().Add(10 * time.Second)
	for sink.calls.Load() < 2 {
		if time.Now().After(deadline) {
			t.Fatal("the feed did not retry a consumer that failed")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if position, _ := p.ChangeFeedPosition(); position != -1 {
		t.Fatalf("the feed moved to offset %d although the consumer took nothing", position)
	}

	sink.refuse.Store(false)
	sink.expect(t, 0, "[0 1 2 3 4 5 6 7 8 9 10 11 12 13 14]")
}

// Under concurrent publishes every event is handed over exactly once and in
// log order.
func TestChangeFeed_ConcurrentPublishes(t *testing.T) {
	sink := &feedSink{}
	p := startedPartition(t, feedManager(t, unacceptedTestConfig(t), sink), 0)

	const publishers, rounds, perPublish = 8, 40, 3
	var wg sync.WaitGroup
	for w := 0; w < publishers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for r := 0; r < rounds; r++ {
				publish(t, p, perPublish)
			}
		}()
	}
	wg.Wait()

	total := publishers * rounds * perPublish
	want := make([]int64, total)
	for i := range want {
		want[i] = int64(i)
	}
	sink.expect(t, 0, fmt.Sprint(want))
}

// A partition created after the feed was set up has one too. In a cluster
// that is every partition but the first.
func TestChangeFeed_CoversPartitionsCreatedLater(t *testing.T) {
	sink := &feedSink{}
	cfg := unacceptedTestConfig(t)
	cfg.PartitionCount = 8
	pm := feedManager(t, cfg, sink)

	p, err := pm.GetOrCreateInternalPartition(5, "orders")
	if err != nil {
		t.Fatal(err)
	}
	publish(t, p, 3)
	sink.expect(t, 5, "[0 1 2]")
}

// The feed's position survives a restart: nothing is skipped, and what was
// handed over before the shutdown is not handed over again. Switching a feed
// on for a partition that already has events does not replay them.
func TestChangeFeed_ResumesAfterRestart(t *testing.T) {
	cfg := unacceptedTestConfig(t)

	// First run: no feed. These events predate it.
	pm := NewPartitionManager("node-1", cfg)
	publish(t, startedPartition(t, pm, 0), 3)
	if err := pm.Close(); err != nil {
		t.Fatal(err)
	}

	// Second run: the feed is switched on.
	first := &feedSink{}
	pm = NewPartitionManager("node-1", cfg)
	pm.SetChangeFeed(first.take)
	p := startedPartition(t, pm, 0)
	publish(t, p, 4)
	first.expect(t, 0, "[3 4 5 6]")
	// The consumer goes down just before the shutdown, with events pending.
	first.refuse.Store(true)
	publish(t, p, 2)
	if err := pm.Close(); err != nil {
		t.Fatal(err)
	}

	// Third run: it continues with what the second did not get to.
	second := &feedSink{}
	pm = NewPartitionManager("node-1", cfg)
	pm.SetChangeFeed(second.take)
	t.Cleanup(func() { pm.Close() })
	p = startedPartition(t, pm, 0)
	second.expect(t, 0, "[7 8]")
	publish(t, p, 1)
	second.expect(t, 0, "[7 8 9]")
}

// In a cluster only the partition's leader exports. A replica keeps the
// position its leader reports and continues from it when it takes over.
func TestChangeFeed_LeaderOnlyAndTakeover(t *testing.T) {
	sink := &feedSink{}
	cfg := unacceptedTestConfig(t)
	cfg.ClusterEnabled = true
	cfg.ReplicationFactor = 1
	pm := feedManager(t, cfg, sink)
	p := startedPartition(t, pm, 0)

	// As a replica: entries arrive from the leader, which is exporting them.
	appendOrders(t, p, 6)
	sink.expect(t, 0, "[]")
	p.NoteLeaderFeedPosition(3, true)
	// An older report, from a message that arrived late, changes nothing.
	p.NoteLeaderFeedPosition(1, true)
	if position, known := p.ChangeFeedPosition(); !known || position != 3 {
		t.Fatalf("replica keeps feed position %d (known=%v), want 3", position, known)
	}

	if err := pm.PromoteToLeader(0, 1); err != nil {
		t.Fatal(err)
	}
	sink.expect(t, 0, "[4 5]")

	// A publish fails past the append and is held. Then this node is demoted,
	// which forgets what was held: the new leader decides about that tail. It
	// must not be exported on the way out.
	failed := p.BeginPublish()
	p.HoldUnaccepted(appendOrders(t, p, 2), false)
	p.EndPublish(failed)
	sink.expect(t, 0, "[4 5]")
	if err := pm.DemoteFromLeader(0); err != nil {
		t.Fatal(err)
	}
	appendOrders(t, p, 2)
	sink.expect(t, 0, "[4 5]")

	// Leading again, at a new epoch, everything in the log counts: it is what
	// this leader's consumers will be given.
	if err := pm.PromoteToLeader(0, 2); err != nil {
		t.Fatal(err)
	}
	sink.expect(t, 0, "[4 5 6 7 8 9]")
}

// A replica whose leader exports nothing starts at the end of its log when
// it takes over, as if the feed had just been switched on.
func TestChangeFeed_TakeoverFromLeaderWithoutFeed(t *testing.T) {
	sink := &feedSink{}
	cfg := unacceptedTestConfig(t)
	cfg.ClusterEnabled = true
	cfg.ReplicationFactor = 1
	pm := feedManager(t, cfg, sink)
	p := startedPartition(t, pm, 0)

	appendOrders(t, p, 5)
	p.NoteLeaderFeedPosition(0, false)
	if err := pm.PromoteToLeader(0, 1); err != nil {
		t.Fatal(err)
	}
	publish(t, p, 2)
	sink.expect(t, 0, "[5 6]")
}

// Without a feed, tracking publishes costs nothing and reports nothing.
func TestChangeFeed_Disabled(t *testing.T) {
	p := startedPartition(t, feedManager(t, unacceptedTestConfig(t), nil), 0)
	if token := p.BeginPublish(); token != 0 {
		t.Fatalf("a partition without a feed tracked a publish (token %d)", token)
	} else {
		p.EndPublish(token)
	}
	if _, known := p.ChangeFeedPosition(); known {
		t.Fatal("a partition without a feed reports a feed position")
	}
	p.NoteLeaderFeedPosition(10, true) // must not panic
}

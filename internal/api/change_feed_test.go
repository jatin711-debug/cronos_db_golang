package api

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/internal/partition"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

// exported collects the message IDs a node's change feed handed over.
type exported struct {
	refuse atomic.Bool

	mu  sync.Mutex
	ids []string
}

func (e *exported) take(_ context.Context, _ int32, events []*types.Event) error {
	if e.refuse.Load() {
		return errors.New("sink is down")
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, event := range events {
		e.ids = append(e.ids, event.MessageId)
	}
	return nil
}

func (e *exported) got() string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return fmt.Sprint(e.ids)
}

// expect waits until exactly want has been exported, then makes sure nothing
// more follows.
func (e *exported) expect(t *testing.T, want []string) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for e.got() != fmt.Sprint(want) {
		if time.Now().After(deadline) {
			t.Fatalf("change feed exported %s, want %v", e.got(), want)
		}
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(200 * time.Millisecond)
	if got := e.got(); got != fmt.Sprint(want) {
		t.Fatalf("change feed went on to export %s, want %v", got, want)
	}
}

// newFeedReplica is a replica of a clustered partition whose node exports a
// change feed into sink.
func newFeedReplica(t *testing.T, nodeID string, sink *exported) *replicaLog {
	t.Helper()
	cfg := &types.Config{DataDir: t.TempDir(), ClusterEnabled: true, PartitionCount: 1, ReplicationFactor: 3, MinInSyncReplicas: 2, TickMS: 10, WheelSize: 100, SegmentSizeBytes: 1 << 20, IndexInterval: 1, FsyncMode: "periodic", FlushIntervalMS: 50, DedupTTLHours: 24, BloomCapacity: 1000}
	pm := partition.NewPartitionManager(nodeID, cfg)
	pm.SetChangeFeed(sink.take)
	t.Cleanup(func() { pm.Close() })
	if err := pm.CreatePartition(0, "orders"); err != nil {
		t.Fatal(err)
	}
	p, err := pm.GetInternalPartition(0)
	if err != nil {
		t.Fatal(err)
	}
	return &replicaLog{t: t, pm: pm, h: NewReplicationServiceHandler(pm), p: p}
}

func ids(tag string, from, n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("%s-%d", tag, from+i)
	}
	return out
}

// Through the public publish path: an event reaches the change feed when its
// publish is accepted, which for a replicated partition means acknowledged by
// the required replicas. A publish that was refused is not exported; its
// retry is, once. Followers hold the same log and export nothing.
func TestChangeFeed_ExportsPublishesWhenTheyAreAccepted(t *testing.T) {
	leaderSink, followerSink := &exported{}, &exported{}
	leader := newFeedReplica(t, "node-a", leaderSink)
	follower := newFeedReplica(t, "node-b", followerSink)
	if err := leader.pm.PromoteToLeader(0, 1); err != nil {
		t.Fatal(err)
	}
	handler := NewEventServiceHandler(leader.pm, leader.p.DedupStore, leader.p.ConsumerGroup)
	publish := func(events []*types.Event) *types.PublishBatchResponse {
		t.Helper()
		resp, err := handler.PublishBatch(context.Background(), &types.PublishBatchRequest{Events: events})
		if err != nil {
			t.Fatal(err)
		}
		return resp
	}

	// No follower is reachable: the publish reaches the leader's log and is
	// refused.
	batch := orders("order", 4)
	if resp := publish(batch); resp.GetSuccess() {
		t.Fatalf("publish without a quorum succeeded: %+v", resp)
	}
	if got := len(leader.log()); got != 4 {
		t.Fatalf("leader log has %d entries, want the 4 of the refused publish", got)
	}
	leaderSink.expect(t, nil)

	// The follower joins and the producer retries.
	if err := leader.pm.AddFollower(0, "node-b", follower.serveReplication()); err != nil {
		t.Fatal(err)
	}
	if resp := publish(batch); !resp.GetSuccess() {
		t.Fatalf("retry: %+v", resp)
	}
	leaderSink.expect(t, ids("order", 0, 4))

	// A duplicate of an accepted publish exports nothing more.
	publish(batch)
	if resp := publish(orders("next", 3)); !resp.GetSuccess() {
		t.Fatalf("publish: %+v", resp)
	}
	leaderSink.expect(t, append(ids("order", 0, 4), ids("next", 0, 3)...))

	if got := len(follower.log()); got != 7 {
		t.Fatalf("follower log has %d entries, want 7", got)
	}
	followerSink.expect(t, nil)
}

// A failover continues the feed. The old leader's consumer was down for its
// last publishes, so they were accepted but never exported; the replica that
// takes over knows how far the feed had got and exports exactly the rest.
func TestChangeFeed_ContinuesOnTheNewLeader(t *testing.T) {
	sinkA, sinkB := &exported{}, &exported{}
	a := newFeedReplica(t, "node-a", sinkA)
	b := newFeedReplica(t, "node-b", sinkB)
	if err := a.pm.PromoteToLeader(0, 1); err != nil {
		t.Fatal(err)
	}
	if err := a.pm.AddFollower(0, "node-b", b.serveReplication()); err != nil {
		t.Fatal(err)
	}
	handler := NewEventServiceHandler(a.pm, a.p.DedupStore, a.p.ConsumerGroup)
	publish := func(events []*types.Event) {
		t.Helper()
		resp, err := handler.PublishBatch(context.Background(), &types.PublishBatchRequest{Events: events})
		if err != nil || !resp.GetSuccess() {
			t.Fatalf("publish: %+v (err=%v)", resp, err)
		}
	}

	publish(orders("early", 5))
	sinkA.expect(t, ids("early", 0, 5))
	// The follower learns how far the feed has got.
	deadline := time.Now().Add(15 * time.Second)
	for {
		if position, known := b.p.ChangeFeedPosition(); known && position == 4 {
			break
		}
		if time.Now().After(deadline) {
			position, known := b.p.ChangeFeedPosition()
			t.Fatalf("follower was told feed position %d (known=%v), want 4", position, known)
		}
		time.Sleep(10 * time.Millisecond)
	}

	// The leader's sink goes down; three more publishes are accepted.
	sinkA.refuse.Store(true)
	publish(orders("late", 3))
	if got := len(b.log()); got != 8 {
		t.Fatalf("follower log has %d entries, want 8", got)
	}

	// node-b takes over. Its only other replica is the old leader, which it
	// has never heard from: on this idle partition nothing would tell it what
	// is on a quorum unless it asked.
	if err := b.pm.PromoteToLeader(0, 2); err != nil {
		t.Fatal(err)
	}
	if err := b.pm.AddFollower(0, "node-a", a.serveReplication()); err != nil {
		t.Fatal(err)
	}
	sinkB.expect(t, ids("late", 0, 3))
	if a.p.IsLeader() {
		t.Fatal("the old leader kept leading after the new one contacted it")
	}
	sinkA.expect(t, ids("early", 0, 5))
}

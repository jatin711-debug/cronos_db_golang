package api

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/internal/dedup"
	"github.com/jatin711-debug/cronos_db_golang/internal/partition"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// gatedReplication is a follower's replication service that can be made to
// fail appends, as a follower cut off from the leader would.
type gatedReplication struct {
	types.ReplicationServiceServer
	refusing atomic.Bool
}

func (g *gatedReplication) refuse(on bool) { g.refusing.Store(on) }

func (g *gatedReplication) Append(ctx context.Context, req *types.ReplicationAppendRequest) (*types.ReplicationAppendResponse, error) {
	if g.refusing.Load() {
		return nil, status.Error(codes.Unavailable, "follower unreachable")
	}
	return g.ReplicationServiceServer.Append(ctx, req)
}

func serveReplicationService(t *testing.T, service types.ReplicationServiceServer) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	types.RegisterReplicationServiceServer(srv, service)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return lis.Addr().String()
}

// orders builds n publishable events, due a minute from now, whose message IDs
// start with tag.
func orders(tag string, n int) []*types.Event {
	due := time.Now().Add(time.Minute).UnixMilli()
	out := make([]*types.Event, n)
	for i := range out {
		out[i] = &types.Event{MessageId: fmt.Sprintf("%s-%d", tag, i), Topic: "orders", Payload: []byte("payload"), ScheduleTs: due}
	}
	return out
}

// publisher is a partition leader that requires one follower to acknowledge
// each publish, together with the public handler in front of it.
type publisher struct {
	*replicaLog
	h *EventServiceHandler
}

func newPublisher(t *testing.T) *publisher {
	t.Helper()
	leader := newReplicaLog(t, "node-a")
	if err := leader.pm.PromoteToLeader(0, 1); err != nil {
		t.Fatal(err)
	}
	return &publisher{replicaLog: leader, h: NewEventServiceHandler(leader.pm, leader.p.DedupStore, leader.p.ConsumerGroup)}
}

func (p *publisher) publish(events []*types.Event) *types.PublishBatchResponse {
	p.t.Helper()
	resp, err := p.h.PublishBatch(context.Background(), &types.PublishBatchRequest{Events: events})
	if err != nil {
		p.t.Fatalf("publish batch: %v", err)
	}
	return resp
}

func (p *publisher) scheduled() int64 { return p.p.Scheduler.GetTimingWheelDepth() }

func (p *publisher) outcome(messageID string) dedup.Outcome {
	p.t.Helper()
	outcome, _, found, err := p.p.DedupStore.Outcome(messageID)
	if err != nil || !found {
		p.t.Fatalf("no record for %s (found=%v, err=%v)", messageID, found, err)
	}
	return outcome
}

// A batch that reaches the leader's log but not the required replicas is
// refused. Its retry must complete that publish: replicate and schedule the
// events already in the log, not append them a second time.
func TestPublishRetry_FinishesTheEarlierPublish(t *testing.T) {
	leader := newPublisher(t)
	follower := newReplicaLog(t, "node-b")
	batch := orders("order", 5)

	first := leader.publish(batch)
	if first.GetSuccess() || first.GetErrorCount() != 5 {
		t.Fatalf("publish without a follower: %+v", first)
	}
	if got := len(leader.log()); got != 5 {
		t.Fatalf("leader log has %d entries after the failed publish, want 5", got)
	}
	if got := leader.scheduled(); got != 0 {
		t.Fatalf("%d timers scheduled for a publish that was refused", got)
	}
	if got := leader.outcome("order-0"); got != dedup.Appended {
		t.Fatalf("refused publish recorded as outcome %v, want appended", got)
	}

	// The follower becomes reachable and the client retries the same batch.
	if err := leader.pm.AddFollower(0, "node-b", follower.serveReplication()); err != nil {
		t.Fatal(err)
	}
	retry := leader.publish(batch)
	if !retry.GetSuccess() || retry.GetErrorCount() != 0 || retry.GetDuplicateCount() != 5 || retry.GetPublishedCount() != 0 {
		t.Fatalf("retry: %+v", retry)
	}
	if got := len(leader.log()); got != 5 {
		t.Fatalf("leader log has %d entries after the retry: the batch was appended again", got)
	}
	if got := len(follower.log()); got != 5 {
		t.Fatalf("follower log has %d entries after the retry, want 5", got)
	}
	if got := leader.scheduled(); got != 5 {
		t.Fatalf("%d timers scheduled after the retry, want 5", got)
	}
	if got := leader.outcome("order-4"); got != dedup.Accepted {
		t.Fatalf("finished publish recorded as outcome %v, want accepted", got)
	}

	// From here on it is an ordinary duplicate.
	again := leader.publish(batch)
	if !again.GetSuccess() || again.GetDuplicateCount() != 5 {
		t.Fatalf("second retry: %+v", again)
	}
	if log, timers := len(leader.log()), leader.scheduled(); log != 5 || timers != 5 {
		t.Fatalf("second retry changed state: %d log entries, %d timers", log, timers)
	}
}

// While the replicas stay unreachable the retry keeps failing, and it still
// must not append or schedule anything.
func TestPublishRetry_KeepsFailingWithoutQuorum(t *testing.T) {
	leader := newPublisher(t)
	batch := orders("order", 3)
	leader.publish(batch)

	for attempt := 1; attempt <= 3; attempt++ {
		retry := leader.publish(batch)
		if retry.GetSuccess() || retry.GetErrorCount() != 3 {
			t.Fatalf("retry %d without a follower: %+v", attempt, retry)
		}
		if !strings.Contains(retry.GetError(), "replication") {
			t.Fatalf("retry %d error does not say why: %q", attempt, retry.GetError())
		}
	}
	if log, timers := len(leader.log()), leader.scheduled(); log != 3 || timers != 0 {
		t.Fatalf("after failed retries: %d log entries, %d timers; want 3 and 0", log, timers)
	}
}

// The single-event RPC follows the same rule and reports the offset the
// earlier attempt was given.
func TestPublishRetry_SingleEvent(t *testing.T) {
	leader := newPublisher(t)
	follower := newReplicaLog(t, "node-b")
	req := &types.PublishRequest{Event: orders("single", 1)[0]}

	first, err := leader.h.Publish(context.Background(), req)
	if err != nil || first.GetSuccess() {
		t.Fatalf("publish without a follower: %+v %v", first, err)
	}
	if err := leader.pm.AddFollower(0, "node-b", follower.serveReplication()); err != nil {
		t.Fatal(err)
	}
	retry, err := leader.h.Publish(context.Background(), req)
	if err != nil || !retry.GetSuccess() || retry.GetOffset() != 0 {
		t.Fatalf("retry: %+v %v", retry, err)
	}
	if log, replica, timers := len(leader.log()), len(follower.log()), leader.scheduled(); log != 1 || replica != 1 || timers != 1 {
		t.Fatalf("after the retry: %d log entries, %d on the follower, %d timers; want 1 each", log, replica, timers)
	}
	again, err := leader.h.Publish(context.Background(), req)
	if err != nil || again.GetSuccess() || again.GetError() != "duplicate message_id" {
		t.Fatalf("publish of an accepted ID: %+v %v", again, err)
	}
}

// A publish that is never retried is not stranded. The next publish that
// reaches the replicas proves the earlier events did too, because a follower
// holds a prefix of the log, and they are scheduled and accepted with it.
func TestPublishRetry_LaterPublishAcceptsEarlierOnes(t *testing.T) {
	leader := newPublisher(t)
	follower := newReplicaLog(t, "node-b")
	refused := orders("refused", 4)
	if first := leader.publish(refused); first.GetSuccess() {
		t.Fatalf("publish without a follower: %+v", first)
	}

	if err := leader.pm.AddFollower(0, "node-b", follower.serveReplication()); err != nil {
		t.Fatal(err)
	}
	if next := leader.publish(orders("next", 2)); !next.GetSuccess() || next.GetPublishedCount() != 2 {
		t.Fatalf("publish with a follower: %+v", next)
	}
	if log, replica, timers := len(leader.log()), len(follower.log()), leader.scheduled(); log != 6 || replica != 6 || timers != 6 {
		t.Fatalf("after the later publish: %d log entries, %d on the follower, %d timers; want 6 each", log, replica, timers)
	}
	if leader.p.HasUnaccepted() {
		t.Fatal("the refused batch is still held after it was replicated")
	}
	if retry := leader.publish(refused); !retry.GetSuccess() || retry.GetDuplicateCount() != 4 {
		t.Fatalf("retry of the batch accepted in the meantime: %+v", retry)
	}
	if log, timers := len(leader.log()), leader.scheduled(); log != 6 || timers != 6 {
		t.Fatalf("retry changed state: %d log entries, %d timers", log, timers)
	}
}

// Catch-up alone finishes a refused publish: once the leader's maintenance
// loop has shipped the events to the follower, they are scheduled without any
// further request from a client.
func TestPublishRetry_CatchUpAcceptsWithoutARetry(t *testing.T) {
	leader := newPublisher(t)
	follower := newReplicaLog(t, "node-b")
	gate := &gatedReplication{ReplicationServiceServer: follower.h}
	if err := leader.pm.AddFollower(0, "node-b", serveReplicationService(t, gate)); err != nil {
		t.Fatal(err)
	}
	if ok := leader.publish(orders("ok", 2)); !ok.GetSuccess() {
		t.Fatalf("publish with a follower: %+v", ok)
	}

	gate.refuse(true)
	if refused := leader.publish(orders("refused", 3)); refused.GetSuccess() {
		t.Fatalf("publish while the follower refuses appends: %+v", refused)
	}
	if got := leader.scheduled(); got != 2 {
		t.Fatalf("%d timers scheduled, want only the 2 accepted ones", got)
	}

	gate.refuse(false)
	deadline := time.Now().Add(15 * time.Second)
	for leader.scheduled() != 5 || leader.p.HasUnaccepted() {
		if time.Now().After(deadline) {
			t.Fatalf("refused publish never accepted: %d timers, follower log %d", leader.scheduled(), len(follower.log()))
		}
		time.Sleep(25 * time.Millisecond)
	}
	if got := leader.outcome("refused-2"); got != dedup.Accepted {
		t.Fatalf("outcome %v after catch-up, want accepted", got)
	}
	if got := len(leader.log()); got != 5 {
		t.Fatalf("leader log has %d entries, want 5", got)
	}
}

// After a restart the node knows only that an event is in its log, not that
// its publish was accepted. A retry is answered from the log: it succeeds as a
// duplicate and appends nothing.
func TestPublishRetry_AfterRestartIsAnsweredFromTheLog(t *testing.T) {
	cfg := &types.Config{DataDir: t.TempDir(), PartitionCount: 1, TickMS: 10, WheelSize: 100, SegmentSizeBytes: 1 << 20, IndexInterval: 1, FsyncMode: "periodic", FlushIntervalMS: 50, DedupTTLHours: 24, BloomCapacity: 1000}
	batch := orders("order", 4)

	// The events reach the log, and the process stops before their dedup
	// records are written.
	pm := partition.NewPartitionManager("node-a", cfg)
	if err := pm.CreatePartition(0, "orders"); err != nil {
		t.Fatal(err)
	}
	p, _ := pm.GetInternalPartition(0)
	if err := p.Wal.AppendBatch(batch); err != nil {
		t.Fatal(err)
	}
	if err := pm.Close(); err != nil {
		t.Fatal(err)
	}

	pm = partition.NewPartitionManager("node-a", cfg)
	t.Cleanup(func() { pm.Close() })
	if err := pm.CreatePartition(0, "orders"); err != nil {
		t.Fatal(err)
	}
	if err := pm.StartPartition(0); err != nil {
		t.Fatal(err)
	}
	p, _ = pm.GetInternalPartition(0)
	h := NewEventServiceHandler(pm, p.DedupStore, p.ConsumerGroup)
	timers := p.Scheduler.GetTimingWheelDepth()

	retry, err := h.PublishBatch(context.Background(), &types.PublishBatchRequest{Events: orders("order", 4)})
	if err != nil || !retry.GetSuccess() || retry.GetDuplicateCount() != 4 || retry.GetPublishedCount() != 0 {
		t.Fatalf("retry after restart: %+v %v", retry, err)
	}
	if got := p.Wal.GetNextOffset(); got != 4 {
		t.Fatalf("log has %d entries after the retry, want 4", got)
	}
	if got := p.Scheduler.GetTimingWheelDepth(); got != timers {
		t.Fatalf("retry changed the timer count from %d to %d: restart had already scheduled the log", timers, got)
	}
}

// When the earlier event is no longer in the log, as after a newer leader
// replaced this node's unreplicated tail, that publish can never complete. Its
// record must not keep refusing the ID: the retry is published as new.
func TestPublishRetry_EventGoneFromTheLogIsPublishedAgain(t *testing.T) {
	leader := newPublisher(t)
	follower := newReplicaLog(t, "node-b")
	batch := orders("order", 3)
	if first := leader.publish(batch); first.GetSuccess() {
		t.Fatalf("publish without a follower: %+v", first)
	}

	// The node is demoted and its unreplicated tail is removed.
	if err := leader.pm.DemoteFromLeader(0); err != nil {
		t.Fatal(err)
	}
	if _, err := leader.p.Wal.TruncateToOffset(0); err != nil {
		t.Fatal(err)
	}
	if err := leader.pm.PromoteToLeader(0, 3); err != nil {
		t.Fatal(err)
	}
	if err := leader.pm.AddFollower(0, "node-b", follower.serveReplication()); err != nil {
		t.Fatal(err)
	}

	retry := leader.publish(batch)
	if !retry.GetSuccess() || retry.GetPublishedCount() != 3 || retry.GetDuplicateCount() != 0 {
		t.Fatalf("retry after the tail was removed: %+v", retry)
	}
	if log, replica := len(leader.log()), len(follower.log()); log != 3 || replica != 3 {
		t.Fatalf("after the retry: %d log entries, %d on the follower; want 3 each", log, replica)
	}
}

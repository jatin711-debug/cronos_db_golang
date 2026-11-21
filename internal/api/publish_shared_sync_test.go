package api

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/internal/partition"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
	"google.golang.org/grpc"
)

// syncingCluster is the leader of one partition with two followers, each with
// a log that syncs what it appends, as a production node's does, and the
// public handler in front of the leader.
type syncingCluster struct {
	leader    *partition.Partition
	followers []*partition.Partition
	h         *EventServiceHandler

	// What the followers were sent: requests that carried events, the events,
	// and the time the followers took over them.
	appends, appended, appendNanos atomic.Int64
}

// countedReplication counts the appends a follower is sent.
type countedReplication struct {
	types.ReplicationServiceServer
	c *syncingCluster
}

func (r *countedReplication) Append(ctx context.Context, req *types.ReplicationAppendRequest) (*types.ReplicationAppendResponse, error) {
	start := time.Now()
	resp, err := r.ReplicationServiceServer.Append(ctx, req)
	if n := len(req.GetEvents()); n > 0 {
		r.c.appends.Add(1)
		r.c.appended.Add(int64(n))
		r.c.appendNanos.Add(int64(time.Since(start)))
	}
	return resp, err
}

func newSyncingCluster(tb testing.TB, fsyncMode string) *syncingCluster {
	tb.Helper()
	node := func(nodeID string) (*partition.PartitionManager, *partition.Partition) {
		cfg := &types.Config{DataDir: tb.TempDir(), PartitionCount: 1, ReplicationFactor: 3, MinInSyncReplicas: 2, TickMS: 10, WheelSize: 100, SegmentSizeBytes: 64 << 20, IndexInterval: 1000, FsyncMode: fsyncMode, FlushIntervalMS: 50, DedupTTLHours: 24, BloomCapacity: 100000}
		pm := partition.NewPartitionManager(nodeID, cfg)
		tb.Cleanup(func() { pm.Close() })
		if err := pm.CreatePartition(0, "orders"); err != nil {
			tb.Fatal(err)
		}
		p, err := pm.GetInternalPartition(0)
		if err != nil {
			tb.Fatal(err)
		}
		return pm, p
	}
	pm, leader := node("node-a")
	if err := pm.PromoteToLeader(0, 1); err != nil {
		tb.Fatal(err)
	}
	c := &syncingCluster{leader: leader, h: NewEventServiceHandler(pm, leader.DedupStore, leader.ConsumerGroup)}
	for _, id := range []string{"node-b", "node-c"} {
		followerPM, follower := node(id)
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			tb.Fatal(err)
		}
		srv := grpc.NewServer()
		types.RegisterReplicationServiceServer(srv, &countedReplication{ReplicationServiceServer: NewReplicationServiceHandler(followerPM), c: c})
		go func() { _ = srv.Serve(lis) }()
		tb.Cleanup(srv.Stop)
		if err := pm.AddFollower(0, id, lis.Addr().String()); err != nil {
			tb.Fatal(err)
		}
		c.followers = append(c.followers, follower)
	}
	return c
}

func logOf(tb testing.TB, p *partition.Partition) []*types.Event {
	tb.Helper()
	last := p.Wal.GetLastOffset()
	if last < 0 {
		return nil
	}
	events, err := p.Wal.ReadEvents(0, last)
	if err != nil {
		tb.Fatal(err)
	}
	return events
}

// Publishes to one partition are appended in order under its publish lock and
// wait for the leader's disk and for the followers outside it. Many at once,
// single and batched, in both syncing modes: each is acknowledged, the log
// holds each once at the offset it was acknowledged with, the followers hold
// the same log, and everything acknowledged is durable on the leader and
// deliverable.
func TestPublishesToOnePartitionWaitForDiskAndFollowersTogether(t *testing.T) {
	for _, mode := range []string{"every_event", "batch"} {
		t.Run(mode, func(t *testing.T) {
			c := newSyncingCluster(t, mode)
			due := time.Now().Add(time.Hour).UnixMilli()

			const publishers, each = 12, 15
			var wg sync.WaitGroup
			errs := make(chan error, publishers)
			var mu sync.Mutex
			acknowledged := map[int64]string{}
			note := func(offset int64, id string) error {
				mu.Lock()
				defer mu.Unlock()
				if other, taken := acknowledged[offset]; taken {
					return fmt.Errorf("offset %d acknowledged for %s and for %s", offset, other, id)
				}
				acknowledged[offset] = id
				return nil
			}
			for publisher := 0; publisher < publishers; publisher++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := 0; i < each; i++ {
						if publisher%3 == 0 {
							// A batch of three.
							events := make([]*types.Event, 3)
							for n := range events {
								events[n] = &types.Event{MessageId: fmt.Sprintf("p%d-%d-%d", publisher, i, n), Topic: "orders", Payload: []byte("payload"), ScheduleTs: due}
							}
							resp, err := c.h.PublishBatch(context.Background(), &types.PublishBatchRequest{Events: events})
							if err != nil || !resp.GetSuccess() || resp.GetPublishedCount() != 3 {
								errs <- fmt.Errorf("publish batch: %+v %v", resp, err)
								return
							}
							for n, event := range events {
								if err := note(resp.GetFirstOffset()+int64(n), event.MessageId); err != nil {
									errs <- err
									return
								}
							}
							continue
						}
						id := fmt.Sprintf("p%d-%d", publisher, i)
						resp, err := c.h.Publish(context.Background(), &types.PublishRequest{Event: &types.Event{MessageId: id, Topic: "orders", Payload: []byte("payload"), ScheduleTs: due}})
						if err != nil || !resp.GetSuccess() {
							errs <- fmt.Errorf("publish %s: %+v %v", id, resp, err)
							return
						}
						if err := note(resp.GetOffset(), id); err != nil {
							errs <- err
							return
						}
					}
				}()
			}
			wg.Wait()
			close(errs)
			for err := range errs {
				t.Fatal(err)
			}

			const want = publishers/3*each*3 + (publishers-publishers/3)*each
			if len(acknowledged) != want {
				t.Fatalf("%d events acknowledged, want %d", len(acknowledged), want)
			}
			log := logOf(t, c.leader)
			found := 0
			for _, event := range log {
				id, ok := acknowledged[event.Offset]
				if !ok {
					continue // not a publish: a record the partition wrote itself
				}
				found++
				if event.MessageId != id {
					t.Fatalf("offset %d holds %s, and %s was acknowledged there", event.Offset, event.MessageId, id)
				}
			}
			if found != want {
				t.Fatalf("the log holds %d of the %d acknowledged events", found, want)
			}
			last := c.leader.Wal.GetLastOffset()
			if got := c.leader.Wal.DurableThrough(); got != last {
				t.Fatalf("the leader's log is durable through %d and ends at %d", got, last)
			}
			if got := c.leader.DeliverableThrough(); got != last {
				t.Fatalf("deliverable through %d, want the log end %d", got, last)
			}
			for _, follower := range c.followers {
				deadline := time.Now().Add(10 * time.Second)
				for follower.Wal.GetLastOffset() < last {
					if time.Now().After(deadline) {
						t.Fatalf("a follower's log ends at %d, the leader's at %d", follower.Wal.GetLastOffset(), last)
					}
					time.Sleep(10 * time.Millisecond)
				}
				held := logOf(t, follower)
				if len(held) != len(log) {
					t.Fatalf("a follower holds %d entries, the leader %d", len(held), len(log))
				}
				for i, event := range held {
					if event.Offset != log[i].Offset || event.MessageId != log[i].MessageId || event.Term != log[i].Term {
						t.Fatalf("a follower's entry %d is %s at offset %d, the leader's %s at %d", i, event.MessageId, event.Offset, log[i].MessageId, log[i].Offset)
					}
				}
			}
		})
	}
}

// Single-event publishes to one partition of three replicas on this machine's
// disk: every publish is synced on the leader and on a follower before it is
// acknowledged.
func BenchmarkReplicatedPublish(b *testing.B) {
	for _, mode := range []string{"every_event", "batch"} {
		for _, publishers := range []int{1, 4, 16} {
			b.Run(fmt.Sprintf("fsync=%s/publishers=%d", mode, publishers), func(b *testing.B) {
				c := newSyncingCluster(b, mode)
				due := time.Now().Add(time.Hour).UnixMilli()
				payload := make([]byte, 1024)
				// One publish first: the followers connect and answer.
				if resp, err := c.h.Publish(context.Background(), &types.PublishRequest{Event: &types.Event{MessageId: "warm", Topic: "orders", Payload: payload, ScheduleTs: due}}); err != nil || !resp.GetSuccess() {
					b.Fatalf("first publish: %+v %v", resp, err)
				}
				var wg sync.WaitGroup
				errs := make(chan error, publishers)
				appends, appended, nanos := c.appends.Load(), c.appended.Load(), c.appendNanos.Load()
				b.ResetTimer()
				for publisher := 0; publisher < publishers; publisher++ {
					wg.Add(1)
					go func() {
						defer wg.Done()
						for i := publisher; i < b.N; i += publishers {
							resp, err := c.h.Publish(context.Background(), &types.PublishRequest{Event: &types.Event{MessageId: fmt.Sprintf("m-%d", i), Topic: "orders", Payload: payload, ScheduleTs: due}})
							if err != nil || !resp.GetSuccess() {
								errs <- fmt.Errorf("publish %d: %+v %v", i, resp, err)
								return
							}
						}
					}()
				}
				wg.Wait()
				b.StopTimer()
				close(errs)
				for err := range errs {
					b.Fatal(err)
				}
				b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "publishes/s")
				if sent := c.appends.Load() - appends; sent > 0 {
					b.ReportMetric(float64(c.appended.Load()-appended)/float64(sent), "events/append")
					b.ReportMetric(float64(c.appendNanos.Load()-nanos)/float64(sent)/1e6, "ms/append")
				}
			})
		}
	}
}

// Batches of 500 events to one partition of three replicas, each batch synced
// on the leader and on a follower before it is acknowledged.
func BenchmarkReplicatedPublishBatch(b *testing.B) {
	const batchSize = 500
	for _, publishers := range []int{1, 4} {
		b.Run(fmt.Sprintf("fsync=every_event/publishers=%d", publishers), func(b *testing.B) {
			c := newSyncingCluster(b, "every_event")
			due := time.Now().Add(time.Hour).UnixMilli()
			payload := make([]byte, 1024)
			if resp, err := c.h.Publish(context.Background(), &types.PublishRequest{Event: &types.Event{MessageId: "warm", Topic: "orders", Payload: payload, ScheduleTs: due}}); err != nil || !resp.GetSuccess() {
				b.Fatalf("first publish: %+v %v", resp, err)
			}
			var wg sync.WaitGroup
			errs := make(chan error, publishers)
			b.ResetTimer()
			for publisher := 0; publisher < publishers; publisher++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := publisher; i < b.N; i += publishers {
						events := make([]*types.Event, batchSize)
						for n := range events {
							events[n] = &types.Event{MessageId: fmt.Sprintf("m-%d-%d", i, n), Topic: "orders", Payload: payload, ScheduleTs: due}
						}
						resp, err := c.h.PublishBatch(context.Background(), &types.PublishBatchRequest{Events: events})
						if err != nil || !resp.GetSuccess() || resp.GetPublishedCount() != batchSize {
							errs <- fmt.Errorf("publish batch %d: %+v %v", i, resp, err)
							return
						}
					}
				}()
			}
			wg.Wait()
			b.StopTimer()
			close(errs)
			for err := range errs {
				b.Fatal(err)
			}
			b.ReportMetric(float64(b.N*batchSize)/b.Elapsed().Seconds(), "events/s")
		})
	}
}

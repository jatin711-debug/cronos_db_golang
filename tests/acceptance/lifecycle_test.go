//go:build acceptance

package acceptance

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/pkg/client"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Real processes activate through committed capabilities, mutate scheduled
// events, fail over, and restart. A successful cancellation stays canceled;
// the rescheduled event is delivered under its new time without any controls.
func TestLifecycleSurvivesLeaderLossAndWholeClusterRestart(t *testing.T) {
	c := newCluster(t, "--event-lifecycle", "--wal-relocation-bytes=4096")
	c.startAll()
	observer := c.dial()
	producer, err := observer.NewProducer(client.DefaultProducerConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	partition := int32(0)
	due := time.Now().Add(40 * time.Second)
	refs := make(map[string]*types.EventReference)
	for _, id := range []string{"canceled", "moved"} {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		got, err := producer.Send(ctx, client.Message{MessageID: id, Topic: "lifecycle", Payload: []byte(id), ScheduleAt: due, PartitionKey: "lifecycle-test"})
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		partition = got.PartitionID
		refs[id] = &types.EventReference{PartitionId: got.PartitionID, Offset: got.Offset, Term: got.Term, MessageId: id, Topic: "lifecycle"}
	}
	mutate := func(id string) {
		t.Helper()
		until := time.Now().Add(25 * time.Second)
		for {
			n := c.leaderOf(observer, refs[id].PartitionId)
			conn, err := grpc.NewClient(n.grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				t.Fatal(err)
			}
			api := types.NewEventServiceClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			state, err := api.GetEventState(ctx, &types.EventStateRequest{Reference: refs[id]})
			if err == nil {
				req := &types.EventMutationRequest{Reference: refs[id], RequestId: "change-" + id, ExpectedRevisionOffset: state.RevisionOffset, ExpectedRevisionTerm: state.RevisionTerm}
				if id == "canceled" {
					_, err = api.CancelEvent(ctx, req)
				} else {
					req.ScheduleTs = time.Now().Add(5 * time.Second).UnixMilli()
					_, err = api.RescheduleEvent(ctx, req)
				}
			}
			cancel()
			conn.Close()
			if err == nil {
				return
			}
			if time.Now().After(until) {
				t.Fatalf("lifecycle activation/mutation: %v", err)
			}
			time.Sleep(200 * time.Millisecond)
		}
	}
	mutate("canceled")
	mutate("moved")
	leader := c.leaderOf(observer, partition)
	c.kill(leader)
	// The old leader stays down until its successor has committed the log.
	if next := c.leaderOf(observer, partition); next.id == leader.id {
		t.Fatal("no failover")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	seen := make(chan string, 100)
	errors := make(chan error, 1)
	cfg := client.DefaultConsumerConfig("lifecycle", "workers")
	cfg.ReconnectBackoff = 200 * time.Millisecond
	consumer := c.dial()
	go func() {
		errors <- consumer.Subscribe(ctx, cfg, func(_ context.Context, d client.Delivery) error {
			events := d.Batch
			if d.Event != nil {
				events = append(events, d.Event)
			}
			for _, e := range events {
				for key := range e.Meta {
					if strings.HasPrefix(key, "cronos.internal.") {
						seen <- "internal-control"
					}
				}
				seen <- e.MessageId
			}
			return nil
		})
	}()
	select {
	case id := <-seen:
		if id != "moved" {
			t.Fatalf("unexpected delivery %q", id)
		}
	case err := <-errors:
		t.Fatalf("consumer stopped: %v", err)
	case <-time.After(60 * time.Second):
		t.Fatal("rescheduled event was lost at failover")
	}
	c.start(leader)
	c.waitReady(c.nodes...)
	for _, n := range c.nodes {
		c.kill(n)
	}
	for _, n := range c.nodes {
		c.start(n)
	}
	c.waitReady(c.nodes...)
	for until := due.Add(3 * time.Second); time.Now().Before(until); {
		select {
		case id := <-seen:
			if id != "moved" {
				t.Fatalf("cancellation/control resurrected after restart: %q", id)
			}
		case err := <-errors:
			t.Fatalf("consumer stopped across restart: %v", err)
		case <-time.After(200 * time.Millisecond):
		}
	}
	cancel()
	select {
	case <-errors:
	case <-time.After(10 * time.Second):
		t.Fatal("consumer did not stop")
	}
	for id := int32(0); id < partitionCount; id++ {
		agreedLog(t, c, id, "lifecycle")
	}
	t.Log(fmt.Sprintf("cancellation and reschedule survived leader loss and restart past %s", due.Format(time.RFC3339)))
}

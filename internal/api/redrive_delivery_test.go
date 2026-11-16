package api

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/pkg/client"
)

// publishOrders sends total events to the single test partition in batches.
func publishOrders(t *testing.T, ctx context.Context, producer *client.Producer, prefix string, total int) {
	t.Helper()
	const batchSize = 1000
	for sent := 0; sent < total; sent += batchSize {
		messages := make([]client.Message, min(batchSize, total-sent))
		for i := range messages {
			messages[i] = client.Message{
				MessageID:    fmt.Sprintf("%s-%d", prefix, sent+i),
				Topic:        "orders",
				PartitionKey: "orders",
				Payload:      []byte("payload"),
				ScheduleTS:   time.Now().UnixMilli(),
			}
		}
		result, err := producer.SendBatch(ctx, messages)
		if err != nil {
			t.Fatal(err)
		}
		if int(result.PublishedCount) != len(messages) {
			t.Fatalf("published %d of %d events", result.PublishedCount, len(messages))
		}
	}
}

func dialOrders(t *testing.T, ctx context.Context, addr string) (*client.Client, *client.Producer) {
	t.Helper()
	cfg := client.DefaultConfig(addr)
	cfg.Security.Insecure = true
	c, err := client.Dial(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	producer, err := c.NewProducer(client.DefaultProducerConfig())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { producer.Close() })
	return c, producer
}

// countDeliveries consumes "orders" with the given credit window and returns a
// counter of distinct-delivery events received.
func countDeliveries(ctx context.Context, c *client.Client, group string, credits int32) *atomic.Int64 {
	var received atomic.Int64
	go func() {
		cfg := client.DefaultConsumerConfig("orders", group)
		cfg.PartitionID = 0
		cfg.StartOffset = 0
		cfg.MaxBufferSize = credits
		_ = c.Subscribe(ctx, cfg, func(_ context.Context, d client.Delivery) error {
			n := int64(len(d.Batch))
			if d.Event != nil {
				n++
			}
			received.Add(n)
			return nil
		})
	}()
	return &received
}

func waitForCount(t *testing.T, counter *atomic.Int64, want int64, within time.Duration, what string) {
	t.Helper()
	deadline := time.Now().Add(within)
	for counter.Load() < want {
		if time.Now().After(deadline) {
			t.Fatalf("%s: received %d of %d events within %v", what, counter.Load(), want, within)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// A consumer that connects to an existing backlog must drain it at the pace
// its credits allow. The backlog used to be re-read at a fixed 512 events per
// second, so this many events took about twenty seconds.
func TestBacklogIsDeliveredAtConsumerPace(t *testing.T) {
	const total = 10000
	_, addr := startPartitionedServer(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	c, producer := dialOrders(t, ctx, addr)

	publishOrders(t, ctx, producer, "backlog", total)
	received := countDeliveries(ctx, c, "backlog-group", 2000)
	waitForCount(t, received, total, 8*time.Second, "backlog")
}

// A group with a small credit window keeps up while events written long ago
// come due among events written a moment ago, and gets every one of them
// once. The consumer used to hold each ack for its flush interval unless 64
// had gathered, which a window of 4 never does: the group could take no more
// than 4 events per interval however fast the server was, and in the soak
// such a group fell behind for good once the server slowed down. The interval
// here is ten seconds, so that held acks would show whatever the server's
// speed: 300 events would take twelve minutes.
func TestSmallWindowGroupGetsNewEventsWhileOldOnesComeDue(t *testing.T) {
	const old, fresh = 100, 200
	_, addr := startPartitionedServer(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	c, producer := dialOrders(t, ctx, addr)

	// The old events come first in the log and fall due over a few seconds,
	// while the new ones are published.
	dueFrom := time.Now().Add(2 * time.Second).UnixMilli()
	messages := make([]client.Message, old)
	for i := range messages {
		messages[i] = client.Message{
			MessageID:    fmt.Sprintf("old-%d", i),
			Topic:        "orders",
			PartitionKey: "orders",
			Payload:      []byte("payload"),
			ScheduleTS:   dueFrom + int64(i)*30, // over three seconds
		}
	}
	if _, err := producer.SendBatch(ctx, messages); err != nil {
		t.Fatal(err)
	}

	var received atomic.Int64
	go func() {
		cfg := client.DefaultConsumerConfig("orders", "small-window")
		cfg.PartitionID = 0
		cfg.StartOffset = 0
		cfg.MaxBufferSize = 4
		cfg.AckFlushInterval = 10 * time.Second
		_ = c.Subscribe(ctx, cfg, func(_ context.Context, d client.Delivery) error {
			n := int64(len(d.Batch))
			if d.Event != nil {
				n++
			}
			received.Add(n)
			return nil
		})
	}()
	time.Sleep(300 * time.Millisecond) // let the subscription register
	for sent := 0; sent < fresh; sent += 20 {
		publishOrders(t, ctx, producer, fmt.Sprintf("fresh-%d", sent), 20)
		time.Sleep(100 * time.Millisecond)
	}
	waitForCount(t, &received, old+fresh, 30*time.Second, "old and new events")

	time.Sleep(1500 * time.Millisecond)
	if got := received.Load(); got != old+fresh {
		t.Fatalf("received %d events for %d published; completed events were redelivered", got, old+fresh)
	}
}

// Events held back while the consumer was out of credits must follow as soon
// as it acks, not wait for a slow rescan of the log.
func TestHeldBackEventsFollowAcks(t *testing.T) {
	const total = 10000
	_, addr := startPartitionedServer(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	c, producer := dialOrders(t, ctx, addr)

	// A small credit window guarantees most of the burst is held back.
	received := countDeliveries(ctx, c, "live-group", 1000)
	time.Sleep(300 * time.Millisecond) // let the subscription register
	publishOrders(t, ctx, producer, "live", total)
	waitForCount(t, received, total, 10*time.Second, "live burst")

	// Nothing is delivered twice once everything has been acked.
	time.Sleep(1500 * time.Millisecond)
	if got := received.Load(); got != total {
		t.Fatalf("received %d events for %d published; completed events were redelivered", got, total)
	}
}

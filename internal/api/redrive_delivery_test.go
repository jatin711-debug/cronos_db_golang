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

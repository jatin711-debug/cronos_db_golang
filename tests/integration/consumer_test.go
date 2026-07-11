package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/pkg/client"
)

func TestConsumerGroupOffsetCommit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	topic := topicName(t)
	groupID := topicName(t) + "-group"
	payload := []byte("offset-test")

	// Start consumer before publishing so it is registered when the event fires.
	deliveries := make(chan client.Delivery, 10)
	consCfg := client.DefaultConsumerConfig(topic, groupID)
	consCfg.AckMode = client.AckModeManual

	go func() {
		_ = testClient.Subscribe(ctx, consCfg, func(ctx context.Context, d client.Delivery) error {
			deliveries <- d
			// Ack success
			return d.AckSuccess(ctx)
		})
	}()

	// Give the consumer a moment to register before publishing.
	time.Sleep(200 * time.Millisecond)

	// Publish one event
	res, err := publishEvent(ctx, topic, payload, 2*time.Second)
	if err != nil {
		t.Fatalf("publish failed: %v", err)
	}
	t.Logf("Published at offset %d", res.Offset)

	select {
	case d := <-deliveries:
		t.Logf("received payload=%q expected=%q", string(d.Event.GetPayload()), string(payload))
		if string(d.Event.GetPayload()) != string(payload) {
			t.Fatalf("payload mismatch: got %q, want %q", string(d.Event.GetPayload()), payload)
		}
		t.Logf("Received and acked: offset=%d", d.Event.GetOffset())
	case <-time.After(15 * time.Second):
		t.Fatal("timeout waiting for delivery")
	}
}

func TestConsumerGroupRebalance(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	topic := topicName(t)
	groupID := topicName(t) + "-group"
	count := 50

	// Start two consumers in the same group before publishing so events are not
	// lost before a subscriber is registered.
	delivered := make(map[string]int)
	deliveries := make(chan string, count*2)

	startConsumer := func(name string) {
		consCfg := client.DefaultConsumerConfig(topic, groupID)
		consCfg.AckMode = client.AckModeAuto
		consCfg.SubscriptionID = name
		_ = testClient.Subscribe(ctx, consCfg, func(ctx context.Context, d client.Delivery) error {
			for _, ev := range d.Batch {
				deliveries <- name
				_ = ev
			}
			if d.Event != nil {
				deliveries <- name
			}
			return nil
		})
	}

	go startConsumer("consumer-1")
	go startConsumer("consumer-2")

	// Give consumers time to register before publishing.
	time.Sleep(1 * time.Second)

	// Publish events to a single partition so the group can round-robin.
	producer, err := client.NewProducer(testClient, client.DefaultProducerConfig())
	if err != nil {
		t.Fatalf("create producer: %v", err)
	}
	defer producer.Close()

	messages := make([]client.Message, count)
	for i := 0; i < count; i++ {
		messages[i] = client.Message{
			Topic:        topic,
			PartitionKey: topic,
			MessageID:    fmt.Sprintf("rebalance-%d", i),
			Payload:      []byte("rebalance-test"),
			ScheduleTS:   time.Now().Add(3 * time.Second).UnixMilli(),
		}
	}
	_, err = producer.SendBatch(ctx, messages)
	if err != nil {
		t.Fatalf("batch publish failed: %v", err)
	}

	// Collect deliveries
	timeout := time.After(30 * time.Second)
loop:
	for {
		select {
		case name := <-deliveries:
			delivered[name]++
			if len(delivered) >= count {
				break loop
			}
		case <-timeout:
			break loop
		}
	}

	total := 0
	for _, c := range delivered {
		total += c
	}
	if total < count {
		t.Fatalf("expected at least %d deliveries, got %d", count, total)
	}

	// Both consumers should have received something (rebalancing worked)
	if len(delivered) < 2 {
		t.Fatalf("expected both consumers to receive messages, only %d did", len(delivered))
	}

	t.Logf("Rebalance test: %d consumers active, total delivered=%d", len(delivered), total)
}

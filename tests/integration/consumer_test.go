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

	// Publish one event
	res, err := publishEvent(ctx, topic, payload, 2*time.Second)
	if err != nil {
		t.Fatalf("publish failed: %v", err)
	}
	t.Logf("Published at offset %d", res.Offset)

	// Consume with manual ack
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

	select {
	case d := <-deliveries:
		if string(d.Event.GetPayload()) != string(payload) {
			t.Fatalf("payload mismatch")
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

	// Publish events
	producer, err := client.NewProducer(testClient, client.DefaultProducerConfig())
	if err != nil {
		t.Fatalf("create producer: %v", err)
	}
	defer producer.Close()

	messages := make([]client.Message, count)
	for i := 0; i < count; i++ {
		messages[i] = client.Message{
			Topic:        topic,
			PartitionKey: fmt.Sprintf("key-%d", i%4), // Spread across partitions
			Payload:      []byte("rebalance-test"),
			ScheduleTS:   time.Now().Add(3 * time.Second).UnixMilli(),
		}
	}
	_, err = producer.SendBatch(ctx, messages)
	if err != nil {
		t.Fatalf("batch publish failed: %v", err)
	}

	// Start two consumers in the same group
	delivered := make(map[string]int)
	deliveries := make(chan string, count*2)

	startConsumer := func(name string) {
		consCfg := client.DefaultConsumerConfig(topic, groupID)
		consCfg.AckMode = client.AckModeAuto
		consCfg.SubscriptionID = name
		_ = testClient.Subscribe(ctx, consCfg, func(ctx context.Context, d client.Delivery) error {
			deliveries <- name
			return nil
		})
	}

	go startConsumer("consumer-1")
	go startConsumer("consumer-2")

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

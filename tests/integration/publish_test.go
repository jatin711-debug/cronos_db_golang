package integration

import (
	"context"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/pkg/client"
)

func TestPublishAndReceive(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	topic := topicName(t)
	payload := []byte("hello-integration")

	// Start consumer first
	deliveries := make(chan client.Delivery, 10)
	consCfg := client.DefaultConsumerConfig(topic, topicName(t)+"-group")
	consCfg.AckMode = client.AckModeAuto

	go func() {
		_ = testClient.Subscribe(ctx, consCfg, func(ctx context.Context, d client.Delivery) error {
			deliveries <- d
			return nil
		})
	}()

	// Publish event scheduled 2s in the future
	res, err := publishEvent(ctx, topic, payload, 2*time.Second)
	if err != nil {
		t.Fatalf("publish failed: %v", err)
	}
	if res == nil || res.Offset < 0 {
		t.Fatalf("unexpected result: %+v", res)
	}
	t.Logf("Published event at offset %d", res.Offset)

	// Wait for delivery
	select {
	case d := <-deliveries:
		if string(d.Event.GetPayload()) != string(payload) {
			t.Fatalf("payload mismatch: got %q, want %q", d.Event.GetPayload(), payload)
		}
		if d.Event.GetTopic() != topic {
			t.Fatalf("topic mismatch: got %q, want %q", d.Event.GetTopic(), topic)
		}
		t.Logf("Received delivery: offset=%d", d.Event.GetOffset())
	case <-time.After(15 * time.Second):
		t.Fatal("timeout waiting for delivery")
	}
}

func TestBatchPublish(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	topic := topicName(t)
	count := 100

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
			Payload:      []byte("batch-msg"),
			ScheduleTS:   time.Now().Add(2 * time.Second).UnixMilli(),
		}
	}

	res, err := producer.SendBatch(ctx, messages)
	if err != nil {
		t.Fatalf("batch publish failed: %v", err)
	}
	if res.PublishedCount != int32(count) {
		t.Fatalf("expected %d published, got %d", count, res.PublishedCount)
	}
	t.Logf("Batch published %d events", res.PublishedCount)

	// Verify delivery count
	deliveries := make(chan client.Delivery, count+10)
	consCfg := client.DefaultConsumerConfig(topic, topicName(t)+"-group")
	consCfg.AckMode = client.AckModeAuto

	go func() {
		_ = testClient.Subscribe(ctx, consCfg, func(ctx context.Context, d client.Delivery) error {
			deliveries <- d
			return nil
		})
	}()

	received := 0
	timeout := time.After(20 * time.Second)
loop:
	for {
		select {
		case <-deliveries:
			received++
			if received >= count {
				break loop
			}
		case <-timeout:
			break loop
		}
	}

	if received != count {
		t.Fatalf("expected %d deliveries, got %d", count, received)
	}
	t.Logf("Received all %d batch events", received)
}

func TestDeduplication(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	topic := topicName(t)
	msgID := "dedup-test-msg-001"
	payload := []byte("dedup-payload")

	producer, err := client.NewProducer(testClient, client.DefaultProducerConfig())
	if err != nil {
		t.Fatalf("create producer: %v", err)
	}
	defer producer.Close()

	// Publish same message ID twice
	for i := 0; i < 2; i++ {
		_, err := producer.Send(ctx, client.Message{
			Topic:        topic,
			PartitionKey: topic,
			MessageID:    msgID,
			Payload:      payload,
			ScheduleTS:   time.Now().Add(2 * time.Second).UnixMilli(),
		})
		if i == 0 && err != nil {
			t.Fatalf("first publish failed: %v", err)
		}
		if i == 1 && err == nil {
			// Server may return success=false without error; check result
			t.Logf("Second publish did not error (server may return success=false in response)")
		}
	}

	// Verify only one delivery
	deliveries := make(chan client.Delivery, 10)
	consCfg := client.DefaultConsumerConfig(topic, topicName(t)+"-group")
	consCfg.AckMode = client.AckModeAuto

	go func() {
		_ = testClient.Subscribe(ctx, consCfg, func(ctx context.Context, d client.Delivery) error {
			deliveries <- d
			return nil
		})
	}()

	received := 0
	timeout := time.After(15 * time.Second)
loop:
	for {
		select {
		case <-deliveries:
			received++
			if received > 1 {
				t.Fatal("received duplicate delivery — dedup failed")
			}
		case <-timeout:
			break loop
		}
	}

	if received != 1 {
		t.Fatalf("expected exactly 1 delivery, got %d", received)
	}
	t.Logf("Deduplication verified: exactly 1 delivery for duplicate publish")
}

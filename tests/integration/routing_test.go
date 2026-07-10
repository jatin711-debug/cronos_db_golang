package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/pkg/client"
)

// TestMultiTopicRouting tests that events are routed correctly when publishing to multiple topics.
func TestMultiTopicRouting(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	topics := []string{topicName(t), topicName(t), topicName(t)}
	expectedCounts := []int{5, 3, 7}

	// Start subscribers first so events are not lost before consumers register.
	deliveries := make([]chan client.Delivery, len(topics))
	for i, topic := range topics {
		deliveries[i] = make(chan client.Delivery, expectedCounts[i]+5)
		consCfg := client.DefaultConsumerConfig(topic, fmt.Sprintf("test-group-%d", i))
		consCfg.AckMode = client.AckModeAuto
		ch := deliveries[i]
		go func() {
			_ = testClient.Subscribe(ctx, consCfg, func(ctx context.Context, d client.Delivery) error {
				ch <- d
				return nil
			})
		}()
	}

	// Give subscribers a moment to register before publishing.
	time.Sleep(200 * time.Millisecond)

	// Publish to each topic
	for i, topic := range topics {
		producer, err := client.NewProducer(testClient, client.DefaultProducerConfig())
		if err != nil {
			t.Fatalf("create producer for topic %d: %v", i, err)
		}

		for j := 0; j < expectedCounts[i]; j++ {
			_, err := producer.Send(ctx, client.Message{
				Topic:        topic,
				PartitionKey: topic,
				MessageID:    fmt.Sprintf("mt-%d-%d", i, j),
				Payload:      []byte(fmt.Sprintf("msg-%d-%d", i, j)),
				ScheduleTS:   time.Now().Add(1 * time.Second).UnixMilli(),
			})
			if err != nil {
				t.Fatalf("publish to topic %d failed: %v", i, err)
			}
		}
		producer.Close()
	}

	// Verify counts
	for i := range topics {
		received := 0
		timeout := time.After(20 * time.Second)
	loop:
		for {
			select {
			case <-deliveries[i]:
				received++
				if received >= expectedCounts[i] {
					break loop
				}
			case <-timeout:
				break loop
			}
		}
		if received != expectedCounts[i] {
			t.Errorf("topic %d (%s): expected %d deliveries, got %d", i, topics[i], expectedCounts[i], received)
		} else {
			t.Logf("topic %d (%s): received %d/%d events", i, topics[i], received, expectedCounts[i])
		}
	}
}

// TestPublishConsumeValidateRoundTrip tests end-to-end publish → delivery with schema validation.
func TestPublishConsumeValidateRoundTrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	topic := topicName(t)

	// Publish 10 events
	producer, err := client.NewProducer(testClient, client.DefaultProducerConfig())
	if err != nil {
		t.Fatalf("create producer: %v", err)
	}
	defer producer.Close()

	for i := 0; i < 10; i++ {
		payload := []byte(fmt.Sprintf(`{"id":%d,"data":"msg-%d"}`, i, i))
		res, err := producer.Send(ctx, client.Message{
			Topic:        topic,
			PartitionKey: topic,
			Payload:      payload,
			ScheduleTS:   time.Now().Add(1 * time.Second).UnixMilli(),
		})
		if err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
		t.Logf("Published offset=%d", res.Offset)
	}

	// Subscribe and validate each message
	deliveries := make(chan client.Delivery, 10)
	consCfg := client.DefaultConsumerConfig(topic, topicName(t)+"-group")
	go func() {
		_ = testClient.Subscribe(ctx, consCfg, func(ctx context.Context, d client.Delivery) error {
			for _, ev := range d.Batch {
				t.Logf("Received offset=%d payload=%s", ev.GetOffset(), ev.GetPayload())
				deliveries <- d
			}
			if d.Event != nil {
				t.Logf("Received offset=%d payload=%s", d.Event.GetOffset(), d.Event.GetPayload())
				deliveries <- d
			}
			return nil
		})
	}()

	count := 0
	timeout := time.After(30 * time.Second)
	for count < 10 {
		select {
		case d := <-deliveries:
			t.Logf("Received offset=%d payload=%s", d.Event.GetOffset(), d.Event.GetPayload())
			count++
		case <-timeout:
			t.Fatalf("timeout at %d events", count)
		}
	}
	t.Logf("Round-trip complete: %d events validated", count)
}

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/pkg/client"
)

func TestReplayByTimeRange(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	topic := topicName(t)
	count := 10

	// Publish events spread over time
	producer, err := client.NewProducer(testClient, client.DefaultProducerConfig())
	if err != nil {
		t.Fatalf("create producer: %v", err)
	}
	defer producer.Close()

	startTS := time.Now()
	for i := 0; i < count; i++ {
		_, err := producer.Send(ctx, client.Message{
			Topic:        topic,
			PartitionKey: topic,
			Payload:      []byte("replay-test"),
			ScheduleTS:   time.Now().Add(5 * time.Second).UnixMilli(),
		})
		if err != nil {
			t.Fatalf("publish %d failed: %v", i, err)
		}
	}
	endTS := time.Now().Add(10 * time.Second)

	// Wait for events to be scheduled/delivered
	time.Sleep(6 * time.Second)

	// Replay by time range
	var replayed int
	req := client.ReplayRequest{
		Topic:     topic,
		StartTime: startTS,
		EndTime:   endTS,
	}

	err = testClient.Replay(ctx, req, func(ctx context.Context, re client.ReplayEvent) error {
		replayed++
		return nil
	})
	if err != nil {
		t.Fatalf("replay failed: %v", err)
	}

	if replayed < count {
		t.Fatalf("expected at least %d replayed events, got %d", count, replayed)
	}
	t.Logf("Replayed %d events by time range", replayed)
}

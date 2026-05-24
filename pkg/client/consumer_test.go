package client

import (
	"context"
	"testing"
	"time"

	"cronos_db/pkg/types"
)

func TestParsePartitionID(t *testing.T) {
	if got := parsePartitionID("group-a:7:member-1-22"); got != 7 {
		t.Fatalf("expected 7, got %d", got)
	}
	if got := parsePartitionID("malformed"); got != -1 {
		t.Fatalf("expected -1 for malformed id, got %d", got)
	}
}

func TestReconnectBackoffBounds(t *testing.T) {
	consumer := &Consumer{
		cfg: ConsumerConfig{
			ReconnectBackoff:    100 * time.Millisecond,
			MaxReconnectBackoff: 400 * time.Millisecond,
			ReconnectJitter:     0,
		},
	}
	if got := consumer.reconnectBackoff(1); got != 100*time.Millisecond {
		t.Fatalf("expected first backoff 100ms, got %v", got)
	}
	if got := consumer.reconnectBackoff(10); got != 400*time.Millisecond {
		t.Fatalf("expected capped backoff 400ms, got %v", got)
	}
}

func TestAssignmentTrackerUpdateFromDelivery(t *testing.T) {
	var assigned []Assignment
	tracker := newAssignmentTracker(Assignment{
		Topic:       "orders",
		PartitionID: -1,
	}, func(_ context.Context, assignment Assignment) {
		assigned = append(assigned, assignment)
	})

	tracker.updateFromDelivery(context.Background(), &types.Delivery{
		Event: &types.Event{
			Topic:       "orders",
			PartitionId: 3,
		},
	})

	if len(assigned) != 1 {
		t.Fatalf("expected one assignment callback, got %d", len(assigned))
	}
	if assigned[0].PartitionID != 3 {
		t.Fatalf("expected assigned partition 3, got %d", assigned[0].PartitionID)
	}
}

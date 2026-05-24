package client

import (
	"context"
	"testing"
)

func TestMemoryCheckpointStoreMonotonic(t *testing.T) {
	store := NewMemoryCheckpointStore()

	offset, err := store.LoadOffset(context.Background(), "group-a", "topic-a", 2)
	if err != nil {
		t.Fatalf("load empty offset: %v", err)
	}
	if offset != -1 {
		t.Fatalf("expected empty offset -1, got %d", offset)
	}

	if err := store.SaveOffset(context.Background(), "group-a", "topic-a", 2, 10); err != nil {
		t.Fatalf("save offset: %v", err)
	}
	if err := store.SaveOffset(context.Background(), "group-a", "topic-a", 2, 8); err != nil {
		t.Fatalf("save lower offset: %v", err)
	}

	offset, err = store.LoadOffset(context.Background(), "group-a", "topic-a", 2)
	if err != nil {
		t.Fatalf("load saved offset: %v", err)
	}
	if offset != 10 {
		t.Fatalf("expected monotonic offset 10, got %d", offset)
	}
}

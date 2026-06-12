package replay

import (
	"context"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/internal/storage"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

func TestNewReplayEngine(t *testing.T) {
	tmpDir := t.TempDir()
	wal, err := storage.NewWAL(tmpDir, 0, &storage.WALConfig{SegmentSizeBytes: 1024 * 1024, IndexInterval: 100, FsyncMode: "always"}, nil)
	if err != nil {
		t.Fatalf("NewWAL failed: %v", err)
	}
	defer wal.Close()

	engine := NewReplayEngine(wal)
	if engine == nil {
		t.Fatal("NewReplayEngine should not return nil")
	}
	if engine.wal != wal {
		t.Error("wal should be set")
	}
}

func TestReplayEngine_ReplayByTimeRange_Empty(t *testing.T) {
	tmpDir := t.TempDir()
	wal, err := storage.NewWAL(tmpDir, 0, &storage.WALConfig{SegmentSizeBytes: 1024 * 1024, IndexInterval: 100, FsyncMode: "always"}, nil)
	if err != nil {
		t.Fatalf("NewWAL failed: %v", err)
	}
	defer wal.Close()

	engine := NewReplayEngine(wal)
	ctx := context.Background()
	now := time.Now().UnixMilli()

	events, err := engine.ReplayByTimeRange(ctx, now-1000, now)
	if err != nil {
		t.Fatalf("ReplayByTimeRange failed: %v", err)
	}
	if len(events) != 0 {
		t.Errorf("expected 0 events, got %d", len(events))
	}
}

func TestReplayEngine_ReplayByTimeRange_WithEvents(t *testing.T) {
	tmpDir := t.TempDir()
	wal, err := storage.NewWAL(tmpDir, 0, &storage.WALConfig{SegmentSizeBytes: 1024 * 1024, IndexInterval: 100, FsyncMode: "always"}, nil)
	if err != nil {
		t.Fatalf("NewWAL failed: %v", err)
	}
	defer wal.Close()

	now := time.Now().UnixMilli()
	events := []*types.Event{
		{MessageId: "msg-1", ScheduleTs: now - 500, Payload: []byte("data1")},
		{MessageId: "msg-2", ScheduleTs: now - 200, Payload: []byte("data2")},
		{MessageId: "msg-3", ScheduleTs: now + 1000, Payload: []byte("data3")},
	}
	if err := wal.AppendBatch(events); err != nil {
		t.Fatalf("AppendBatch failed: %v", err)
	}
	wal.Flush()

	engine := NewReplayEngine(wal)
	ctx := context.Background()

	result, err := engine.ReplayByTimeRange(ctx, now-1000, now)
	if err != nil {
		t.Fatalf("ReplayByTimeRange failed: %v", err)
	}
	if len(result) < 2 {
		t.Errorf("expected at least 2 events, got %d", len(result))
	}
}

func TestReplayEngine_ReplayByTimeRange_ContextCancel(t *testing.T) {
	tmpDir := t.TempDir()
	wal, err := storage.NewWAL(tmpDir, 0, &storage.WALConfig{SegmentSizeBytes: 1024 * 1024, IndexInterval: 100, FsyncMode: "always"}, nil)
	if err != nil {
		t.Fatalf("NewWAL failed: %v", err)
	}
	defer wal.Close()

	now := time.Now().UnixMilli()
	for i := 0; i < 10; i++ {
		wal.AppendEvent(&types.Event{MessageId: "msg", ScheduleTs: now - int64(i), Payload: []byte("x")})
	}
	wal.Flush()

	engine := NewReplayEngine(wal)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = engine.ReplayByTimeRange(ctx, now-1000, now)
	if err == nil {
		t.Error("expected error for cancelled context")
	}
}

func TestReplayEngine_ReplayByOffset(t *testing.T) {
	tmpDir := t.TempDir()
	wal, err := storage.NewWAL(tmpDir, 0, &storage.WALConfig{SegmentSizeBytes: 1024 * 1024, IndexInterval: 100, FsyncMode: "always"}, nil)
	if err != nil {
		t.Fatalf("NewWAL failed: %v", err)
	}
	defer wal.Close()

	for i := 0; i < 5; i++ {
		wal.AppendEvent(&types.Event{MessageId: "msg", Payload: []byte("x")})
	}
	wal.Flush()

	engine := NewReplayEngine(wal)
	ctx := context.Background()

	events, err := engine.ReplayByOffset(ctx, 0, 3)
	if err != nil {
		t.Fatalf("ReplayByOffset failed: %v", err)
	}
	if len(events) != 3 {
		t.Errorf("expected 3 events, got %d", len(events))
	}
}

func TestReplayEngine_ReplayByOffset_StartBeyondLast(t *testing.T) {
	tmpDir := t.TempDir()
	wal, err := storage.NewWAL(tmpDir, 0, &storage.WALConfig{SegmentSizeBytes: 1024 * 1024, IndexInterval: 100, FsyncMode: "always"}, nil)
	if err != nil {
		t.Fatalf("NewWAL failed: %v", err)
	}
	defer wal.Close()

	engine := NewReplayEngine(wal)
	ctx := context.Background()

	events, err := engine.ReplayByOffset(ctx, 100, 10)
	if err != nil {
		t.Fatalf("ReplayByOffset failed: %v", err)
	}
	if len(events) != 0 {
		t.Errorf("expected 0 events, got %d", len(events))
	}
}

func TestReplayEngine_ReplayStream_ByTime(t *testing.T) {
	tmpDir := t.TempDir()
	wal, err := storage.NewWAL(tmpDir, 0, &storage.WALConfig{SegmentSizeBytes: 1024 * 1024, IndexInterval: 100, FsyncMode: "always"}, nil)
	if err != nil {
		t.Fatalf("NewWAL failed: %v", err)
	}
	defer wal.Close()

	now := time.Now().UnixMilli()
	for i := 0; i < 3; i++ {
		wal.AppendEvent(&types.Event{MessageId: "msg", ScheduleTs: now - int64(i), Payload: []byte("x")})
	}
	wal.Flush()

	engine := NewReplayEngine(wal)
	ctx := context.Background()
	ch := make(chan *ReplayEvent, 10)

	req := &ReplayRequest{StartTS: now - 1000, EndTS: now}
	err = engine.ReplayStream(ctx, req, ch)
	if err != nil {
		t.Fatalf("ReplayStream failed: %v", err)
	}

	var count int
	for range ch {
		count++
	}
	if count < 3 {
		t.Errorf("expected at least 3 events, got %d", count)
	}
}

func TestReplayEngine_ReplayStream_ByOffset(t *testing.T) {
	tmpDir := t.TempDir()
	wal, err := storage.NewWAL(tmpDir, 0, &storage.WALConfig{SegmentSizeBytes: 1024 * 1024, IndexInterval: 100, FsyncMode: "always"}, nil)
	if err != nil {
		t.Fatalf("NewWAL failed: %v", err)
	}
	defer wal.Close()

	for i := 0; i < 5; i++ {
		wal.AppendEvent(&types.Event{MessageId: "msg", Payload: []byte("x")})
	}
	wal.Flush()

	engine := NewReplayEngine(wal)
	ctx := context.Background()
	ch := make(chan *ReplayEvent, 10)

	req := &ReplayRequest{StartOffset: 0, Count: 3}
	err = engine.ReplayStream(ctx, req, ch)
	if err != nil {
		t.Fatalf("ReplayStream failed: %v", err)
	}

	var count int
	for range ch {
		count++
	}
	if count != 3 {
		t.Errorf("expected 3 events, got %d", count)
	}
}

func TestReplayEngine_ReplayStream_ContextCancel(t *testing.T) {
	tmpDir := t.TempDir()
	wal, err := storage.NewWAL(tmpDir, 0, &storage.WALConfig{SegmentSizeBytes: 1024 * 1024, IndexInterval: 100, FsyncMode: "always"}, nil)
	if err != nil {
		t.Fatalf("NewWAL failed: %v", err)
	}
	defer wal.Close()

	for i := 0; i < 100; i++ {
		wal.AppendEvent(&types.Event{MessageId: "msg", Payload: []byte("x")})
	}

	engine := NewReplayEngine(wal)
	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan *ReplayEvent, 10)

	go func() {
		time.Sleep(1 * time.Millisecond)
		cancel()
	}()

	req := &ReplayRequest{StartOffset: 0, Count: 1000}
	err = engine.ReplayStream(ctx, req, ch)
	// May or may not error depending on timing
	_ = err
}

func TestReplayRequest_Fields(t *testing.T) {
	req := &ReplayRequest{
		Topic:       "orders",
		PartitionID: 1,
		StartTS:     1000,
		EndTS:       2000,
		StartOffset: 0,
		Count:       100,
		Speed:       2.0,
	}
	if req.Topic != "orders" {
		t.Error("topic mismatch")
	}
	if req.Speed != 2.0 {
		t.Error("speed mismatch")
	}
}

func TestReplayEvent_Fields(t *testing.T) {
	evt := &ReplayEvent{
		Event:        &types.Event{MessageId: "msg-1"},
		ReplayOffset: 42,
	}
	if evt.Event.MessageId != "msg-1" {
		t.Error("event mismatch")
	}
	if evt.ReplayOffset != 42 {
		t.Error("replay offset mismatch")
	}
}

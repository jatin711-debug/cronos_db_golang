package storage

import (
	"os"
	"testing"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

func BenchmarkWAL_AppendEvent(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "wal_bench")
	if err != nil {
		b.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	config := &WALConfig{
		SegmentSizeBytes: 64 * 1024 * 1024, // 64MB to avoid rotation in benchmark
		IndexInterval:    10,
		FsyncMode:        "batch",
		FlushIntervalMS:  10,
	}

	wal, err := NewWAL(tmpDir, 0, config, nil)
	if err != nil {
		b.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	event := &types.Event{
		MessageId:  "msg-bench",
		ScheduleTs: 1000,
		Payload:    []byte("benchmark payload data"),
		Topic:      "benchmark",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := wal.AppendEvent(event); err != nil {
			b.Fatalf("Append failed: %v", err)
		}
	}
}

func BenchmarkWAL_AppendBatch(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "wal_bench_batch")
	if err != nil {
		b.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	config := &WALConfig{
		SegmentSizeBytes: 64 * 1024 * 1024,
		IndexInterval:    10,
		FsyncMode:        "batch",
		FlushIntervalMS:  10,
	}

	wal, err := NewWAL(tmpDir, 0, config, nil)
	if err != nil {
		b.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	events := []*types.Event{
		{MessageId: "msg-b1", ScheduleTs: 1000, Payload: []byte("batch payload"), Topic: "benchmark"},
		{MessageId: "msg-b2", ScheduleTs: 2000, Payload: []byte("batch payload"), Topic: "benchmark"},
		{MessageId: "msg-b3", ScheduleTs: 3000, Payload: []byte("batch payload"), Topic: "benchmark"},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := wal.AppendBatch(events); err != nil {
			b.Fatalf("AppendBatch failed: %v", err)
		}
	}
}

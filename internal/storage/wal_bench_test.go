package storage

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

// benchPayloads maps a descriptive name to a fixed payload size.
var benchPayloads = map[string]int{
	"64B":   64,
	"256B":  256,
	"4KB":   4096,
	"64KB":  64 * 1024,
	"256KB": 256 * 1024,
}

// benchFsyncModes are the modes under test. "periodic" and "batch" currently
// behave identically in the WAL (both flushed by the background loop), so the
// benchmark captures both before #12 differentiates them.
var benchFsyncModes = []string{"every_event", "batch", "periodic"}

// benchBatchSizes are the number of events per AppendBatch call.
var benchBatchSizes = []int{1, 10, 100, 1000}

// benchParallelism is the number of goroutines hammering the WAL concurrently.
var benchParallelism = []int{1, 4, 16}

// makeBenchEvents creates a reusable slice of events with the given payload size.
func makeBenchEvents(count, payloadSize int) []*types.Event {
	payload := make([]byte, payloadSize)
	for i := range payload {
		payload[i] = byte('a' + i%26)
	}
	events := make([]*types.Event, count)
	for i := 0; i < count; i++ {
		events[i] = &types.Event{
			MessageId:  fmt.Sprintf("msg-%d", i),
			ScheduleTs: time.Now().UnixMilli() + int64(i),
			Payload:    payload,
			Topic:      "benchmark",
		}
	}
	return events
}

// BenchmarkWAL_AppendBatch_Matrix measures the write-path throughput of the
// WAL across fsync modes, payload sizes, batch sizes, and parallelism levels.
// This is the primary benchmark used to validate Tier 3 throughput changes.
func BenchmarkWAL_AppendBatch_Matrix(b *testing.B) {
	for _, fsyncMode := range benchFsyncModes {
		for payloadName, payloadSize := range benchPayloads {
			for _, batchSize := range benchBatchSizes {
				for _, par := range benchParallelism {
					name := fmt.Sprintf("fsync=%s/payload=%s/batch=%d/par=%d", fsyncMode, payloadName, batchSize, par)
					b.Run(name, func(b *testing.B) {
						tmpDir := b.TempDir()
						config := &WALConfig{
							SegmentSizeBytes: 512 * 1024 * 1024, // 512MB to avoid rotation
							IndexInterval:    1000,
							FsyncMode:        fsyncMode,
							FlushIntervalMS:  10,
						}
						wal, err := NewWAL(tmpDir, 0, config, nil)
						if err != nil {
							b.Fatalf("NewWAL: %v", err)
						}
						defer wal.Close()

						// Pre-create a template event; the WAL mutates event.Offset/PartitionId,
						// so each iteration allocates its own event slice.
						template := makeBenchEvents(1, payloadSize)[0]
						payload := template.Payload

						b.ResetTimer()
						b.SetBytes(int64(batchSize * payloadSize))

						b.RunParallel(func(pb *testing.PB) {
							for pb.Next() {
								scratch := makeBenchEvents(batchSize, payloadSize)
								// Reuse the same payload backing store to keep allocation minimal.
								for _, ev := range scratch {
									ev.Payload = payload
								}
								if err := wal.AppendBatch(scratch); err != nil {
									b.Fatalf("AppendBatch: %v", err)
								}
							}
						})
					})
				}
			}
		}
	}
}

// BenchmarkWAL_AppendBatch_Encrypted is the encrypted counterpart of the matrix.
// It isolates the cost of AES-256-GCM on the write path.
func BenchmarkWAL_AppendBatch_Encrypted_Matrix(b *testing.B) {
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i)
	}
	cipher, err := NewSegmentCipher(key, 0)
	if err != nil {
		b.Fatalf("NewSegmentCipher: %v", err)
	}

	for _, fsyncMode := range benchFsyncModes {
		for payloadName, payloadSize := range benchPayloads {
			for _, batchSize := range benchBatchSizes {
				for _, par := range benchParallelism {
					name := fmt.Sprintf("fsync=%s/payload=%s/batch=%d/par=%d", fsyncMode, payloadName, batchSize, par)
					b.Run(name, func(b *testing.B) {
						tmpDir := b.TempDir()
						config := &WALConfig{
							SegmentSizeBytes: 512 * 1024 * 1024,
							IndexInterval:    1000,
							FsyncMode:        fsyncMode,
							FlushIntervalMS:  10,
						}
						wal, err := NewWAL(tmpDir, 0, config, cipher)
						if err != nil {
							b.Fatalf("NewWAL: %v", err)
						}
						defer wal.Close()

						template := makeBenchEvents(1, payloadSize)[0]
						payload := template.Payload

						b.ResetTimer()
						b.SetBytes(int64(batchSize * payloadSize))

						b.RunParallel(func(pb *testing.PB) {
							for pb.Next() {
								scratch := makeBenchEvents(batchSize, payloadSize)
								for _, ev := range scratch {
									ev.Payload = payload
								}
								if err := wal.AppendBatch(scratch); err != nil {
									b.Fatalf("AppendBatch: %v", err)
								}
							}
						})
					})
				}
			}
		}
	}
}

// BenchmarkWAL_AppendEvent_Single benchmarks the single-event path used by the
// legacy AppendEvent API. This path is expected to be dominated by syscall cost
// under every_event mode.
func BenchmarkWAL_AppendEvent_Single(b *testing.B) {
	for _, fsyncMode := range benchFsyncModes {
		for payloadName, payloadSize := range benchPayloads {
			for _, par := range benchParallelism {
				name := fmt.Sprintf("fsync=%s/payload=%s/par=%d", fsyncMode, payloadName, par)
				b.Run(name, func(b *testing.B) {
					tmpDir := b.TempDir()
					config := &WALConfig{
						SegmentSizeBytes: 512 * 1024 * 1024,
						IndexInterval:    1000,
						FsyncMode:        fsyncMode,
						FlushIntervalMS:  10,
					}
					wal, err := NewWAL(tmpDir, 0, config, nil)
					if err != nil {
						b.Fatalf("NewWAL: %v", err)
					}
					defer wal.Close()

					payload := make([]byte, payloadSize)
					for i := range payload {
						payload[i] = byte('a' + i%26)
					}
					event := &types.Event{
						MessageId:  "msg-single",
						ScheduleTs: time.Now().UnixMilli(),
						Payload:    payload,
						Topic:      "benchmark",
					}

					b.ResetTimer()
					b.SetBytes(int64(payloadSize))

					b.RunParallel(func(pb *testing.PB) {
						for pb.Next() {
							if err := wal.AppendEvent(event); err != nil {
								b.Fatalf("AppendEvent: %v", err)
							}
						}
					})
				})
			}
		}
	}
}

// oldBenchmarkWAL_AppendEvent is preserved for comparison until the new matrix
// stabilizes, then it can be removed.
func BenchmarkWAL_AppendEvent_Legacy(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "wal_bench")
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

// oldBenchmarkWAL_AppendBatch is preserved for comparison.
func BenchmarkWAL_AppendBatch_Legacy(b *testing.B) {
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

package storage

import (
	"fmt"
	"os"
	"testing"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

// TestSegment_WriteGrowsMmapSufficiently is a regression test for a crash on the
// append path: writeRecordMmap grew the mapping by only mmapSize*2, which is
// insufficient when a single record exceeds the current mapped size, and it
// bounded the copy by mmapSize (bookkeeping) instead of len(mmapData) (the real
// buffer). A segment reopened with a small mmap that then took large writes could
// advance mmapWritePos past len(mmapData) and panic
// ("slice bounds out of range"). This writes records larger than the initial
// preallocation to force the grow path, then reads them back.
func TestSegment_WriteGrowsMmapSufficiently(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "seg_grow_test")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Tiny preallocation so the very first append must grow the mmap, and each
	// record (large payload) exceeds the current mapped size — the insufficient
	// doubling scenario.
	seg, err := NewSegmentWithSize(tmpDir, 0, true, nil, 256)
	if err != nil {
		t.Fatalf("NewSegmentWithSize: %v", err)
	}
	defer seg.Close()

	const n = 20
	bigPayload := make([]byte, 8192) // each record far larger than the 256B prealloc
	for i := range bigPayload {
		bigPayload[i] = byte(i)
	}
	for i := 0; i < n; i++ {
		ev := &types.Event{
			MessageId:  fmt.Sprintf("big-%d", i),
			ScheduleTs: int64(1000 + i),
			Payload:    bigPayload,
			Topic:      "test",
			Offset:     int64(i), // Segment.AppendEvent uses pre-assigned offsets
		}
		if err := seg.AppendEvent(ev, 4); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	if err := seg.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	events, err := seg.ReadEventsByOffsetRange(0, n-1)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	if len(events) != n {
		t.Fatalf("expected %d events after mmap growth, got %d", n, len(events))
	}
	for i, ev := range events {
		if ev.Offset != int64(i) || len(ev.Payload) != len(bigPayload) {
			t.Fatalf("event %d corrupted: offset=%d payloadLen=%d", i, ev.Offset, len(ev.Payload))
		}
	}
}

// TestWAL_TruncateToOffset exercises follower-log truncation: appending events,
// truncating the divergent tail back to an offset, and asserting the WAL rewinds
// (next offset, reads, and that new appends land contiguously at the truncation
// point). Covers both a single-segment truncation and one spanning rotation.
func TestWAL_TruncateToOffset(t *testing.T) {
	for _, tc := range []struct {
		name       string
		segSize    int64
		total      int
		truncateTo int64
	}{
		{"single_segment", 64 * 1024 * 1024, 30, 20},
		{"across_segments", 400, 60, 15}, // tiny segments → truncation spans rotations
	} {
		t.Run(tc.name, func(t *testing.T) {
			tmpDir, err := os.MkdirTemp("", "wal_truncate_test")
			if err != nil {
				t.Fatalf("temp dir: %v", err)
			}
			defer os.RemoveAll(tmpDir)

			config := &WALConfig{
				SegmentSizeBytes: tc.segSize,
				IndexInterval:    2,
				FsyncMode:        "batch",
				FlushIntervalMS:  50,
			}
			wal, err := NewWAL(tmpDir, 0, config, nil)
			if err != nil {
				t.Fatalf("create WAL: %v", err)
			}
			defer wal.Close()

			for i := 0; i < tc.total; i++ {
				ev := &types.Event{
					MessageId:  fmt.Sprintf("t-%d", i),
					ScheduleTs: int64(1000 + i),
					Payload:    []byte(fmt.Sprintf("p-%d", i)),
					Topic:      "test",
				}
				if err := wal.AppendEvent(ev); err != nil {
					t.Fatalf("append %d: %v", i, err)
				}
			}
			if err := wal.Flush(); err != nil {
				t.Fatalf("flush: %v", err)
			}

			removed, err := wal.TruncateToOffset(tc.truncateTo)
			if err != nil {
				t.Fatalf("TruncateToOffset: %v", err)
			}
			if want := int64(tc.total) - tc.truncateTo; int64(removed) != want {
				t.Fatalf("removed %d events, want %d", removed, want)
			}

			if got := wal.GetNextOffset(); got != tc.truncateTo {
				t.Fatalf("next offset after truncate = %d, want %d", got, tc.truncateTo)
			}

			// Surviving events must be readable and contiguous.
			survivors, err := wal.ReadEvents(0, tc.truncateTo-1)
			if err != nil {
				t.Fatalf("read survivors: %v", err)
			}
			if int64(len(survivors)) != tc.truncateTo {
				t.Fatalf("expected %d surviving events, got %d", tc.truncateTo, len(survivors))
			}
			for i, ev := range survivors {
				if ev.Offset != int64(i) {
					t.Fatalf("survivor %d has offset %d", i, ev.Offset)
				}
			}

			// Reading past the truncation point returns nothing.
			after, err := wal.ReadEvents(tc.truncateTo, int64(tc.total))
			if err != nil {
				t.Fatalf("read after truncate: %v", err)
			}
			if len(after) != 0 {
				t.Fatalf("expected no events past truncation, got %d", len(after))
			}

			// New appends must land contiguously at the truncation point.
			newEv := &types.Event{MessageId: "post-trunc", ScheduleTs: 9999, Payload: []byte("x"), Topic: "test"}
			if err := wal.AppendEvent(newEv); err != nil {
				t.Fatalf("append after truncate: %v", err)
			}
			if err := wal.Flush(); err != nil {
				t.Fatalf("flush after truncate: %v", err)
			}
			got, err := wal.ReadEvent(tc.truncateTo)
			if err != nil {
				t.Fatalf("read new event: %v", err)
			}
			if got.GetMessageId() != "post-trunc" {
				t.Fatalf("new event at offset %d = %q, want post-trunc", tc.truncateTo, got.GetMessageId())
			}
		})
	}
}

// TestWAL_ReadAfterRotation is a regression test for the rotated-segment read
// bug: rotation used to Close() the old active segment's file handle while
// keeping it in w.segments, so any read of a rotated segment failed with
// os.ErrClosed (breaking Replay, follower catch-up, cross-region fetch). This
// test forces several rotations, then reads from the earliest offset (which
// lives in a rotated, now-inactive segment) and asserts success.
func TestWAL_ReadAfterRotation(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "wal_rotation_test")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	config := &WALConfig{
		SegmentSizeBytes: 500, // tiny → forces frequent rotation
		IndexInterval:    2,
		FsyncMode:        "batch",
		FlushIntervalMS:  50,
	}

	wal, err := NewWAL(tmpDir, 0, config, nil)
	if err != nil {
		t.Fatalf("create WAL: %v", err)
	}
	defer wal.Close()

	const total = 40
	for i := 0; i < total; i++ {
		ev := &types.Event{
			MessageId:  fmt.Sprintf("rot-%d", i),
			ScheduleTs: int64(1000 + i),
			Payload:    []byte(fmt.Sprintf("payload-%d", i)),
			Topic:      "test",
		}
		if err := wal.AppendEvent(ev); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	if err := wal.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// Sanity: rotation must actually have happened for this test to be meaningful.
	if got := len(wal.GetSegments()); got < 2 {
		t.Fatalf("expected multiple segments after rotation, got %d", got)
	}

	// Read the earliest events — these live in a rotated (deactivated) segment.
	early, err := wal.ReadEvents(0, 4)
	if err != nil {
		t.Fatalf("read from rotated segment failed: %v", err)
	}
	if len(early) != 5 {
		t.Fatalf("expected 5 early events from rotated segment, got %d", len(early))
	}
	for i, ev := range early {
		if ev.Offset != int64(i) || ev.MessageId != fmt.Sprintf("rot-%d", i) {
			t.Fatalf("event %d mismatch: offset=%d id=%q", i, ev.Offset, ev.MessageId)
		}
	}

	// Full-range read across rotated + active segments.
	all, err := wal.ReadEvents(0, int64(total-1))
	if err != nil {
		t.Fatalf("full read failed: %v", err)
	}
	if len(all) != total {
		t.Fatalf("expected %d events across all segments, got %d", total, len(all))
	}
}

// TestWAL_AppendAfterReopenThenRead is a regression test for the restart
// corruption bug: on reopen, a segment's mmap write cursor was left at the
// preallocated EOF (not the real data end), so the first append after reopen
// wrote ~prealloc-size bytes past the data, leaving a zero gap that silently
// truncated all subsequent range reads. This test appends, closes, reopens,
// appends more, and asserts every event is readable and contiguous.
func TestWAL_AppendAfterReopenThenRead(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "wal_reopen_test")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	config := &WALConfig{
		// Large segment so everything stays in one preallocated segment; this is
		// exactly the scenario that triggered the bug (big prealloc tail).
		SegmentSizeBytes: 64 * 1024 * 1024,
		IndexInterval:    8,
		FsyncMode:        "batch",
		FlushIntervalMS:  50,
	}

	const firstBatch = 20
	const secondBatch = 20

	// Phase 1: append firstBatch events and close cleanly.
	wal, err := NewWAL(tmpDir, 0, config, nil)
	if err != nil {
		t.Fatalf("create WAL: %v", err)
	}
	for i := 0; i < firstBatch; i++ {
		ev := &types.Event{
			MessageId:  fmt.Sprintf("msg-%d", i),
			ScheduleTs: int64(1000 + i),
			Payload:    []byte(fmt.Sprintf("payload-%d", i)),
			Topic:      "test",
		}
		if err := wal.AppendEvent(ev); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	if err := wal.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if err := wal.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Phase 2: reopen and append secondBatch more events.
	wal2, err := NewWAL(tmpDir, 0, config, nil)
	if err != nil {
		t.Fatalf("reopen WAL: %v", err)
	}
	defer wal2.Close()

	if got := wal2.GetNextOffset(); got != firstBatch {
		t.Fatalf("after reopen expected next offset %d, got %d", firstBatch, got)
	}

	for i := firstBatch; i < firstBatch+secondBatch; i++ {
		ev := &types.Event{
			MessageId:  fmt.Sprintf("msg-%d", i),
			ScheduleTs: int64(1000 + i),
			Payload:    []byte(fmt.Sprintf("payload-%d", i)),
			Topic:      "test",
		}
		if err := wal2.AppendEvent(ev); err != nil {
			t.Fatalf("append after reopen %d: %v", i, err)
		}
	}
	if err := wal2.Flush(); err != nil {
		t.Fatalf("flush after reopen: %v", err)
	}

	// Phase 3: read the full range back. Before the fix, the post-reopen appends
	// landed at the preallocated EOF, so this read returned only the first batch
	// (range read stops at the zero gap).
	total := firstBatch + secondBatch
	events, err := wal2.ReadEvents(0, int64(total-1))
	if err != nil {
		t.Fatalf("read events: %v", err)
	}
	if len(events) != total {
		t.Fatalf("expected %d events after reopen+append, got %d (zero-gap corruption)", total, len(events))
	}
	for i, ev := range events {
		if ev.Offset != int64(i) {
			t.Fatalf("event %d has non-contiguous offset %d", i, ev.Offset)
		}
		want := fmt.Sprintf("msg-%d", i)
		if ev.MessageId != want {
			t.Fatalf("event at offset %d: expected message_id %q, got %q", i, want, ev.MessageId)
		}
	}
}

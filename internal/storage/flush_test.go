package storage

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

func flushedPos(s *Segment) int64 {
	s.flushMu.Lock()
	defer s.flushMu.Unlock()
	return s.mmapFlushedPos
}

func writePos(s *Segment) int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.mmapWritePos
}

// Flushes only cover bytes written since the previous flush, so the flushed
// mark has to follow the write position exactly: across repeated flushes, and
// backwards when a truncation removes already-flushed records that later
// appends then overwrite.
func TestSegment_IncrementalFlushTracksWritePosition(t *testing.T) {
	seg, err := NewSegment(t.TempDir(), 0, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer seg.Close()
	if seg.mmapData == nil {
		t.Skip("segment is not memory-mapped on this platform")
	}

	appendRange := func(from, to int64) {
		t.Helper()
		for offset := from; offset < to; offset++ {
			if err := seg.AppendEvent(makeEvent(offset, fmt.Sprintf("m-%d", offset), "flush"), 1); err != nil {
				t.Fatalf("append %d: %v", offset, err)
			}
		}
	}

	appendRange(0, 50)
	if err := seg.FlushBuffer(); err != nil {
		t.Fatal(err)
	}
	if got, want := flushedPos(seg), writePos(seg); got != want {
		t.Fatalf("after FlushBuffer flushed=%d, written=%d", got, want)
	}

	appendRange(50, 120)
	if err := seg.Sync(); err != nil {
		t.Fatal(err)
	}
	if got, want := flushedPos(seg), writePos(seg); got != want {
		t.Fatalf("after Sync flushed=%d, written=%d", got, want)
	}

	if _, err := seg.TruncateAfterOffset(19); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	if got, limit := flushedPos(seg), writePos(seg); got > limit {
		t.Fatalf("flushed mark %d is beyond the truncated end %d; rewritten records would never be flushed", got, limit)
	}

	appendRange(20, 60)
	if err := seg.Sync(); err != nil {
		t.Fatal(err)
	}
	if got, want := flushedPos(seg), writePos(seg); got != want {
		t.Fatalf("after re-append flushed=%d, written=%d", got, want)
	}
	events, err := seg.ReadEventsByOffsetRange(0, 59)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 60 {
		t.Fatalf("read %d events after truncate and re-append, want 60", len(events))
	}
}

// The background flush must not hold the WAL lock while it does disk I/O: an
// append has to complete even while a flush of the same WAL is stuck.
func TestWAL_AppendDoesNotWaitForBackgroundFlush(t *testing.T) {
	wal, err := NewWAL(t.TempDir(), 0, &WALConfig{SegmentSizeBytes: 1 << 20, IndexInterval: 1, FsyncMode: "periodic", FlushIntervalMS: 0}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()

	event := func(i int) *types.Event {
		return &types.Event{MessageId: fmt.Sprintf("m-%d", i), Topic: "flush", Payload: []byte("payload"), ScheduleTs: 1}
	}
	if err := wal.AppendBatch([]*types.Event{event(0)}); err != nil {
		t.Fatal(err)
	}

	// Hold the segment's flush lock, as a slow fsync would, and start a
	// background flush that has to wait for it.
	wal.mu.RLock()
	seg := wal.activeSegment
	wal.mu.RUnlock()
	seg.flushMu.Lock()
	var unlockOnce sync.Once
	unlock := func() { unlockOnce.Do(seg.flushMu.Unlock) }
	defer unlock()

	var flushing atomic.Bool
	flushed := make(chan struct{})
	go func() {
		flushing.Store(true)
		_, _, _ = wal.backgroundFlushBuffer()
		close(flushed)
	}()
	for !flushing.Load() {
		time.Sleep(time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond) // let the flush reach the held lock

	appended := make(chan error, 1)
	go func() { appended <- wal.AppendBatch([]*types.Event{event(1)}) }()
	select {
	case err := <-appended:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("append blocked behind an in-flight flush of the same WAL")
	}

	unlock()
	select {
	case <-flushed:
	case <-time.After(3 * time.Second):
		t.Fatal("background flush did not finish after the flush lock was released")
	}
}

// In batch mode an append may only be acknowledged by a sync that started
// after it was written. A writer that arrives while a sync is in flight must
// therefore wait for the following sync, which covers its bytes, rather than
// share the result of the one already running.
func TestWAL_GroupCommitCoversWritersThatArriveDuringSync(t *testing.T) {
	wal, err := NewWAL(t.TempDir(), 0, &WALConfig{SegmentSizeBytes: 1 << 20, IndexInterval: 1, FsyncMode: "batch", FlushIntervalMS: 0}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	wal.mu.RLock()
	seg := wal.activeSegment
	wal.mu.RUnlock()
	if seg.mmapData == nil {
		t.Skip("segment is not memory-mapped on this platform")
	}

	appendOne := func(id string) chan error {
		done := make(chan error, 1)
		go func() {
			done <- wal.AppendBatch([]*types.Event{{MessageId: id, Topic: "flush", Payload: []byte("payload"), ScheduleTs: 1}})
		}()
		return done
	}
	waitForOffset := func(offset int64) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for wal.GetLastOffset() < offset {
			if time.Now().After(deadline) {
				t.Fatalf("append of offset %d did not reach the segment", offset)
			}
			time.Sleep(time.Millisecond)
		}
		time.Sleep(50 * time.Millisecond) // let the writer reach its sync
	}

	// Stall the first writer's sync after it has captured its write position.
	seg.flushMu.Lock()
	var unlockOnce sync.Once
	unlock := func() { unlockOnce.Do(seg.flushMu.Unlock) }
	defer unlock()

	first := appendOne("first")
	waitForOffset(0)
	second := appendOne("second")
	waitForOffset(1)

	select {
	case err := <-first:
		t.Fatalf("first append returned before its sync could run: %v", err)
	case err := <-second:
		t.Fatalf("second append returned while the only sync was still stalled: %v", err)
	default:
	}

	unlock()
	for name, done := range map[string]chan error{"first": first, "second": second} {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("%s append: %v", name, err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("%s append did not finish", name)
		}
	}
	if flushed, written := flushedPos(seg), writePos(seg); flushed < written {
		t.Fatalf("second append was acknowledged with %d of %d bytes flushed: it shared a sync that started before it was written", flushed, written)
	}
}

// Many concurrent batch-mode writers share syncs without losing or reordering
// anything, including across segment rotations.
func TestWAL_GroupCommitConcurrentWriters(t *testing.T) {
	wal, err := NewWAL(t.TempDir(), 0, &WALConfig{SegmentSizeBytes: 16 << 10, IndexInterval: 1, FsyncMode: "batch", FlushIntervalMS: 0}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()

	const writers, perWriter = 8, 40
	var wg sync.WaitGroup
	errs := make(chan error, writers)
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				batch := []*types.Event{
					{MessageId: fmt.Sprintf("w%d-%d-a", w, i), Topic: "flush", Payload: []byte("payload"), ScheduleTs: 1},
					{MessageId: fmt.Sprintf("w%d-%d-b", w, i), Topic: "flush", Payload: []byte("payload"), ScheduleTs: 1},
				}
				if err := wal.AppendBatch(batch); err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	const total = writers * perWriter * 2
	events, err := wal.ReadEvents(0, total-1)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != total {
		t.Fatalf("read %d events, want %d", len(events), total)
	}
	seen := make(map[string]bool, total)
	for i, event := range events {
		if event.Offset != int64(i) {
			t.Fatalf("offset %d at position %d", event.Offset, i)
		}
		if seen[event.MessageId] {
			t.Fatalf("duplicate record %s", event.MessageId)
		}
		seen[event.MessageId] = true
	}
}

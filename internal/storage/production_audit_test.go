package storage

import (
	"fmt"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
	"testing"
)

func TestAuditUnorderedTimestampsAcrossRotationAndRestart(t *testing.T) {
	dir := t.TempDir()
	cfg := &WALConfig{SegmentSizeBytes: 512, IndexInterval: 1, FsyncMode: "batch", FlushIntervalMS: 100}
	w, err := NewWAL(dir, 0, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if w != nil {
			w.Close()
		}
	})
	for i, ts := range []int64{5000, 1000, 4000, 1500, 6000, 1750} {
		if err := w.AppendEvent(&types.Event{MessageId: fmt.Sprintf("event-%d", i), Topic: "audit", Payload: make([]byte, 512), ScheduleTs: ts}); err != nil {
			t.Fatal(err)
		}
	}
	if len(w.GetSegments()) < 2 {
		t.Fatal("test did not rotate segments")
	}
	for reopen := 0; reopen < 2; reopen++ {
		events, err := w.ReadEventsByTime(1000, 2000)
		if err != nil {
			t.Fatal(err)
		}
		want := map[string]bool{"event-1": true, "event-3": true, "event-5": true}
		for _, event := range events {
			if !want[event.MessageId] {
				t.Fatalf("unexpected or duplicate event: %s", event.MessageId)
			}
			delete(want, event.MessageId)
		}
		if len(want) != 0 {
			t.Fatalf("missing records across segments (reopen=%d): %v", reopen, want)
		}
		if reopen == 0 {
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			w, err = NewWAL(dir, 0, cfg, nil)
			if err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestAuditUnorderedTimestampsRemainQueryable(t *testing.T) {
	cfg := &WALConfig{SegmentSizeBytes: 1 << 20, IndexInterval: 1, FsyncMode: "batch", FlushIntervalMS: 100}
	w, err := NewWAL(t.TempDir(), 0, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	for _, ts := range []int64{5000, 1000, 4000} {
		if err := w.AppendEvent(&types.Event{MessageId: "audit", Topic: "audit", Payload: []byte("payload"), ScheduleTs: ts}); err != nil {
			t.Fatal(err)
		}
	}
	events, err := w.ReadEventsByTime(1000, 2000)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 {
		t.Fatalf("time-range lookup missed out-of-order event: got %d, want 1", len(events))
	}
}
func TestAuditWrongEncryptionKeyFailsClosed(t *testing.T) {
	dir := t.TempDir()
	cfg := &WALConfig{SegmentSizeBytes: 1 << 20, IndexInterval: 1, FsyncMode: "batch", FlushIntervalMS: 100}
	key1 := make([]byte, 32)
	key2 := make([]byte, 32)
	key2[0] = 1
	cipher1, _ := NewSegmentCipher(key1, 0)
	cipher2, _ := NewSegmentCipher(key2, 0)
	w, err := NewWAL(dir, 0, cfg, cipher1)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.AppendEvent(&types.Event{MessageId: "protected", Payload: []byte("secret"), ScheduleTs: 1}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	w2, err := NewWAL(dir, 0, cfg, cipher2)
	if err == nil {
		defer w2.Close()
		t.Fatalf("wrong key opened populated WAL as writable: nextOffset=%d", w2.GetNextOffset())
	}
}

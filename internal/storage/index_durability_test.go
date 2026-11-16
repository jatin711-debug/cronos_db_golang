package storage

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

func appendNumbered(t *testing.T, w *WAL, from, to int) {
	t.Helper()
	for start := from; start < to; start += 100 {
		batch := make([]*types.Event, 0, 100)
		for i := start; i < min(start+100, to); i++ {
			batch = append(batch, &types.Event{MessageId: fmt.Sprintf("m-%d", i), Topic: "orders", Payload: []byte("payload"), ScheduleTs: int64(i)})
		}
		if err := w.AppendBatch(batch); err != nil {
			t.Fatal(err)
		}
	}
}

func indexFileEntries(t *testing.T, dir string) int64 {
	t.Helper()
	info, err := os.Stat(filepath.Join(dir, "index", fmt.Sprintf("%020d.index", 0)))
	if err != nil {
		t.Fatal(err)
	}
	return info.Size() / indexEntrySize
}

// A WAL that a coalescer flushes has its index written to disk too, not only
// when its segment closes. The coalescer used to flush and sync the records
// alone, so a node that was killed lost its active segments' index.
func TestFsyncCoalescerWritesTheIndex(t *testing.T) {
	dir := t.TempDir()
	coalescer := NewFsyncCoalescer(20 * time.Millisecond)
	defer coalescer.Close()
	w, err := NewWALWithCoalescer(dir, 0, &WALConfig{SegmentSizeBytes: 32 << 20, IndexInterval: 10, FsyncMode: "every_event", FlushIntervalMS: 20}, nil, coalescer)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	appendNumbered(t, w, 0, 500)

	deadline := time.Now().Add(5 * time.Second)
	for indexFileEntries(t, dir) < 50 {
		if time.Now().After(deadline) {
			t.Fatalf("%d index entries on disk after the coalescer ran, want 50", indexFileEntries(t, dir))
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// A segment whose index on disk stops short, as a killed process leaves it,
// adds the missing entries when it is opened, and point reads beyond the old
// end of the index find their records.
func TestOpenSegmentAddsMissingIndexEntries(t *testing.T) {
	key := bytes.Repeat([]byte{4}, 32)
	cipher, err := NewSegmentCipher(key, 0)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	open := func() *WAL {
		w, err := NewWAL(dir, 0, &WALConfig{SegmentSizeBytes: 32 << 20, IndexInterval: 1000, FsyncMode: "batch"}, cipher)
		if err != nil {
			t.Fatal(err)
		}
		return w
	}
	const total = 5500
	w := open()
	appendNumbered(t, w, 0, total)
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Keep only the first entry, as if the rest had never left the buffer.
	path := filepath.Join(dir, "index", fmt.Sprintf("%020d.index", 0))
	if err := os.Truncate(path, indexEntrySize); err != nil {
		t.Fatal(err)
	}

	w = open()
	if got := indexFileEntries(t, dir); got < total/rebuiltIndexInterval {
		t.Fatalf("%d index entries after reopening, want at least %d", got, total/rebuiltIndexInterval)
	}
	entries := w.segments[len(w.segments)-1].index.entriesAfter(-1)
	for i := 1; i < len(entries); i++ {
		if gap := entries[i].Offset - entries[i-1].Offset; gap > rebuiltIndexInterval {
			t.Fatalf("index entries %d and %d are %d records apart", entries[i-1].Offset, entries[i].Offset, gap)
		}
	}
	for _, offset := range []int64{0, 999, 1000, 2500, 4999, total - 1} {
		event, err := w.ReadEvent(offset)
		if err != nil || event.MessageId != fmt.Sprintf("m-%d", offset) {
			t.Fatalf("read offset %d: %v, %v", offset, event, err)
		}
	}

	// Opening a segment whose index is whole adds nothing.
	before := indexFileEntries(t, dir)
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	w = open()
	defer w.Close()
	if after := indexFileEntries(t, dir); after != before {
		t.Fatalf("reopening a whole index changed it from %d to %d entries", before, after)
	}
}

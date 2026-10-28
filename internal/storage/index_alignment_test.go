package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

func indexTestEvent(i int) *types.Event {
	return &types.Event{MessageId: fmt.Sprintf("m-%d", i), Topic: "t", Payload: []byte("p"), ScheduleTs: 1_700_000_000_000 + int64(i)}
}

// The index buffer puts out whole entries when it fills. It used to put out
// 65536 bytes, 16 bytes into an entry, and the file stayed that way until the
// next flush, which in production came only when the segment was closed.
func TestIndexBufferPutsOutWholeEntries(t *testing.T) {
	dir := t.TempDir()
	cfg := &WALConfig{SegmentSizeBytes: 64 << 20, IndexInterval: 1, FsyncMode: "batch"}
	w, err := NewWAL(dir, 0, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	for i := 0; i < 2800; i++ {
		if err := w.AppendEvent(indexTestEvent(i)); err != nil {
			t.Fatal(err)
		}
	}
	st, err := os.Stat(filepath.Join(dir, "index", w.GetActiveSegment().indexFilename))
	if err != nil {
		t.Fatal(err)
	}
	if st.Size() == 0 || st.Size()%indexEntrySize != 0 {
		t.Fatalf("the index file is %d bytes, which is not a whole number of %d-byte entries", st.Size(), indexEntrySize)
	}
}

// An index file that ends inside an entry, as a crash during a write leaves,
// is cut back to its last whole entry when it is opened. Entries appended
// after a restart used to land 16 bytes off, and after the next restart reads
// of the events they point to returned nothing, and no error.
func TestIndexPartialEntryAfterCrash(t *testing.T) {
	dir := t.TempDir()
	cfg := &WALConfig{SegmentSizeBytes: 64 << 20, IndexInterval: 1, FsyncMode: "batch"}
	w, err := NewWAL(dir, 0, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2800; i++ {
		if err := w.AppendEvent(indexTestEvent(i)); err != nil {
			t.Fatal(err)
		}
	}
	idxPath := filepath.Join(dir, "index", w.GetActiveSegment().indexFilename)
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(idxPath, 65536); err != nil { // 2730 entries and 16 bytes of the next
		t.Fatal(err)
	}

	w, err = NewWAL(dir, 0, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := 2800; i < 2810; i++ {
		if err := w.AppendEvent(indexTestEvent(i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	w, err = NewWAL(dir, 0, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if last := w.GetLastOffset(); last != 2809 {
		t.Fatalf("last offset %d, want 2809", last)
	}
	got, err := w.ReadEvents(2730, 2809)
	if err != nil || len(got) != 80 {
		t.Fatalf("ReadEvents(2730, 2809) returned %d events (err=%v), want 80", len(got), err)
	}
	if _, err := w.ReadEvent(2805); err != nil {
		t.Fatalf("ReadEvent(2805): %v", err)
	}
	if st, _ := os.Stat(idxPath); st.Size()%indexEntrySize != 0 {
		t.Fatalf("the index file is %d bytes, not a whole number of entries", st.Size())
	}
}

// An index entry only says where to start reading. One that points to the
// wrong place, the middle of a record or past the last one, does not make a
// read come back empty: the segment is read from its start instead.
func TestReadsDoNotTrustAWrongIndexEntry(t *testing.T) {
	dir := t.TempDir()
	cfg := &WALConfig{SegmentSizeBytes: 64 << 20, IndexInterval: 1000, FsyncMode: "batch"}
	w, err := NewWAL(dir, 0, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	for i := 0; i < 100; i++ {
		if err := w.AppendEvent(indexTestEvent(i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	seg := w.GetActiveSegment()
	for _, wrong := range []struct{ offset, position int64 }{
		{50, 64 + 7},             // inside the first record
		{60, seg.GetSize() - 30}, // inside the last record
		{70, seg.GetSize()},      // past the last record
	} {
		if err := seg.index.AddEntry(1, wrong.offset, wrong.position); err != nil {
			t.Fatal(err)
		}
	}
	for _, r := range [][2]int64{{50, 59}, {60, 69}, {70, 99}} {
		got, err := w.ReadEvents(r[0], r[1])
		if err != nil || int64(len(got)) != r[1]-r[0]+1 || got[0].Offset != r[0] {
			t.Fatalf("ReadEvents(%d, %d) returned %d events (err=%v)", r[0], r[1], len(got), err)
		}
	}
	for _, offset := range []int64{55, 65, 75} {
		event, err := w.ReadEvent(offset)
		if err != nil || event.Offset != offset {
			t.Fatalf("ReadEvent(%d) = %v, %v", offset, event, err)
		}
	}
}

package storage

import (
	"bytes"
	"fmt"
	"io"
	"io/fs"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

// countingReaderAt counts the bytes read through it.
type countingReaderAt struct {
	io.ReaderAt
	read int64
}

func (c *countingReaderAt) ReadAt(p []byte, off int64) (int, error) {
	n, err := c.ReaderAt.ReadAt(p, off)
	c.read += int64(n)
	return n, err
}

// The scanner passes over records by their lengths and then returns the next
// one as if it had read them all, wherever the records passed over lie in its
// read-ahead blocks. It does not read a record larger than a block to pass it.
func TestRecordScanner_SkipsRecordsByTheirLengths(t *testing.T) {
	bodies := [][]byte{
		body(10, 'a'),
		body(recordScanBlock-40, 'b'), // ends just short of the first block
		body(100, 'c'),                // straddles the boundary
		body(5*recordScanBlock, 'd'),  // much larger than the read-ahead block
		body(1, 'e'),
		body(300, 'f'),
	}
	const start = 64
	file := make([]byte, start)
	positions := make([]int64, len(bodies))
	for i, b := range bodies {
		positions[i] = int64(len(file)) + 4
		file = append(file, frame(b)...)
	}
	file = append(file, make([]byte, 4096)...) // preallocated zero tail

	for skip := 0; skip <= len(bodies); skip++ {
		scanner := newRecordScanner(bytes.NewReader(file), start)
		if ok, err := scanner.skip(int64(skip)); err != nil || !ok {
			t.Fatalf("skip %d: ok=%v err=%v", skip, ok, err)
		}
		for i := skip; i < len(bodies); i++ {
			got, pos, ok, err := scanner.record()
			if err != nil || !ok {
				t.Fatalf("skip %d, record %d: ok=%v err=%v", skip, i, ok, err)
			}
			if !bytes.Equal(got, bodies[i]) || pos != positions[i] {
				t.Fatalf("skip %d, record %d: %d bytes at %d, want %d at %d", skip, i, len(got), pos, len(bodies[i]), positions[i])
			}
		}
		if _, _, ok, err := scanner.record(); ok || err != nil {
			t.Fatalf("skip %d: the zero tail must end the scan cleanly: ok=%v err=%v", skip, ok, err)
		}
	}

	// More records than there are.
	scanner := newRecordScanner(bytes.NewReader(file), start)
	if ok, err := scanner.skip(int64(len(bodies) + 1)); ok || err != nil {
		t.Fatalf("skipped past the last record: ok=%v err=%v", ok, err)
	}

	// The large record is passed, not read.
	counted := &countingReaderAt{ReaderAt: bytes.NewReader(file)}
	scanner = newRecordScanner(counted, start)
	if ok, err := scanner.skip(4); err != nil || !ok {
		t.Fatalf("skip 4: ok=%v err=%v", ok, err)
	}
	if got, _, ok, err := scanner.record(); err != nil || !ok || !bytes.Equal(got, bodies[4]) {
		t.Fatalf("record after the large one: ok=%v err=%v", ok, err)
	}
	if counted.read > 3*recordScanBlock {
		t.Fatalf("read %d bytes to pass a record of %d", counted.read, len(bodies[3]))
	}
}

// Passing over a record that the file ends inside of, or a length that is not
// one, ends the scan without an error.
func TestRecordScanner_SkipStopsWhereTheRecordsDo(t *testing.T) {
	complete := frame(body(50, 'x'))
	for name, file := range map[string][]byte{
		"truncated record":       append(append([]byte{}, complete...), frame(body(500, 'y'))[:200]...),
		"truncated large record": append(append([]byte{}, complete...), frame(body(2*recordScanBlock, 'y'))[:recordScanBlock+9]...),
		"partial length":         append(append([]byte{}, complete...), 0, 0),
		"zero tail":              append(append([]byte{}, complete...), make([]byte, 64)...),
		"clean end":              complete,
	} {
		scanner := newRecordScanner(bytes.NewReader(file), 0)
		if ok, err := scanner.skip(1); err != nil || !ok {
			t.Fatalf("%s: first record ok=%v err=%v", name, ok, err)
		}
		ok, err := scanner.skip(1)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if ok {
			// The lengths alone cannot tell that the file ends inside the
			// record; reading it does.
			if _, _, ok, err := scanner.record(); ok || err != nil {
				t.Fatalf("%s: a record after the end of the data: ok=%v err=%v", name, ok, err)
			}
		}
	}
}

func rangeReadEvents(total int) [][]*types.Event {
	var batches [][]*types.Event
	for start := 0; start < total; start += 50 {
		batch := make([]*types.Event, 0, 50)
		for i := start; i < start+50 && i < total; i++ {
			payload := []byte("small")
			if i%37 == 5 {
				payload = bytes.Repeat([]byte{byte(i)}, 9000)
			}
			batch = append(batch, &types.Event{MessageId: fmt.Sprintf("m-%d", i), Topic: "orders", Payload: payload, ScheduleTs: int64(i)})
		}
		batches = append(batches, batch)
	}
	return batches
}

// A range read begins at any offset of a log, in a segment being written and
// in sealed ones, encrypted or not, with records of mixed sizes, across
// segments and across reopening, whether or not it is bounded by bytes. It
// passes over the records between the nearest index entry and its first one
// by their lengths; this checks it begins with the right one.
func TestWAL_RangeReadsBeginAtAnyOffset(t *testing.T) {
	key := bytes.Repeat([]byte{9}, 32)
	for _, encrypted := range []bool{false, true} {
		t.Run(fmt.Sprintf("encrypted=%v", encrypted), func(t *testing.T) {
			var cipher *SegmentCipher
			if encrypted {
				var err error
				if cipher, err = NewSegmentCipher(key, 0); err != nil {
					t.Fatal(err)
				}
			}
			dir := t.TempDir()
			open := func() *WAL {
				w, err := NewWAL(dir, 0, &WALConfig{SegmentSizeBytes: 256 << 10, IndexInterval: 100, FsyncMode: "batch"}, cipher)
				if err != nil {
					t.Fatal(err)
				}
				return w
			}
			w := open()
			const total = 2500
			for _, batch := range rangeReadEvents(total) {
				if err := w.AppendBatch(batch); err != nil {
					t.Fatal(err)
				}
			}
			check := func(w *WAL) {
				t.Helper()
				rng := rand.New(rand.NewSource(7))
				read := func(first, last, maxBytes int64) {
					t.Helper()
					events, err := w.ReadEventsWithin(first, last, maxBytes)
					if err != nil {
						t.Fatalf("read %d-%d within %d bytes: %v", first, last, maxBytes, err)
					}
					wantLast := min(last, total-1)
					if len(events) == 0 || (maxBytes == 0 && int64(len(events)) != wantLast-first+1) {
						t.Fatalf("read %d-%d within %d bytes: %d events", first, last, maxBytes, len(events))
					}
					for i, event := range events {
						offset := first + int64(i)
						if want := fmt.Sprintf("m-%d", offset); event.Offset != offset || event.MessageId != want {
							t.Fatalf("read %d-%d: event %d has offset %d, id %s; want %s", first, last, i, event.Offset, event.MessageId, want)
						}
					}
				}
				for offset := int64(0); offset < total; offset++ {
					read(offset, offset, 0) // what a reader following the log asks for
				}
				for i := 0; i < 300; i++ {
					first := rng.Int63n(total)
					last := first + rng.Int63n(400)
					read(first, last, 0)
					read(first, last, 1+rng.Int63n(64<<10))
				}
				if events, err := w.ReadEventsWithin(total, total+10, 0); err != nil || len(events) != 0 {
					t.Fatalf("read beyond the end of the log: %d events, %v", len(events), err)
				}
			}
			check(w)
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			reopened := open()
			defer reopened.Close()
			check(reopened)
		})
	}
}

// Counting records from an index entry finds the wrong record when the entry
// names the wrong offset for its position. The read then begins with a record
// that is not the one asked for, and is done again from the entry, reading
// each record's offset as it used to.
func TestSegment_RangeReadFallsBackWhenCountingRecordsMisses(t *testing.T) {
	cipher, err := NewSegmentCipher(bytes.Repeat([]byte{9}, 32), 0)
	if err != nil {
		t.Fatal(err)
	}
	// One index entry, for the first record.
	w, err := NewWAL(t.TempDir(), 0, &WALConfig{SegmentSizeBytes: 8 << 20, IndexInterval: 1 << 30, FsyncMode: "batch"}, cipher)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	const total = 400
	events := make([]*types.Event, 0, total)
	for i := 0; i < total; i++ {
		events = append(events, &types.Event{MessageId: fmt.Sprintf("m-%d", i), Topic: "orders", Payload: []byte("small"), ScheduleTs: int64(i)})
	}
	if err := w.AppendBatch(events); err != nil {
		t.Fatal(err)
	}
	segment := w.GetActiveSegment()
	if got := segment.index.Count(); got != 1 {
		t.Fatalf("%d index entries, want the first record's alone", got)
	}

	// An entry at record 200's position that says it is record 198.
	segment.mu.RLock()
	pos, ok := segment.skipRecords(64, 200)
	segment.mu.RUnlock()
	if !ok {
		t.Fatal("no position for record 200")
	}
	if err := segment.index.AddEntry(198, 198, pos); err != nil {
		t.Fatal(err)
	}

	// From record 200 on, the entry is the nearest one before the range, and
	// counting from it lands two records too far.
	for _, first := range []int64{200, 201, 205, 333, 399} {
		got, err := w.ReadEventsWithin(first, first+3, 0)
		if err != nil {
			t.Fatalf("read from %d: %v", first, err)
		}
		if len(got) != int(min(first+3, total-1)-first+1) {
			t.Fatalf("read from %d: %d events", first, len(got))
		}
		for i, event := range got {
			if event.Offset != first+int64(i) || event.MessageId != fmt.Sprintf("m-%d", event.Offset) {
				t.Fatalf("read from %d: event %d has offset %d, id %s", first, i, event.Offset, event.MessageId)
			}
		}
	}
}

func checkIndexSpacing(t *testing.T, w *WAL, wantDense bool) {
	t.Helper()
	entries := 0
	for _, segment := range w.GetSegments() {
		list := segment.index.GetEntries()
		entries += len(list)
		previous := int64(64)
		for _, entry := range list {
			// The next entry is for the first record that begins a span or
			// more after the last one indexed, so a gap is under a span and
			// one record.
			if gap := entry.FilePosition - previous; gap > indexSpanBytes+16<<10 {
				t.Fatalf("segment %s: %d bytes between index entries, at offset %d", segment.GetFilename(), gap, entry.Offset)
			}
			previous = entry.FilePosition
		}
		if tail := segment.sizeBytes - previous; tail > indexSpanBytes+16<<10 {
			t.Fatalf("segment %s: %d bytes after its last index entry", segment.GetFilename(), tail)
		}
	}
	if wantDense && entries < 10 {
		t.Fatalf("%d index entries for megabytes of records", entries)
	}
}

// With events of a few kilobytes, an index entry every thousand events leaves
// megabytes between entries, which a read has to pass over. The index gets an
// entry at least every indexSpanBytes, and so does one rebuilt from its
// segment; small events are indexed as before.
func TestWAL_IndexEntriesAreNoMoreThanASpanApart(t *testing.T) {
	config := &WALConfig{SegmentSizeBytes: 4 << 20, IndexInterval: 1000, FsyncMode: "batch"}
	dir := t.TempDir()
	w, err := NewWAL(dir, 0, config, nil)
	if err != nil {
		t.Fatal(err)
	}
	const total = 900
	for start := 0; start < total; start += 30 {
		batch := make([]*types.Event, 0, 30)
		for i := start; i < start+30; i++ {
			batch = append(batch, &types.Event{MessageId: fmt.Sprintf("m-%d", i), Topic: "orders", Payload: bytes.Repeat([]byte{byte(i)}, 8<<10), ScheduleTs: int64(i)})
		}
		if err := w.AppendBatch(batch); err != nil {
			t.Fatal(err)
		}
	}
	checkIndexSpacing(t, w, true)
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Without its index files, a log rebuilds them from its segments.
	removed := 0
	err = filepath.WalkDir(dir, func(path string, entry fs.DirEntry, err error) error {
		if err == nil && !entry.IsDir() && strings.HasSuffix(path, ".index") {
			removed++
			return os.Remove(path)
		}
		return err
	})
	if err != nil || removed == 0 {
		t.Fatalf("removed %d index files: %v", removed, err)
	}
	reopened, err := NewWAL(dir, 0, config, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	checkIndexSpacing(t, reopened, true)
	for offset := int64(0); offset < total; offset += 7 {
		event, err := reopened.ReadEvent(offset)
		if err != nil || event.MessageId != fmt.Sprintf("m-%d", offset) {
			t.Fatalf("read offset %d with the rebuilt index: %v", offset, err)
		}
	}

	// A thousand small events are well under a span: one entry, as before.
	small, err := NewWAL(t.TempDir(), 0, config, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer small.Close()
	batch := make([]*types.Event, 0, 900)
	for i := 0; i < 900; i++ {
		batch = append(batch, &types.Event{MessageId: fmt.Sprintf("s-%d", i), Topic: "orders", Payload: []byte("small"), ScheduleTs: int64(i)})
	}
	if err := small.AppendBatch(batch); err != nil {
		t.Fatal(err)
	}
	if got := small.GetActiveSegment().index.Count(); got != 1 {
		t.Fatalf("%d index entries for 900 small events, want 1", got)
	}
}

// benchReadWAL is an encrypted log of 999 events of about a kilobyte in one
// segment, with an index entry every thousand events: the shape the soak's
// reads met.
func benchReadWAL(b *testing.B) *WAL {
	b.Helper()
	cipher, err := NewSegmentCipher(bytes.Repeat([]byte{9}, 32), 0)
	if err != nil {
		b.Fatal(err)
	}
	w, err := NewWAL(b.TempDir(), 0, &WALConfig{SegmentSizeBytes: 64 << 20, IndexInterval: 1000, FsyncMode: "batch"}, cipher)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { w.Close() })
	for start := 0; start < 999; start += 111 {
		batch := make([]*types.Event, 0, 111)
		for i := start; i < start+111; i++ {
			batch = append(batch, &types.Event{MessageId: fmt.Sprintf("m-%d", i), Topic: "orders", Payload: bytes.Repeat([]byte{byte(i)}, 1500), ScheduleTs: int64(i)})
		}
		if err := w.AppendBatch(batch); err != nil {
			b.Fatal(err)
		}
	}
	return w
}

// What a reader following the end of the log does: read the newest event.
func BenchmarkWAL_ReadNewestEvent(b *testing.B) {
	w := benchReadWAL(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		events, err := w.ReadEventsWithin(998, 998, 4<<20)
		if err != nil || len(events) != 1 {
			b.Fatalf("%d events, %v", len(events), err)
		}
	}
}

// What bringing events back from the cold store does: read one event anywhere.
func BenchmarkWAL_ReadOneEventAnywhere(b *testing.B) {
	w := benchReadWAL(b)
	rng := rand.New(rand.NewSource(3))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := w.ReadEvent(rng.Int63n(999)); err != nil {
			b.Fatal(err)
		}
	}
}

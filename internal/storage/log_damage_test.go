package storage

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

// A zeroed record can be larger than the lookahead used to distinguish a
// preallocated tail. A later indexed, checksummed record proves that the log
// did not end at the zeroes, even when this is the active segment.
func TestWAL_ZeroedRecordBeforeIndexedRecord(t *testing.T) {
	for _, laterSegment := range []bool{false, true} {
		for _, encrypted := range []bool{false, true} {
			t.Run(fmt.Sprintf("laterSegment=%t/encrypted=%t", laterSegment, encrypted), func(t *testing.T) {
				dir := t.TempDir()
				segmentSize := int64(1 << 20)
				if laterSegment {
					segmentSize = 256 << 10
				}
				cfg := &WALConfig{SegmentSizeBytes: segmentSize, IndexInterval: 1, FsyncMode: "batch"}
				var cipher *SegmentCipher
				if encrypted {
					var err error
					cipher, err = NewSegmentCipher(bytes.Repeat([]byte{7}, 32), 0)
					if err != nil {
						t.Fatal(err)
					}
				}
				w, err := NewWAL(dir, 0, cfg, cipher)
				if err != nil {
					t.Fatal(err)
				}
				for i := 0; i < 3; i++ {
					if err := w.AppendEvent(&types.Event{MessageId: fmt.Sprint(i), Topic: "orders", Payload: bytes.Repeat([]byte{1}, 100<<10), ScheduleTs: 1}); err != nil {
						t.Fatal(err)
					}
				}
				segment := w.GetSegments()[0]
				entries := segment.index.GetEntries()
				path := filepath.Join(dir, "segments", segment.GetFilename())
				if len(entries) < 3 {
					t.Fatalf("expected three index entries, got %d", len(entries))
				}
				if laterSegment {
					if err := w.AppendEvent(&types.Event{MessageId: "later", Topic: "orders", Payload: []byte("later"), ScheduleTs: 1}); err != nil {
						t.Fatal(err)
					}
				}
				if err := w.Close(); err != nil {
					t.Fatal(err)
				}
				overwrite(t, path, entries[1].FilePosition, make([]byte, entries[2].FilePosition-entries[1].FilePosition))
				// Both repeated starts and the offline checker must preserve evidence.
				for i := 0; i < 2; i++ {
					opened, err := NewWAL(dir, 0, cfg, cipher)
					if err == nil {
						opened.Close()
						t.Fatal("opened a log with a zeroed record before a valid record")
					}
					if !errors.Is(err, ErrLogDamaged) {
						t.Fatalf("got %v, want ErrLogDamaged", err)
					}
				}
				check, err := CheckLog(dir, cipher, false)
				if err != nil || len(check.Damage) != 1 || check.Damage[0].EndOfLog {
					t.Fatalf("check: %+v, %v", check, err)
				}
				if !laterSegment && check.Damage[0].NextOffset != -1 {
					t.Fatalf("last segment must report an unknown loss endpoint: %+v", check.Damage[0])
				}
				repaired, err := CheckLog(dir, cipher, true)
				if err != nil || len(repaired.Damage) != 1 || !repaired.Damage[0].Repaired {
					t.Fatalf("repair: %+v, %v", repaired, err)
				}
				opened, err := NewWAL(dir, 0, cfg, cipher)
				if err != nil {
					t.Fatalf("open after explicit repair: %v", err)
				}
				defer opened.Close()
				want := "[0]"
				if laterSegment {
					want = "[0 3]"
				}
				if got := fmt.Sprint(offsetsOf(t, opened)); got != want {
					t.Fatalf("repaired offsets %s, want %s", got, want)
				}
			})
		}
	}
}

func TestIndexedRecordAfterIgnoresStaleIndexEntry(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWAL(dir, 0, &WALConfig{SegmentSizeBytes: 1 << 20, IndexInterval: 1, FsyncMode: "batch"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	for i := 0; i < 3; i++ {
		if err := w.AppendEvent(&types.Event{MessageId: fmt.Sprint(i), Topic: "orders", Payload: []byte("payload"), ScheduleTs: 1}); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	segment := w.GetSegments()[0]
	entries := segment.index.GetEntries()
	if len(entries) < 3 {
		t.Fatalf("expected three index entries, got %d", len(entries))
	}
	// A stale entry points at a valid record, but claims a different offset.
	if err := segment.index.AddEntry(1, 99, entries[2].FilePosition); err != nil {
		t.Fatal(err)
	}
	info, err := segment.segmentFile.Stat()
	if err != nil {
		t.Fatal(err)
	}
	offset, err := segment.indexedRecordAfter(entries[1].FilePosition, info.Size())
	if err != nil || offset != -1 {
		t.Fatalf("stale index treated as evidence: offset %d, error %v", offset, err)
	}
}

func TestWAL_ZeroedClosedSegmentIsDetectedWithoutIndex(t *testing.T) {
	for _, followingRecords := range []bool{false, true} {
		for _, encrypted := range []bool{false, true} {
			t.Run(fmt.Sprintf("following=%t/encrypted=%t", followingRecords, encrypted), func(t *testing.T) {
				dir := t.TempDir()
				count := 2
				if followingRecords {
					count = 3
				}
				var cipher *SegmentCipher
				if encrypted {
					var err error
					cipher, err = NewSegmentCipher(bytes.Repeat([]byte{3}, 32), 0)
					if err != nil {
						t.Fatal(err)
					}
				}
				original, err := NewWAL(dir, 0, damageConfig, cipher)
				if err != nil {
					t.Fatal(err)
				}
				for i := 0; i < count; i++ {
					if err := original.AppendEvent(&types.Event{MessageId: fmt.Sprint(i), Topic: "orders", Payload: make([]byte, 2048), ScheduleTs: 1}); err != nil {
						t.Fatal(err)
					}
				}
				var paths []string
				for _, segment := range original.GetSegments() {
					paths = append(paths, filepath.Join(dir, "segments", segment.GetFilename()))
				}
				if err := original.Close(); err != nil {
					t.Fatal(err)
				}
				victim := paths[1]
				info, err := os.Stat(victim)
				if err != nil {
					t.Fatal(err)
				}
				overwrite(t, victim, 64, make([]byte, info.Size()-64))
				if err := os.Remove(filepath.Join(dir, "index", strings.TrimSuffix(filepath.Base(victim), ".log")+".index")); err != nil {
					t.Fatal(err)
				}
				for attempt := 0; attempt < 2; attempt++ {
					w, err := NewWAL(dir, 0, damageConfig, cipher)
					if err == nil {
						w.Close()
						t.Fatal("opened a zeroed closed segment")
					}
					if !errors.Is(err, ErrLogDamaged) {
						t.Fatalf("got %v, want ErrLogDamaged", err)
					}
				}
				check, err := CheckLog(dir, cipher, false)
				if err != nil || len(check.Damage) != 1 || check.Damage[0].EndOfLog {
					t.Fatalf("check: %+v, %v", check, err)
				}
				repaired, err := CheckLog(dir, cipher, true)
				if err != nil || len(repaired.Damage) != 1 || !repaired.Damage[0].Repaired {
					t.Fatalf("repair: %+v, %v", repaired, err)
				}
				w, err := NewWAL(dir, 0, damageConfig, cipher)
				if err != nil {
					t.Fatalf("open repaired log: %v", err)
				}
				defer w.Close()
			})
		}
	}
}

func TestWAL_InterruptedClosedSegmentTruncationFailsClosed(t *testing.T) {
	dir := t.TempDir()
	cfg := &WALConfig{SegmentSizeBytes: 1024, IndexInterval: 1, FsyncMode: "batch"}
	w, err := NewWAL(dir, 0, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		if err := w.AppendEvent(&types.Event{MessageId: fmt.Sprint(i), Topic: "orders", Payload: bytes.Repeat([]byte{1}, 320), ScheduleTs: 1}); err != nil {
			t.Fatal(err)
		}
	}
	first := w.GetSegments()[0]
	if first.GetLastOffset() < 1 || len(w.GetSegments()) < 2 {
		t.Fatal("test needs a closed segment with multiple records")
	}
	// Model a crash after shortening the predecessor but before deleting its
	// successor, whose durable header still records the original boundary.
	if _, err := first.TruncateAfterOffset(0); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	opened, err := NewWAL(dir, 0, cfg, nil)
	if err == nil {
		opened.Close()
		t.Fatal("opened an interrupted truncation without detecting the boundary mismatch")
	}
	if !errors.Is(err, ErrLogDamaged) {
		t.Fatalf("got %v, want ErrLogDamaged", err)
	}
}

func TestCheckLogRefusesToRepairRecordsBeyondBoundary(t *testing.T) {
	dir := t.TempDir()
	cfg := &WALConfig{SegmentSizeBytes: 1024, IndexInterval: 1, FsyncMode: "batch"}
	w, err := NewWAL(dir, 0, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		if err := w.AppendEvent(&types.Event{MessageId: fmt.Sprint(i), Topic: "orders", Payload: make([]byte, 320), ScheduleTs: 1}); err != nil {
			t.Fatal(err)
		}
	}
	segments := w.GetSegments()
	entries := segments[0].index.GetEntries()
	if len(segments) < 2 || len(entries) < 2 {
		t.Fatal("test needs a closed segment with multiple records")
	}
	if err := segments[1].setPredecessor(&segmentBoundary{first: 0, last: 0, size: entries[1].FilePosition}); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "segments", segments[0].GetFilename())
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := CheckLog(dir, nil, true); err == nil || !strings.Contains(err.Error(), "beyond its recorded boundary") {
		t.Fatalf("repair should refuse an overlong segment: %v", err)
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatalf("failed repair changed the segment: %v", err)
	}
}

func TestCheckpointRetainsClosedSegmentBoundary(t *testing.T) {
	dir, dest := t.TempDir(), t.TempDir()
	w := oneEventSegments(t, dir, 3)
	if _, _, err := w.Checkpoint(dest); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dest, "segments", fmt.Sprintf("%020d.log", 1))
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	overwrite(t, path, 64, make([]byte, info.Size()-64))
	if err := os.Remove(filepath.Join(dest, "index", fmt.Sprintf("%020d.index", 1))); err != nil {
		t.Fatal(err)
	}
	opened, err := NewWAL(dest, 0, damageConfig, nil)
	if err == nil {
		opened.Close()
		t.Fatal("checkpoint opened after a closed segment was zeroed")
	}
	if !errors.Is(err, ErrLogDamaged) {
		t.Fatalf("got %v, want ErrLogDamaged", err)
	}
}

func TestIncrementalBackupRetainsBoundaryAndReplacesPlaceholder(t *testing.T) {
	dir, dest := t.TempDir(), t.TempDir()
	w := oneEventSegments(t, dir, 2)
	defer w.Close()
	if err := BackupWAL(dir, dest); err != nil {
		t.Fatal(err)
	}
	placeholder := filepath.Join(dest, "segments", fmt.Sprintf("%020d.log", 2))
	if info, err := os.Stat(placeholder); err != nil || info.Size() != 64 {
		t.Fatalf("boundary placeholder: %v, %v", info, err)
	}
	if err := w.AppendEvent(&types.Event{MessageId: "third", Topic: "orders", Payload: make([]byte, 2048), ScheduleTs: 1}); err != nil {
		t.Fatal(err)
	}
	if err := BackupWAL(dir, dest); err != nil {
		t.Fatal(err)
	}
	if info, err := os.Stat(placeholder); err != nil || info.Size() <= 64 {
		t.Fatalf("closed segment did not replace placeholder: %v, %v", info, err)
	}
	backup, err := NewWAL(dest, 0, damageConfig, nil)
	if err != nil {
		t.Fatalf("open incremental backup: %v", err)
	}
	if got := fmt.Sprint(offsetsOf(t, backup)); got != "[0 1 2]" {
		t.Fatalf("backup offsets %s", got)
	}
	if err := backup.Close(); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(placeholder)
	if err != nil {
		t.Fatal(err)
	}
	overwrite(t, placeholder, 64, make([]byte, info.Size()-64))
	if err := os.Remove(filepath.Join(dest, "index", fmt.Sprintf("%020d.index", 2))); err != nil {
		t.Fatal(err)
	}
	backup, err = NewWAL(dest, 0, damageConfig, nil)
	if err == nil {
		backup.Close()
		t.Fatal("incremental backup opened after its last closed segment was zeroed")
	}
	if !errors.Is(err, ErrLogDamaged) {
		t.Fatalf("got %v, want ErrLogDamaged", err)
	}
}

var damageConfig = &WALConfig{SegmentSizeBytes: 1024, IndexInterval: 1, FsyncMode: "batch", FlushIntervalMS: 10}

// closedLog writes count events, one per segment, and closes the log. It
// returns the paths of the segment files in offset order; the last is the
// empty segment the log would append to next.
func closedLog(t *testing.T, dir string, count int) []string {
	t.Helper()
	w := oneEventSegments(t, dir, count)
	paths := make([]string, 0, count+1)
	for _, segment := range w.GetSegments() {
		paths = append(paths, filepath.Join(dir, "segments", segment.GetFilename()))
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return paths
}

// overwrite puts data into a file at a position.
func overwrite(t *testing.T, path string, position int64, data []byte) {
	t.Helper()
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if _, err := file.WriteAt(data, position); err != nil {
		t.Fatal(err)
	}
}

// Bytes that are not a valid record, inside a segment that later segments
// follow, are damage: valid records were there, and what was written after
// them is in the later segments. The log used to take them for the end of
// the segment and open with the events behind them missing. It refuses to
// open now, and says what cannot be read and what to do.
func TestWAL_DamageInsideTheLogIsNotTakenForItsEnd(t *testing.T) {
	dir := t.TempDir()
	paths := closedLog(t, dir, 4)
	// Segment 1 holds the event at offset 1. Its record starts after the
	// 64-byte header; a flipped byte in the middle fails the checksum.
	overwrite(t, paths[1], 64+200, []byte{0xFF, 0xFE, 0xFD})

	_, err := NewWAL(dir, 0, damageConfig, nil)
	if err == nil {
		t.Fatal("a log with a damaged segment in the middle opened")
	}
	if !errors.Is(err, ErrLogDamaged) {
		t.Fatalf("opening the damaged log failed with %v, want an error that says the log is damaged", err)
	}
	for _, want := range []string{filepath.Base(paths[1]), "offset 2", "check-log"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the error does not mention %q: %v", want, err)
		}
	}
	// Nothing was changed: the damaged file is as it was found.
	data, readErr := os.ReadFile(paths[1])
	if readErr != nil || data[64+200] != 0xFF {
		t.Fatalf("opening the damaged log changed the damaged file (%v)", readErr)
	}
}

// At the end of the log the same bytes are what a crash leaves behind: a
// write that did not finish. The log ends before them. They are cleared, so
// that they are not found again, in a closed segment, after later writes went
// over their start but not their end.
func TestWAL_AnInterruptedWriteAtTheEndIsCutOff(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWAL(dir, 0, &WALConfig{SegmentSizeBytes: 1 << 20, IndexInterval: 1, FsyncMode: "batch", FlushIntervalMS: 10}, nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := w.AppendEvent(&types.Event{MessageId: fmt.Sprintf("event-%d", i), Topic: "orders", Payload: make([]byte, 100), ScheduleTs: 1}); err != nil {
			t.Fatal(err)
		}
	}
	segment := w.GetSegments()[0]
	path, end := filepath.Join(dir, "segments", segment.GetFilename()), segment.GetSize()
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	// A record that claims 300 bytes and holds noise, as far as it got.
	torn := make([]byte, 180)
	torn[2], torn[3] = 0x01, 0x2C
	for i := 4; i < len(torn); i++ {
		torn[i] = byte(i)
	}
	overwrite(t, path, end, torn)

	reopened, err := NewWAL(dir, 0, &WALConfig{SegmentSizeBytes: 1 << 20, IndexInterval: 1, FsyncMode: "batch", FlushIntervalMS: 10}, nil)
	if err != nil {
		t.Fatalf("a log with an interrupted write at its end did not open: %v", err)
	}
	defer reopened.Close()
	if got, want := fmt.Sprint(offsetsOf(t, reopened)), "[0 1 2]"; got != want {
		t.Fatalf("log holds offsets %s, want %s", got, want)
	}
	// A short record goes over the start of what was cleared. Nothing of the
	// interrupted write is left behind it.
	if err := reopened.AppendEvent(&types.Event{MessageId: "after", Topic: "orders", Payload: []byte("x"), ScheduleTs: 1}); err != nil {
		t.Fatal(err)
	}
	if err := reopened.Flush(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for position := reopened.GetSegments()[0].GetSize(); position < int64(len(data)); position++ {
		if data[position] != 0 {
			t.Fatalf("byte %d after the last record is %#x: part of the interrupted write is still in the file", position, data[position])
		}
	}
}

// A version-2 successor must carry its predecessor's boundary, even when it is
// empty. Missing evidence is not interpreted as a safe interrupted write.
func TestWAL_MissingSuccessorBoundaryIsRejected(t *testing.T) {
	dir := t.TempDir()
	paths := closedLog(t, dir, 3)
	successor, err := OpenSegment(dir, filepath.Base(paths[3]), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := successor.setPredecessor(nil); err != nil {
		t.Fatal(err)
	}
	if err := successor.Close(); err != nil {
		t.Fatal(err)
	}
	w, err := NewWAL(dir, 0, damageConfig, nil)
	if err == nil {
		w.Close()
		t.Fatal("opened a log without its successor boundary")
	}
	if !errors.Is(err, ErrLogDamaged) {
		t.Fatalf("got %v, want ErrLogDamaged", err)
	}
	if _, err := CheckLog(dir, nil, false); !errors.Is(err, ErrLogDamaged) {
		t.Fatalf("check-log: got %v, want ErrLogDamaged", err)
	}
}

// The offline check finds the same damage without opening the log for use,
// and its repair cuts the damaged segment after its last valid record. The
// log then opens with a hole where the unreadable events were.
func TestCheckLog_ReportsDamageAndCutsItOut(t *testing.T) {
	dir := t.TempDir()
	paths := closedLog(t, dir, 4)
	overwrite(t, paths[1], 64+200, []byte{0xFF, 0xFE, 0xFD})

	check, err := CheckLog(dir, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(check.Damage) != 1 {
		t.Fatalf("found %d damaged places, want 1: %+v", len(check.Damage), check.Damage)
	}
	damage := check.Damage[0]
	if damage.Segment != filepath.Base(paths[1]) || damage.EndOfLog || damage.Repaired || damage.AfterOffset != 0 || damage.NextOffset != 2 {
		t.Fatalf("damage reported as %+v, want segment %s, after offset 0, next offset 2", damage, filepath.Base(paths[1]))
	}
	if check.Events != 3 || check.FirstOffset != 0 || check.LastOffset != 3 {
		t.Fatalf("log reported as %d events at offsets %d to %d, want 3 at 0 to 3", check.Events, check.FirstOffset, check.LastOffset)
	}
	if _, err := NewWAL(dir, 0, damageConfig, nil); !errors.Is(err, ErrLogDamaged) {
		t.Fatalf("checking without repair changed what opening the log does: %v", err)
	}

	repaired, err := CheckLog(dir, nil, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(repaired.Damage) != 1 || !repaired.Damage[0].Repaired {
		t.Fatalf("repair reported %+v", repaired.Damage)
	}
	w, err := NewWAL(dir, 0, damageConfig, nil)
	if err != nil {
		t.Fatalf("the repaired log did not open: %v", err)
	}
	defer w.Close()
	if got, want := fmt.Sprint(offsetsOf(t, w)), "[0 2 3]"; got != want {
		t.Fatalf("the repaired log holds offsets %s, want %s", got, want)
	}
	if again, err := CheckLog(dir, nil, false); err != nil || len(again.Damage) != 0 {
		t.Fatalf("a second check of the repaired log: %+v %v", again, err)
	}
}

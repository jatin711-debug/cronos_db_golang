package storage

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

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

// A crash just after a rotation can lose the last write to the old segment
// while the new, empty one already exists and begins after it. The log then
// ends where the old segment ends, and continues there.
func TestWAL_EmptySegmentAfterALostWriteIsRemoved(t *testing.T) {
	dir := t.TempDir()
	paths := closedLog(t, dir, 3)
	// The record of offset 2 is lost: its segment holds noise instead. The
	// segment after it is the empty one that begins at offset 3.
	info, err := os.Stat(paths[2])
	if err != nil {
		t.Fatal(err)
	}
	noise := make([]byte, info.Size()-64)
	for i := range noise {
		noise[i] = byte(i%251) + 1
	}
	overwrite(t, paths[2], 64, noise)

	w, err := NewWAL(dir, 0, damageConfig, nil)
	if err != nil {
		t.Fatalf("the log did not open: %v", err)
	}
	defer w.Close()
	if got, want := fmt.Sprint(offsetsOf(t, w)), "[0 1]"; got != want {
		t.Fatalf("log holds offsets %s, want %s", got, want)
	}
	if next := w.GetNextOffset(); next != 2 {
		t.Fatalf("the log continues at offset %d, want 2", next)
	}
	if err := w.AppendEvent(&types.Event{MessageId: "again", Topic: "orders", Payload: []byte("x"), ScheduleTs: 1}); err != nil {
		t.Fatal(err)
	}
	if got, want := fmt.Sprint(offsetsOf(t, w)), "[0 1 2]"; got != want {
		t.Fatalf("after the next append the log holds offsets %s, want %s", got, want)
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

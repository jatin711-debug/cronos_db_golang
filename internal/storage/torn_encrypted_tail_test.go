package storage

import (
	"bytes"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

// writeFourRecords writes four 600-byte events and returns the segment path
// and the byte range of the fourth record.
func writeFourRecords(t *testing.T, dir string, cipher *SegmentCipher) (path string, start, end int64) {
	t.Helper()
	cfg := &WALConfig{SegmentSizeBytes: 1 << 20, IndexInterval: 1000, FsyncMode: "every_event"}
	w, err := NewWAL(dir, 0, cfg, cipher)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		if i == 3 {
			start = w.GetActiveSegment().GetSize()
		}
		if err := w.AppendEvent(&types.Event{MessageId: fmt.Sprint(i), Topic: "t", Payload: bytes.Repeat([]byte{1}, 600), ScheduleTs: 1}); err != nil {
			t.Fatal(err)
		}
	}
	end = w.GetActiveSegment().GetSize()
	path = filepath.Join(dir, "segments", w.GetActiveSegment().GetFilename())
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return path, start, end
}

func testCipher(t *testing.T, fill byte) *SegmentCipher {
	t.Helper()
	cipher, err := NewSegmentCipher(bytes.Repeat([]byte{fill}, 32), 0)
	if err != nil {
		t.Fatal(err)
	}
	return cipher
}

// A crash during a write leaves the final record with its length on disk and
// only part of the rest. A plaintext log cut it off and opened; an encrypted
// one refused to open, and check-log --repair failed the same way.
func TestTornFinalRecordIsCut(t *testing.T) {
	for _, encrypted := range []bool{false, true} {
		t.Run(fmt.Sprintf("encrypted=%t", encrypted), func(t *testing.T) {
			dir := t.TempDir()
			var cipher *SegmentCipher
			if encrypted {
				cipher = testCipher(t, 7)
			}
			path, start, end := writeFourRecords(t, dir, cipher)
			// Only the first 100 bytes of the fourth record reached the disk.
			overwrite(t, path, start+100, make([]byte, end-start-100))

			cfg := &WALConfig{SegmentSizeBytes: 1 << 20, IndexInterval: 1000, FsyncMode: "every_event"}
			reopened, err := NewWAL(dir, 0, cfg, cipher)
			if err != nil {
				t.Fatalf("a log with a torn final record did not open: %v", err)
			}
			defer reopened.Close()
			if got := fmt.Sprint(offsetsOf(t, reopened)); got != "[0 1 2]" {
				t.Fatalf("offsets %s, want [0 1 2]", got)
			}
		})
	}
}

func TestCheckLogRepairsTornEncryptedTail(t *testing.T) {
	dir := t.TempDir()
	cipher := testCipher(t, 7)
	path, start, end := writeFourRecords(t, dir, cipher)
	overwrite(t, path, start+100, make([]byte, end-start-100))
	if _, err := CheckLog(dir, cipher, true); err != nil {
		t.Fatalf("check-log --repair cannot handle a torn encrypted tail: %v", err)
	}
}

// A final record that was written completely and then changed is damage, not
// an interrupted write, and the log still refuses to open.
func TestCorruptFinalEncryptedRecordIsNotCut(t *testing.T) {
	dir := t.TempDir()
	cipher := testCipher(t, 7)
	path, start, end := writeFourRecords(t, dir, cipher)
	overwrite(t, path, start+200, []byte{0xFF, 0x00, 0xFF})
	overwrite(t, path, end-1, []byte{0x5A}) // a written tag, whatever byte it ended in
	cfg := &WALConfig{SegmentSizeBytes: 1 << 20, IndexInterval: 1000, FsyncMode: "every_event"}
	w, err := NewWAL(dir, 0, cfg, cipher)
	if err == nil {
		w.Close()
		t.Fatal("a log whose complete final record does not decrypt was opened")
	}
	if !strings.Contains(err.Error(), "wrong key or corrupt data") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// The only record of a segment can be torn too. A wrong key fails that record
// as well, so it is taken for torn only when its whole authentication tag is
// still unwritten.
func TestTornOnlyEncryptedRecordIsCut(t *testing.T) {
	dir := t.TempDir()
	cipher := testCipher(t, 7)
	cfg := &WALConfig{SegmentSizeBytes: 1 << 20, IndexInterval: 1000, FsyncMode: "every_event"}
	w, err := NewWAL(dir, 0, cfg, cipher)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.AppendEvent(&types.Event{MessageId: "only", Topic: "t", Payload: bytes.Repeat([]byte{1}, 600), ScheduleTs: 1}); err != nil {
		t.Fatal(err)
	}
	end := w.GetActiveSegment().GetSize()
	path := filepath.Join(dir, "segments", w.GetActiveSegment().GetFilename())
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	overwrite(t, path, 64+100, make([]byte, end-64-100))
	reopened, err := NewWAL(dir, 0, cfg, cipher)
	if err != nil {
		t.Fatalf("a log whose only record is torn did not open: %v", err)
	}
	defer reopened.Close()
	if last := reopened.GetLastOffset(); last >= 0 {
		t.Fatalf("the torn record is still in the log: last offset %d", last)
	}
}

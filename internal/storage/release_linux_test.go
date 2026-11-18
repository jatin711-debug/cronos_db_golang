//go:build linux

package storage

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

// reserveBeyondEnd reserves n bytes of blocks past the end of path, as XFS
// does when a write extends a file, without changing its length.
func reserveBeyondEnd(t *testing.T, path string, n int64) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if err := unix.Fallocate(int(f.Fd()), unix.FALLOC_FL_KEEP_SIZE, info.Size(), n); err != nil {
		if errors.Is(err, unix.EOPNOTSUPP) {
			t.Skipf("this filesystem cannot reserve blocks past a file's end: %v", err)
		}
		t.Fatal(err)
	}
}

// beyondEnd returns how many bytes path has allocated past its length.
func beyondEnd(t *testing.T, path string) int64 {
	t.Helper()
	var st unix.Stat_t
	if err := unix.Stat(path, &st); err != nil {
		t.Fatal(err)
	}
	return st.Blocks*512 - st.Size
}

// reserved is what the tests reserve past a segment's end; slack is the most
// a freed segment may still hold there (block rounding, metadata).
const (
	reserved = 8 << 20
	slack    = 1 << 20
)

// A sealed segment stays open for reads for as long as the log keeps it. In
// the 24-hour soak every one held 64 MiB that XFS had reserved past its end
// when its last batch ran past the preallocated 32 MiB: 25 GiB across three
// nodes, until the disk guard refused every publish. Sealing a segment frees
// what is reserved past its end.
//
// The log is encrypted, as the soak's was: encrypted segments are written
// through a buffer, and their last batch extends the file with plain writes.
func TestSealedSegmentFreesBlocksPastItsEnd(t *testing.T) {
	cipher, err := NewSegmentCipher(bytes.Repeat([]byte{7}, 32), 0)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	w, err := NewWAL(dir, 0, &WALConfig{SegmentSizeBytes: 256 << 10, IndexInterval: 10, FsyncMode: "batch"}, cipher)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	appendNumbered(t, w, 0, 1000)
	first := filepath.Join(dir, "segments", w.GetSegments()[0].GetFilename())
	reserveBeyondEnd(t, first, reserved)
	if got := beyondEnd(t, first); got < reserved {
		t.Fatalf("reserved %d bytes past the end, the file shows %d", reserved, got)
	}

	appendNumbered(t, w, 1000, 12000)
	if len(w.GetSegments()) < 2 {
		t.Fatalf("%d segments; the first should have been sealed", len(w.GetSegments()))
	}
	if got := beyondEnd(t, first); got > slack {
		t.Fatalf("the sealed segment still holds %d bytes past its end", got)
	}
	for _, offset := range []int64{0, 999, 11999} {
		if _, err := w.ReadEvent(offset); err != nil {
			t.Fatalf("read %d: %v", offset, err)
		}
	}
}

// Segments that earlier versions left with blocks past their end get them
// back when the log is opened: a restart alone did not return them.
func TestOpeningASegmentFreesBlocksPastItsEnd(t *testing.T) {
	dir := t.TempDir()
	cfg := &WALConfig{SegmentSizeBytes: 256 << 10, IndexInterval: 10, FsyncMode: "batch"}
	w, err := NewWAL(dir, 0, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	appendNumbered(t, w, 0, 12000)
	var paths []string
	for _, seg := range w.GetSegments() {
		paths = append(paths, filepath.Join(dir, "segments", seg.GetFilename()))
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	for _, p := range paths {
		reserveBeyondEnd(t, p, reserved)
	}

	w, err = NewWAL(dir, 0, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	for _, p := range paths {
		if got := beyondEnd(t, p); got > slack {
			t.Errorf("%s still holds %d bytes past its end after the log was opened", filepath.Base(p), got)
		}
	}
	if ev, err := w.ReadEvent(11999); err != nil || ev.GetMessageId() != "m-11999" {
		t.Fatalf("read 11999: %v, %v", ev, err)
	}
}

package storage

import (
	"fmt"
	"github.com/jatin711-debug/cronos_db_golang/pkg/utils"
	"io"
	"os"
	"path/filepath"
	"runtime"
)

type CheckpointFile struct {
	Path, Filename                string
	FirstOffset, LastOffset, Size int64
	IsIndex                       bool
}

// Checkpoint copies one stable WAL generation, including the active segment,
// into dest. It returns the copied files and the last offset they hold.
//
// Appends are paused only while the end of every file is noted. After that a
// segment file and its index can change in three ways: they grow at the end,
// which leaves the noted prefix as it was; retention deletes them; or a
// replica that discards a divergent suffix cuts them short. checkpointMu keeps
// the last two out until the copy is done, so the bytes are copied while
// appends carry on, however large the partition is.
func (w *WAL) Checkpoint(dest string) ([]CheckpointFile, int64, error) {
	cuts, err := CutCheckpoints([]*WAL{w})
	if err != nil {
		return nil, 0, err
	}
	if w.afterCheckpointCut != nil {
		w.afterCheckpointCut()
	}
	return cuts[0].CopyTo(dest)
}

// CheckpointCut is where every file of one WAL ended at the moment of a cut.
// Until it is copied or released, the WAL's files are neither deleted nor cut
// short, so CopyTo copies exactly the log as it was then.
type CheckpointCut struct {
	w     *WAL
	files []CheckpointFile // without Path
	last  int64
	held  bool
}

// CutCheckpoints cuts several WALs at one instant: the logs copied from the
// returned cuts hold exactly the events appended to each of them before that
// instant. wals must be distinct, and callers that cut overlapping sets must
// pass them in the same order.
//
// Everything is written out one log at a time first, which is where the time
// goes. All logs are then held together only to flush what was appended in
// the meantime and to note where each file ends.
func CutCheckpoints(wals []*WAL) ([]*CheckpointCut, error) {
	cuts := make([]*CheckpointCut, len(wals))
	for i, w := range wals {
		w.checkpointMu.Lock()
		cuts[i] = &CheckpointCut{w: w, held: true}
	}
	release := func() {
		for _, cut := range cuts {
			cut.Release()
		}
	}

	flushed := make([]map[*Segment]int64, len(wals))
	for i, w := range wals {
		w.mu.Lock()
		sizes, err := w.flushSegmentsLocked(nil)
		w.mu.Unlock()
		if err != nil {
			release()
			return nil, err
		}
		flushed[i] = sizes
	}

	for _, w := range wals {
		w.mu.Lock()
	}
	var err error
	for i, w := range wals {
		if _, err = w.flushSegmentsLocked(flushed[i]); err != nil {
			break
		}
		if cuts[i].files, cuts[i].last, err = w.noteFileEndsLocked(); err != nil {
			break
		}
	}
	for i := len(wals) - 1; i >= 0; i-- {
		wals[i].mu.Unlock()
	}
	if err != nil {
		release()
		return nil, err
	}
	return cuts, nil
}

// LastOffset is the last event offset in the cut log, -1 if it is empty.
func (c *CheckpointCut) LastOffset() int64 { return c.last }

// Release lets the WAL delete and truncate files again without copying the
// cut. It is safe to call after CopyTo.
func (c *CheckpointCut) Release() {
	if c.held {
		c.held = false
		c.w.checkpointMu.Unlock()
	}
}

// CopyTo copies the cut log into dest and releases the cut. It returns the
// copied files and the last offset they hold.
func (c *CheckpointCut) CopyTo(dest string) ([]CheckpointFile, int64, error) {
	defer c.Release()
	if !c.held {
		return nil, 0, fmt.Errorf("checkpoint cut was already copied or released")
	}
	for _, name := range []string{"segments", "index"} {
		if err := os.MkdirAll(filepath.Join(dest, name), 0700); err != nil {
			return nil, 0, err
		}
	}
	files := make([]CheckpointFile, 0, len(c.files))
	for _, src := range c.files {
		dir := "segments"
		if src.IsIndex {
			dir = "index"
		}
		path := filepath.Join(dest, dir, src.Filename)
		if err := copyFilePrefix(filepath.Join(c.w.dataDir, dir, src.Filename), path, src.Size); err != nil {
			return nil, 0, fmt.Errorf("copy %s: %w", src.Filename, err)
		}
		src.Path = path
		files = append(files, src)
	}
	for _, dir := range []string{"segments", "index", ""} {
		if err := SyncDirectory(filepath.Join(dest, dir)); err != nil {
			return nil, 0, err
		}
	}
	return files, c.last, nil
}

// flushSegmentsLocked writes out every segment and index that has grown since
// already was recorded (all of them when already is nil) and returns the size
// each segment has now. The caller holds w.mu.
func (w *WAL) flushSegmentsLocked(already map[*Segment]int64) (map[*Segment]int64, error) {
	sizes := make(map[*Segment]int64, len(w.segments))
	for _, seg := range w.segments {
		sizes[seg] = seg.sizeBytes
		if size, ok := already[seg]; ok && size == seg.sizeBytes {
			continue
		}
		if err := seg.Flush(); err != nil {
			return nil, err
		}
		if seg.index != nil {
			if err := seg.index.Flush(); err != nil {
				return nil, err
			}
		}
	}
	return sizes, nil
}

// noteFileEndsLocked records how many bytes of each segment and index file
// belong to the log as it is now. The caller holds w.mu and has flushed.
func (w *WAL) noteFileEndsLocked() ([]CheckpointFile, int64, error) {
	var files []CheckpointFile
	last := int64(-1)
	for _, seg := range w.segments {
		files = append(files, CheckpointFile{Filename: seg.filename, FirstOffset: seg.firstOffset, LastOffset: seg.lastOffset, Size: seg.sizeBytes})
		info, err := os.Stat(filepath.Join(w.dataDir, "index", seg.indexFilename))
		if err == nil {
			files = append(files, CheckpointFile{Filename: seg.indexFilename, FirstOffset: seg.firstOffset, LastOffset: seg.lastOffset, Size: info.Size(), IsIndex: true})
		} else if !os.IsNotExist(err) {
			return nil, 0, err
		}
		// Not seg.lastOffset: the active segment is empty right after a
		// rotation, and the checkpoint still ends where the log does.
		last = logEndOf(seg)
	}
	return files, last, nil
}

// copyFilePrefix copies the first size bytes of src into a new file dst and
// syncs it.
func copyFilePrefix(src, dst string, size int64) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	_, copyErr := io.CopyN(out, in, size)
	if copyErr == nil {
		copyErr = out.Sync()
	}
	if closeErr := out.Close(); copyErr == nil {
		copyErr = closeErr
	}
	return copyErr
}

func SyncDirectory(path string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

// RecoverSnapshot finishes or undoes a snapshot install that was interrupted,
// so that segments and index are always one generation, never a mix.
//
// The journal is written "prepared" before anything moves and "installed" only
// after the new files were loaded and verified. With "prepared" the old
// directories, wherever they got to, are put back and whatever stands in their
// place is discarded. With "installed" the new directories stay and the old
// ones are removed. Every step can be repeated, so a crash during recovery is
// recovered the same way.
func RecoverSnapshot(root string) error {
	journal := filepath.Join(root, "snapshot-install.state")
	state, err := os.ReadFile(journal)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if string(state) != "prepared" && string(state) != "installed" {
		return fmt.Errorf("invalid snapshot install journal")
	}
	for _, dir := range []string{"segments", "index"} {
		current, old := filepath.Join(root, dir), filepath.Join(root, dir+".old")
		if _, err := os.Stat(old); err == nil {
			if string(state) == "prepared" {
				if err := os.RemoveAll(current); err != nil {
					return err
				}
				if err := os.Rename(old, current); err != nil {
					return err
				}
			} else if err := os.RemoveAll(old); err != nil {
				return err
			}
		} else if !os.IsNotExist(err) {
			return err
		}
	}
	if err := SyncDirectory(root); err != nil {
		return err
	}
	if err := os.Remove(journal); err != nil {
		return err
	}
	return SyncDirectory(root)
}

// InstallCheckpoint replaces this WAL's files with the checkpoint in staging,
// which must end at expectedLast. Both old directories are kept until the new
// WAL has been loaded and verified; any failure, or a crash before the journal
// says "installed", leaves the old WAL in place.
func (w *WAL) InstallCheckpoint(staging string, expectedLast int64) (result error) {
	w.checkpointMu.Lock()
	defer w.checkpointMu.Unlock()
	w.mu.Lock()
	defer w.mu.Unlock()
	journal := filepath.Join(w.dataDir, "snapshot-install.state")
	if err := RecoverSnapshot(w.dataDir); err != nil {
		return err
	}
	if err := utils.AtomicWriteFile(journal, []byte("prepared"), 0600); err != nil {
		return err
	}
	closeSegments := func() {
		for _, seg := range w.segments {
			_ = seg.Close()
		}
		w.segments = nil
		w.activeSegment = nil
	}
	closeSegments()
	defer func() {
		if result != nil {
			closeSegments()
			if err := RecoverSnapshot(w.dataDir); err != nil {
				result = fmt.Errorf("%w; rollback failed: %v", result, err)
				return
			}
			if err := w.loadSegments(); err != nil {
				result = fmt.Errorf("%w; reload old WAL: %v", result, err)
				return
			}
			_ = w.openActiveSegment()
		}
		w.appendSeq.Store(w.nextOffset.Load())
	}()
	for _, dir := range []string{"segments", "index"} {
		if err := os.Rename(filepath.Join(w.dataDir, dir), filepath.Join(w.dataDir, dir+".old")); err != nil {
			return err
		}
	}
	for _, dir := range []string{"segments", "index"} {
		if err := os.Rename(filepath.Join(staging, dir), filepath.Join(w.dataDir, dir)); err != nil {
			return err
		}
	}
	w.nextOffset.Store(0)
	if err := w.loadSegments(); err != nil {
		return err
	}
	if w.nextOffset.Load() != expectedLast+1 {
		return fmt.Errorf("snapshot offset mismatch")
	}
	if err := w.openActiveSegment(); err != nil {
		return err
	}
	if err := SyncDirectory(w.dataDir); err != nil {
		return err
	}
	if err := utils.AtomicWriteFile(journal, []byte("installed"), 0600); err != nil {
		return err
	}
	// Publication is complete. Cleanup may be retried at the next startup.
	_ = RecoverSnapshot(w.dataDir)
	return nil
}

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

// Checkpoint copies one stable WAL generation, including the active segment.
// Appends and compaction are paused only during the local copy, not network I/O.
func (w *WAL) Checkpoint(dest string) ([]CheckpointFile, int64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, name := range []string{"segments", "index"} {
		if err := os.MkdirAll(filepath.Join(dest, name), 0700); err != nil {
			return nil, 0, err
		}
	}
	var files []CheckpointFile
	last := int64(-1)
	for _, seg := range w.segments {
		if err := seg.Flush(); err != nil {
			return nil, 0, err
		}
		for _, isIndex := range []bool{false, true} {
			name, dir, size := seg.filename, "segments", seg.sizeBytes
			if isIndex {
				name, dir = seg.indexFilename, "index"
				if seg.index != nil {
					if err := seg.index.Flush(); err != nil {
						return nil, 0, err
					}
				}
				info, err := os.Stat(filepath.Join(w.dataDir, dir, name))
				if os.IsNotExist(err) {
					continue
				}
				if err != nil {
					return nil, 0, err
				}
				size = info.Size()
			}
			src, err := os.Open(filepath.Join(w.dataDir, dir, name))
			if err != nil {
				return nil, 0, err
			}
			path := filepath.Join(dest, dir, name)
			out, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
			if err != nil {
				src.Close()
				return nil, 0, err
			}
			_, copyErr := io.CopyN(out, src, size)
			src.Close()
			if copyErr == nil {
				copyErr = out.Sync()
			}
			closeErr := out.Close()
			if copyErr != nil {
				return nil, 0, copyErr
			}
			if closeErr != nil {
				return nil, 0, closeErr
			}
			files = append(files, CheckpointFile{path, name, seg.firstOffset, seg.lastOffset, size, isIndex})
		}
		last = seg.lastOffset
	}
	for _, dir := range []string{"segments", "index", ""} {
		if err := SyncDirectory(filepath.Join(dest, dir)); err != nil {
			return nil, 0, err
		}
	}
	return files, last, nil
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

// RecoverSnapshot rolls an interrupted install back to the old complete pair.
// An installed journal means the replacement was verified before publication.
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

// InstallCheckpoint keeps both old directories until the new WAL is verified.
func (w *WAL) InstallCheckpoint(staging string, expectedLast int64) (result error) {
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

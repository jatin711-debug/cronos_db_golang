package partition

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/shirou/gopsutil/v3/disk"
)

// BallastFile is the name of the file that reserves disk space in the data
// directory (see EnsureBallast).
const BallastFile = "disk-ballast"

// ballastHeadroom is how much must stay free after the ballast is created:
// a node does not fill its own disk to reserve space on it.
const ballastHeadroom = 256 << 20

// diskFree returns the free bytes of the filesystem holding dir; a variable
// so that tests can set it.
var diskFree = func(dir string) (uint64, error) {
	stat, err := disk.Usage(dir)
	if err != nil {
		return 0, err
	}
	return stat.Free, nil
}

// EnsureBallast keeps a file of size bytes in dataDir that holds disk space
// in reserve. When the disk fills anyway, an operator deletes it, and the
// room it frees lets the node deliver, prune its log and restart, which all
// take some disk. The node creates it again at its next start, when there is
// room for it and ballastHeadroom besides.
//
// The space is allocated, not a sparse file: on Linux with fallocate, and
// elsewhere by writing the bytes. On a filesystem that compresses data it
// may reserve less. A size of 0 removes a ballast left from before. Nothing
// here stops the node: a ballast that cannot be made is logged and left out.
func EnsureBallast(dataDir string, size int64) {
	path := filepath.Join(dataDir, BallastFile)
	info, err := os.Stat(path)
	switch {
	case size <= 0:
		if err == nil {
			if err := os.Remove(path); err != nil {
				slog.Warn("Could not remove the disk ballast, which is no longer configured", "path", path, "error", err)
			} else {
				slog.Info("Removed the disk ballast: --disk-ballast-bytes is 0", "path", path, "bytes", info.Size())
			}
		}
		return
	case err == nil && info.Size() == size:
		return
	case err == nil:
		// Another size: made again below, once the old one is gone.
		if err := os.Remove(path); err != nil {
			slog.Warn("Could not replace the disk ballast", "path", path, "error", err)
			return
		}
	}
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		slog.Warn("Could not create the data directory for the disk ballast", "path", path, "error", err)
		return
	}
	free, err := diskFree(dataDir)
	if err != nil {
		slog.Warn("Disk ballast not created: free space unknown", "path", path, "error", err)
		return
	}
	if free < uint64(size)+ballastHeadroom {
		slog.Warn("Disk ballast not created: not enough free space", "path", path, "bytes", size, "free_bytes", free)
		return
	}
	if err := writeBallast(path, size); err != nil {
		_ = os.Remove(path)
		slog.Warn("Disk ballast not created", "path", path, "bytes", size, "error", err)
		return
	}
	slog.Info("Disk ballast in place; delete it to free space if the disk fills", "path", path, "bytes", size)
}

// writeBallast creates path with size bytes allocated and synced.
func writeBallast(path string, size int64) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if err := allocate(f, size); err != nil {
		_ = f.Close()
		return fmt.Errorf("allocate %d bytes: %w", size, err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

// writeZeros fills f with size bytes, a megabyte at a time.
func writeZeros(f *os.File, size int64) error {
	chunk := make([]byte, 1<<20)
	for written := int64(0); written < size; {
		n := min(int64(len(chunk)), size-written)
		if _, err := f.Write(chunk[:n]); err != nil {
			return err
		}
		written += n
	}
	return nil
}

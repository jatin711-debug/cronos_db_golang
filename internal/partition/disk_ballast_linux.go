//go:build linux

package partition

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

// allocate reserves size bytes for f with fallocate, which allocates blocks
// without writing them, or writes them where the filesystem cannot.
func allocate(f *os.File, size int64) error {
	err := unix.Fallocate(int(f.Fd()), 0, 0, size)
	if errors.Is(err, unix.EOPNOTSUPP) || errors.Is(err, unix.ENOSYS) {
		return writeZeros(f, size)
	}
	return err
}

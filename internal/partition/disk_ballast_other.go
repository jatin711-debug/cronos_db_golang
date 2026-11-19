//go:build !linux

package partition

import "os"

// allocate reserves size bytes for f by writing them.
func allocate(f *os.File, size int64) error {
	return writeZeros(f, size)
}

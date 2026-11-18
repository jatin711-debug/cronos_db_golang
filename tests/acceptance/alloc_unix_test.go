//go:build acceptance && !windows

package acceptance

import (
	"io/fs"
	"syscall"
)

// allocatedBytes is what a file takes on disk: its allocated blocks, which
// can be more than its length.
func allocatedBytes(info fs.FileInfo) int64 {
	if st, ok := info.Sys().(*syscall.Stat_t); ok {
		return st.Blocks * 512
	}
	return info.Size()
}

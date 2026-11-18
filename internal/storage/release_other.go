//go:build !linux

package storage

import "os"

// releaseBeyondEnd frees blocks held past a file's end; see the Linux
// version. Elsewhere it does nothing: Windows cannot truncate a mapped file,
// and the filesystems that keep such blocks are Linux ones.
func releaseBeyondEnd(*os.File) error { return nil }

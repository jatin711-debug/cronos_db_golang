package partition

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The guard refuses publishes while less than its floor of bytes is free,
// whatever the used fraction, and takes them again once two points of the
// filesystem more are free.
func TestDiskGuard_RefusesBelowTheFreeSpaceFloor(t *testing.T) {
	const total = 100 << 30 // two points: 2 GiB
	g := NewDiskGuard(t.TempDir(), 0)
	g.SetMinFree(10 << 30)
	var free uint64
	g.usage = func() (diskSample, error) {
		return diskSample{used: 1 - float64(free)/total, free: free, total: total}, nil
	}
	for i, step := range []struct {
		free    uint64
		refuses bool
	}{
		{50 << 30, false},
		{10 << 30, false},
		{10<<30 - 1, true},
		{11 << 30, true}, // within the gap
		{12 << 30, false},
	} {
		free = step.free
		g.sample()
		err := g.Check()
		if refuses := err != nil; refuses != step.refuses {
			t.Fatalf("step %d (%d bytes free): refuses=%v, want %v", i, step.free, refuses, step.refuses)
		}
		if err != nil && !strings.Contains(err.Error(), "--min-free-disk-bytes") {
			t.Fatalf("refusal does not name the floor: %v", err)
		}
	}
}

// A guard with a floor only (no used-fraction limit) never refuses for the
// fraction alone.
func TestDiskGuard_NoFractionLimitWhenZero(t *testing.T) {
	g := NewDiskGuard(t.TempDir(), 0)
	g.SetMinFree(1 << 20)
	g.usage = func() (diskSample, error) { return diskSample{used: 0.999, free: 1 << 30, total: 1 << 40}, nil }
	g.sample()
	if err := g.Check(); err != nil {
		t.Fatalf("refused at 99.9%% used with no fraction limit and enough free: %v", err)
	}
}

func TestEnsureBallast(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, BallastFile)
	size := func() int64 {
		info, err := os.Stat(path)
		if err != nil {
			return -1
		}
		return info.Size()
	}

	EnsureBallast(dir, 3<<20)
	if got := size(); got != 3<<20 {
		t.Fatalf("ballast is %d bytes, want %d", got, 3<<20)
	}
	// Kept as it is, then made again at another size.
	EnsureBallast(dir, 3<<20)
	EnsureBallast(dir, 1<<20)
	if got := size(); got != 1<<20 {
		t.Fatalf("ballast is %d bytes after resizing, want %d", got, 1<<20)
	}
	// Deleted by an operator, made again at the next start.
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	EnsureBallast(dir, 1<<20)
	if got := size(); got != 1<<20 {
		t.Fatalf("ballast not made again after it was deleted: %d", got)
	}
	// 0 removes it.
	EnsureBallast(dir, 0)
	if got := size(); got != -1 {
		t.Fatalf("ballast left in place with a size of 0: %d bytes", got)
	}
}

// A node does not fill its own disk to reserve space on it.
func TestEnsureBallast_LeavesRoom(t *testing.T) {
	dir := t.TempDir()
	saved := diskFree
	t.Cleanup(func() { diskFree = saved })
	diskFree = func(string) (uint64, error) { return 100 << 20, nil }
	EnsureBallast(dir, 1<<20)
	if _, err := os.Stat(filepath.Join(dir, BallastFile)); !os.IsNotExist(err) {
		t.Fatalf("ballast made with only 100 MiB free: %v", err)
	}
}

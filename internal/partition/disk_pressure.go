package partition

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"
)

// DiskMonitor watches disk usage and triggers emergency compaction.
type DiskMonitor struct {
	dataDir        string
	thresholdPct   float64 // e.g. 0.85 for 85%
	checkInterval  time.Duration
	compactFn      func()
	quit           chan struct{}
}

// NewDiskMonitor creates a disk pressure monitor.
func NewDiskMonitor(dataDir string, thresholdPct float64, compactFn func()) *DiskMonitor {
	return &DiskMonitor{
		dataDir:       dataDir,
		thresholdPct:  thresholdPct,
		checkInterval: 30 * time.Second,
		compactFn:     compactFn,
		quit:          make(chan struct{}),
	}
}

// Start begins background monitoring.
func (dm *DiskMonitor) Start() {
	go dm.loop()
}

// Stop stops monitoring.
func (dm *DiskMonitor) Stop() {
	close(dm.quit)
}

func (dm *DiskMonitor) loop() {
	ticker := time.NewTicker(dm.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			usage, err := dm.usage()
			if err != nil {
				slog.Warn("Disk usage check failed", "error", err)
				continue
			}
			if usage >= dm.thresholdPct {
				slog.Warn("Disk pressure detected", "usage", fmt.Sprintf("%.1f%%", usage*100), "threshold", fmt.Sprintf("%.1f%%", dm.thresholdPct*100))
				if dm.compactFn != nil {
					dm.compactFn()
				}
			}
		case <-dm.quit:
			return
		}
	}
}

func (dm *DiskMonitor) usage() (float64, error) {
	// Cross-platform disk usage check
	// On Unix: syscall.Statfs. On Windows: GetDiskFreeSpaceEx.
	// For portability, we use a simple heuristic based on file sizes in the data dir.
	var total int64
	var count int
	_ = filepath.Walk(dm.dataDir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		total += info.Size()
		count++
		return nil
	})
	if count == 0 {
		return 0, fmt.Errorf("empty data dir")
	}
	// Approximate usage based on segment sizes vs a configurable max
	// Real implementation would use platform-specific syscalls
	maxBytes := int64(100 << 30) // 100GB default placeholder
	return float64(total) / float64(maxBytes), nil
}

// WalDiskSize returns the total size of WAL files in bytes.
func WalDiskSize(dataDir string) (int64, error) {
	var total int64
	err := filepath.Walk(dataDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if !info.IsDir() {
			total += info.Size()
		}
		return nil
	})
	return total, err
}

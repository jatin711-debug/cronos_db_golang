package partition

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/shirou/gopsutil/v3/disk"
)

// DiskMonitor watches disk usage and triggers emergency compaction.
type DiskMonitor struct {
	dataDir       string
	thresholdPct  float64 // e.g. 0.85 for 85%
	checkInterval time.Duration
	compactFn     func()
	quit          chan struct{}
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
	usageStat, err := disk.Usage(dm.dataDir)
	if err != nil {
		return 0, fmt.Errorf("disk usage check: %w", err)
	}
	return usageStat.UsedPercent / 100.0, nil
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

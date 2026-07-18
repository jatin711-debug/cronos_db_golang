package partition

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/shirou/gopsutil/v3/disk"
)

// DiskMonitor watches disk usage under dataDir and invokes compactFn when
// usage meets or exceeds thresholdPct (fraction in [0,1], e.g. 0.85 for 85%).
type DiskMonitor struct {
	dataDir       string        // path whose filesystem usage is sampled
	thresholdPct  float64       // trip threshold as a fraction of capacity (0–1)
	checkInterval time.Duration // how often usage is sampled (default 30s)
	compactFn     func()        // emergency compaction callback; may be nil
	quit          chan struct{} // closed by Stop to end the loop
}

// NewDiskMonitor creates a disk pressure monitor for dataDir. compactFn is
// called when used fraction >= thresholdPct (e.g. 0.85).
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

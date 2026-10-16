package partition

import (
	"fmt"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
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

// diskPublishRefused is 1 while the node refuses publishes for lack of disk.
var diskPublishRefused = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "cronos_disk_publish_refused",
	Help: "1 while the node refuses publishes because the filesystem holding its data is nearly full",
})

// diskGuardResumeGap is how far below its limit disk usage must fall before a
// DiskGuard takes publishes again, so that it does not switch back and forth
// at the limit.
const diskGuardResumeGap = 0.02

// DiskGuard refuses publishes while the filesystem that holds the node's data
// is nearly full.
//
// Without it a full disk is found by a write: a segment that cannot grow, a
// sync that fails. A failed write stops the log (see storage.ErrLogFailed),
// and an mmap segment can even fail where nothing can return an error. What a
// node needs to recover, delivery and acknowledgements, retention, a restart,
// takes some disk as well. So publishes, the one thing that only adds data,
// are refused first, with space still left, while deliveries, retention and
// replication go on.
//
// Usage is sampled in the background; Check is an atomic load, cheap enough
// for every publish.
type DiskGuard struct {
	dataDir  string
	refuseAt float64 // used fraction at which publishes are refused
	interval time.Duration
	usage    func() (float64, error)
	used     atomic.Uint64 // math.Float64bits of the last sample
	refusing atomic.Bool
	quit     chan struct{}
	stopOnce sync.Once
}

// NewDiskGuard returns a guard that refuses publishes once the filesystem
// holding dataDir is refuseAt full (a fraction in (0, 1]), and takes them again
// below refuseAt minus two points.
func NewDiskGuard(dataDir string, refuseAt float64) *DiskGuard {
	g := &DiskGuard{
		dataDir:  dataDir,
		refuseAt: refuseAt,
		interval: 2 * time.Second,
		quit:     make(chan struct{}),
	}
	g.usage = func() (float64, error) {
		stat, err := disk.Usage(g.dataDir)
		if err != nil {
			return 0, fmt.Errorf("disk usage check: %w", err)
		}
		return stat.UsedPercent / 100.0, nil
	}
	return g
}

// Start samples usage once, so that the first publishes are already judged,
// and then in the background.
func (g *DiskGuard) Start() {
	g.sample()
	go func() {
		ticker := time.NewTicker(g.interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				g.sample()
			case <-g.quit:
				return
			}
		}
	}()
}

// Stop ends the background sampling.
func (g *DiskGuard) Stop() {
	g.stopOnce.Do(func() { close(g.quit) })
}

// sample reads usage and decides whether publishes are refused. A failed read
// leaves the decision as it was.
func (g *DiskGuard) sample() {
	used, err := g.usage()
	if err != nil {
		slog.Warn("Disk usage check failed", "data_dir", g.dataDir, "error", err)
		return
	}
	g.used.Store(math.Float64bits(used))
	switch {
	case !g.refusing.Load() && used >= g.refuseAt:
		g.refusing.Store(true)
		diskPublishRefused.Set(1)
		slog.Error("Refusing publishes: the filesystem holding the data is nearly full",
			"data_dir", g.dataDir, "used", fmt.Sprintf("%.1f%%", used*100), "limit", fmt.Sprintf("%.1f%%", g.refuseAt*100))
	case g.refusing.Load() && used < g.refuseAt-diskGuardResumeGap:
		g.refusing.Store(false)
		diskPublishRefused.Set(0)
		slog.Info("Taking publishes again: disk usage is below the limit",
			"data_dir", g.dataDir, "used", fmt.Sprintf("%.1f%%", used*100), "limit", fmt.Sprintf("%.1f%%", g.refuseAt*100))
	}
}

// Check returns an error while publishes are refused.
func (g *DiskGuard) Check() error {
	if g == nil || !g.refusing.Load() {
		return nil
	}
	used := math.Float64frombits(g.used.Load())
	return fmt.Errorf("the node's disk is %.1f%% full, at or above its limit of %.1f%%; publishes are refused until usage falls below %.1f%% (deliveries continue, and retention frees space as consumers finish events)",
		used*100, g.refuseAt*100, (g.refuseAt-diskGuardResumeGap)*100)
}

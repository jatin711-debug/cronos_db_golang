package cluster

import (
	"fmt"
	"log/slog"
	"runtime"
	"time"

	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/disk"
	"github.com/shirou/gopsutil/v3/mem"
	"github.com/shirou/gopsutil/v3/net"
)

// AutoScalerMetrics provides resource metrics for cluster scaling decisions.
type AutoScalerMetrics interface {
	// CPUUsage returns host CPU utilization in the range [0.0, 1.0].
	CPUUsage() float64
	// MemoryUsage returns host memory utilization in the range [0.0, 1.0].
	MemoryUsage() float64
	// DiskUsage returns data-directory disk utilization in the range [0.0, 1.0].
	DiskUsage() float64
	// RequestRate returns approximate requests per second handled by this node.
	RequestRate() float64
	// PartitionLag returns per-partition consumer/replication lag in events.
	PartitionLag() map[int32]int64
}

// AutoScaler periodically evaluates resource metrics and logs scale-up signals
// when thresholds are exceeded (advisory; does not mutate cluster membership).
type AutoScaler struct {
	metrics       AutoScalerMetrics
	cpuThreshold  float64
	memThreshold  float64
	diskThreshold float64
	checkInterval time.Duration
	quit          chan struct{}
}

// NewAutoScaler creates an AutoScaler with default thresholds (CPU 70%, mem/disk 80%).
func NewAutoScaler(metrics AutoScalerMetrics) *AutoScaler {
	return &AutoScaler{
		metrics:       metrics,
		cpuThreshold:  0.70,
		memThreshold:  0.80,
		diskThreshold: 0.80,
		checkInterval: 30 * time.Second,
		quit:          make(chan struct{}),
	}
}

// Start begins background scaling checks.
func (a *AutoScaler) Start() {
	go a.loop()
}

// Stop stops the auto-scaler.
func (a *AutoScaler) Stop() {
	close(a.quit)
}

func (a *AutoScaler) loop() {
	ticker := time.NewTicker(a.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			a.evaluate()
		case <-a.quit:
			return
		}
	}
}

func (a *AutoScaler) evaluate() {
	cpu := a.metrics.CPUUsage()
	mem := a.metrics.MemoryUsage()
	disk := a.metrics.DiskUsage()

	if cpu > a.cpuThreshold {
		slog.Warn("AutoScaler: CPU above threshold", "cpu", fmt.Sprintf("%.1f%%", cpu*100), "threshold", fmt.Sprintf("%.1f%%", a.cpuThreshold*100))
	}
	if mem > a.memThreshold {
		slog.Warn("AutoScaler: Memory above threshold", "mem", fmt.Sprintf("%.1f%%", mem*100), "threshold", fmt.Sprintf("%.1f%%", a.memThreshold*100))
	}
	if disk > a.diskThreshold {
		slog.Warn("AutoScaler: Disk above threshold", "disk", fmt.Sprintf("%.1f%%", disk*100))
	}

	lag := a.metrics.PartitionLag()
	for pid, l := range lag {
		if l > 10000 {
			slog.Warn("AutoScaler: partition lag high", "partition", pid, "lag", l)
		}
	}
}

// SystemMetrics collects real OS-level metrics using gopsutil for AutoScaler.
type SystemMetrics struct {
	dataDir string
}

// NewSystemMetrics creates a SystemMetrics collector rooted at dataDir for disk usage.
func NewSystemMetrics(dataDir string) *SystemMetrics {
	return &SystemMetrics{dataDir: dataDir}
}

// CPUUsage returns host CPU utilization in the range [0.0, 1.0].
func (SystemMetrics) CPUUsage() float64 {
	percent, err := cpu.Percent(100*time.Millisecond, false)
	if err != nil || len(percent) == 0 {
		return 0
	}
	return percent[0] / 100.0
}

// MemoryUsage returns host virtual-memory utilization in the range [0.0, 1.0].
func (SystemMetrics) MemoryUsage() float64 {
	v, err := mem.VirtualMemory()
	if err != nil {
		return 0
	}
	return v.UsedPercent / 100.0
}

// DiskUsage returns used-percent of the filesystem containing dataDir in [0.0, 1.0].
func (s SystemMetrics) DiskUsage() float64 {
	usage, err := disk.Usage(s.dataDir)
	if err != nil {
		return 0
	}
	return usage.UsedPercent / 100.0
}

// RequestRate currently returns 0; intended for metrics-pipeline integration.
func (SystemMetrics) RequestRate() float64 {
	// Would integrate with metrics pipeline
	return 0
}

// PartitionLag currently returns nil; intended for consumer-offset integration.
func (SystemMetrics) PartitionLag() map[int32]int64 {
	// Would integrate with consumer offset store
	return nil
}

// SimpleMetrics is a basic metrics implementation using runtime stats.
// Deprecated: use SystemMetrics for real host metrics.
type SimpleMetrics struct{}

// CPUUsage always returns 0 for SimpleMetrics.
func (SimpleMetrics) CPUUsage() float64 { return 0 }

// MemoryUsage returns process Sys memory in gibibytes (not a 0–1 ratio).
func (SimpleMetrics) MemoryUsage() float64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return float64(m.Sys) / float64(1<<30)
}

// DiskUsage always returns 0 for SimpleMetrics.
func (SimpleMetrics) DiskUsage() float64 { return 0 }

// RequestRate always returns 0 for SimpleMetrics.
func (SimpleMetrics) RequestRate() float64 { return 0 }

// PartitionLag always returns nil for SimpleMetrics.
func (SimpleMetrics) PartitionLag() map[int32]int64 { return nil }

// NetworkStats provides host network I/O statistics via gopsutil.
type NetworkStats struct{}

// IOStats returns aggregate bytes sent and received on host network interfaces.
func (NetworkStats) IOStats() (sent uint64, recv uint64, err error) {
	ioCounters, err := net.IOCounters(false)
	if err != nil || len(ioCounters) == 0 {
		return 0, 0, err
	}
	return ioCounters[0].BytesSent, ioCounters[0].BytesRecv, nil
}

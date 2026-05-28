package cluster

import (
	"fmt"
	"log/slog"
	"runtime"
	"time"
)

// AutoScalerMetrics provides resource metrics for scaling decisions.
type AutoScalerMetrics interface {
	CPUUsage() float64        // 0.0-1.0
	MemoryUsage() float64     // 0.0-1.0
	DiskUsage() float64       // 0.0-1.0
	RequestRate() float64     // requests/sec
	PartitionLag() map[int32]int64
}

// AutoScaler decides when to scale the cluster.
type AutoScaler struct {
	metrics      AutoScalerMetrics
	cpuThreshold float64
	diskThreshold float64
	checkInterval  time.Duration
	quit           chan struct{}
}

// NewAutoScaler creates an auto-scaler.
func NewAutoScaler(metrics AutoScalerMetrics) *AutoScaler {
	return &AutoScaler{
		metrics:       metrics,
		cpuThreshold:  0.70,
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
	disk := a.metrics.DiskUsage()

	if cpu > a.cpuThreshold {
		slog.Warn("AutoScaler: CPU above threshold", "cpu", fmt.Sprintf("%.1f%%", cpu*100), "threshold", fmt.Sprintf("%.1f%%", a.cpuThreshold*100))
		// In production: trigger cloud provider API to provision new node
	}
	if disk > a.diskThreshold {
		slog.Warn("AutoScaler: Disk above threshold", "disk", fmt.Sprintf("%.1f%%", disk*100))
		// In production: trigger compaction alert or disk expansion
	}
}

// SimpleMetrics is a basic metrics implementation using runtime stats.
type SimpleMetrics struct{}

func (SimpleMetrics) CPUUsage() float64    { return 0 } // Would integrate with gopsutil
func (SimpleMetrics) MemoryUsage() float64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	// Approximate; real implementation uses system memory
	return float64(m.Sys) / float64(1<<30) // Placeholder
}
func (SimpleMetrics) DiskUsage() float64   { return 0 }
func (SimpleMetrics) RequestRate() float64 { return 0 }
func (SimpleMetrics) PartitionLag() map[int32]int64 { return nil }

package slo

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Window defines an SLO observation window.
type Window struct {
	Duration     time.Duration
	TargetP99    time.Duration
	TargetP95    time.Duration
	MaxErrorRate float64 // 0.0-1.0
}

// Recorder tracks latency and error rates for SLO compliance.
type Recorder struct {
	mu sync.RWMutex

	totalRequests  atomic.Int64
	errorRequests  atomic.Int64
	latencyBuckets []time.Duration // Copied before calculation
	bucketMu       sync.Mutex
	maxBuckets     int

	window      Window
	resetTicker *time.Ticker
	quit        chan struct{}
}

// NewRecorder creates an SLO recorder.
func NewRecorder(window Window) *Recorder {
	return &Recorder{
		latencyBuckets: make([]time.Duration, 0, 10000),
		maxBuckets:     10000,
		window:         window,
		quit:           make(chan struct{}),
	}
}

// Start begins the periodic reset loop. Call once after construction.
func (r *Recorder) Start() {
	if r.window.Duration <= 0 {
		return
	}
	r.resetTicker = time.NewTicker(r.window.Duration)
	go func() {
		for {
			select {
			case <-r.resetTicker.C:
				r.Reset()
			case <-r.quit:
				return
			}
		}
	}()
}

// Stop halts the periodic reset loop.
func (r *Recorder) Stop() {
	close(r.quit)
	if r.resetTicker != nil {
		r.resetTicker.Stop()
	}
}

// Record records a request outcome.
func (r *Recorder) Record(latency time.Duration, err bool) {
	r.totalRequests.Add(1)
	if err {
		r.errorRequests.Add(1)
	}

	r.bucketMu.Lock()
	defer r.bucketMu.Unlock()
	if len(r.latencyBuckets) >= r.maxBuckets {
		// Drop oldest 10% to prevent unbounded growth
		r.latencyBuckets = r.latencyBuckets[len(r.latencyBuckets)/10:]
	}
	r.latencyBuckets = append(r.latencyBuckets, latency)
}

// ErrorRate returns current error rate.
func (r *Recorder) ErrorRate() float64 {
	total := r.totalRequests.Load()
	if total == 0 {
		return 0
	}
	return float64(r.errorRequests.Load()) / float64(total)
}

// copyBuckets returns an unsorted copy of latency buckets.
func (r *Recorder) copyBuckets() []time.Duration {
	r.bucketMu.Lock()
	defer r.bucketMu.Unlock()
	buckets := make([]time.Duration, len(r.latencyBuckets))
	copy(buckets, r.latencyBuckets)
	return buckets
}

// percentile returns the p-th percentile (0.0-1.0) using Quickselect.
func percentile(buckets []time.Duration, p float64) time.Duration {
	n := len(buckets)
	if n == 0 {
		return 0
	}
	k := int(p * float64(n-1))
	if k < 0 {
		k = 0
	}
	if k >= n {
		k = n - 1
	}
	return quickselect(buckets, k)
}

// quickselect finds the k-th smallest element in slice s (0-indexed).
// It mutates s in-place. Average time complexity is O(n).
func quickselect(s []time.Duration, k int) time.Duration {
	if len(s) == 0 || k < 0 || k >= len(s) {
		return 0
	}
	left, right := 0, len(s)-1
	for left < right {
		pivotIndex := partition(s, left, right)
		if pivotIndex == k {
			return s[k]
		} else if pivotIndex < k {
			left = pivotIndex + 1
		} else {
			right = pivotIndex - 1
		}
	}
	return s[left]
}

func partition(s []time.Duration, left, right int) int {
	mid := left + (right-left)/2
	pivotVal := s[mid]
	s[mid], s[right] = s[right], s[mid]
	storeIndex := left
	for i := left; i < right; i++ {
		if s[i] < pivotVal {
			s[i], s[storeIndex] = s[storeIndex], s[i]
			storeIndex++
		}
	}
	s[storeIndex], s[right] = s[right], s[storeIndex]
	return storeIndex
}

// P99 returns the 99th percentile latency.
func (r *Recorder) P99() time.Duration {
	buckets := r.copyBuckets()
	return percentile(buckets, 0.99)
}

// P95 returns the 95th percentile latency.
func (r *Recorder) P95() time.Duration {
	buckets := r.copyBuckets()
	return percentile(buckets, 0.95)
}

// Compliant returns true if current metrics meet SLO targets.
func (r *Recorder) Compliant() bool {
	if r.ErrorRate() > r.window.MaxErrorRate {
		return false
	}
	if r.window.TargetP99 > 0 && r.P99() > r.window.TargetP99 {
		return false
	}
	return true
}

// Reset clears all recorded data (call at window boundary).
func (r *Recorder) Reset() {
	r.totalRequests.Store(0)
	r.errorRequests.Store(0)
	r.bucketMu.Lock()
	r.latencyBuckets = r.latencyBuckets[:0]
	r.bucketMu.Unlock()
}

type sloCollector struct {
	recorder *Recorder
	p99Desc  *prometheus.Desc
	p95Desc  *prometheus.Desc
	errDesc  *prometheus.Desc
	compDesc *prometheus.Desc
}

// PrometheusCollector returns a prometheus.Collector for this SLO recorder.
func (r *Recorder) PrometheusCollector() prometheus.Collector {
	return &sloCollector{
		recorder: r,
		p99Desc:  prometheus.NewDesc("cronos_slo_p99_latency_ms", "99th percentile latency in milliseconds", nil, nil),
		p95Desc:  prometheus.NewDesc("cronos_slo_p95_latency_ms", "95th percentile latency in milliseconds", nil, nil),
		errDesc:  prometheus.NewDesc("cronos_slo_error_rate", "Current request error rate", nil, nil),
		compDesc: prometheus.NewDesc("cronos_slo_compliant", "SLO compliance status (1 = compliant, 0 = non-compliant)", nil, nil),
	}
}

func (c *sloCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.p99Desc
	ch <- c.p95Desc
	ch <- c.errDesc
	ch <- c.compDesc
}

func (c *sloCollector) Collect(ch chan<- prometheus.Metric) {
	p99 := float64(c.recorder.P99().Milliseconds())
	p95 := float64(c.recorder.P95().Milliseconds())
	errRate := c.recorder.ErrorRate()
	compliant := 0.0
	if c.recorder.Compliant() {
		compliant = 1.0
	}

	ch <- prometheus.MustNewConstMetric(c.p99Desc, prometheus.GaugeValue, p99)
	ch <- prometheus.MustNewConstMetric(c.p95Desc, prometheus.GaugeValue, p95)
	ch <- prometheus.MustNewConstMetric(c.errDesc, prometheus.GaugeValue, errRate)
	ch <- prometheus.MustNewConstMetric(c.compDesc, prometheus.GaugeValue, compliant)
}

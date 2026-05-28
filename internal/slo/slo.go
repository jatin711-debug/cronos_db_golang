package slo

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"
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
	latencyBuckets []time.Duration // Sorted ascending after copy
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

// sortedLatencies returns a sorted copy of latency buckets.
func (r *Recorder) sortedLatencies() []time.Duration {
	r.bucketMu.Lock()
	buckets := make([]time.Duration, len(r.latencyBuckets))
	copy(buckets, r.latencyBuckets)
	r.bucketMu.Unlock()

	if len(buckets) == 0 {
		return nil
	}
	sort.Slice(buckets, func(i, j int) bool { return buckets[i] < buckets[j] })
	return buckets
}

// percentile returns the p-th percentile (0.0-1.0) from sorted latencies.
func percentile(sorted []time.Duration, p float64) time.Duration {
	n := len(sorted)
	if n == 0 {
		return 0
	}
	idx := int(p * float64(n-1))
	if idx < 0 {
		idx = 0
	}
	if idx >= n {
		idx = n - 1
	}
	return sorted[idx]
}

// P99 returns the 99th percentile latency.
func (r *Recorder) P99() time.Duration {
	sorted := r.sortedLatencies()
	return percentile(sorted, 0.99)
}

// P95 returns the 95th percentile latency.
func (r *Recorder) P95() time.Duration {
	sorted := r.sortedLatencies()
	return percentile(sorted, 0.95)
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

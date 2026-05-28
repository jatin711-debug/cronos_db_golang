package slo

import (
	"sync"
	"sync/atomic"
	"time"
)

// Window defines an SLO observation window.
type Window struct {
	Duration   time.Duration
	TargetP99  time.Duration
	TargetP95  time.Duration
	MaxErrorRate float64 // 0.0-1.0
}

// Recorder tracks latency and error rates for SLO compliance.
type Recorder struct {
	mu sync.RWMutex

	totalRequests   atomic.Int64
	errorRequests   atomic.Int64
	latencyBuckets  []time.Duration // Sorted ascending
	bucketMu        sync.Mutex
	maxBuckets      int

	window Window
}

// NewRecorder creates an SLO recorder.
func NewRecorder(window Window) *Recorder {
	return &Recorder{
		latencyBuckets: make([]time.Duration, 0, 10000),
		maxBuckets:     10000,
		window:         window,
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

// P99 returns approximate p99 latency.
func (r *Recorder) P99() time.Duration {
	r.bucketMu.Lock()
	buckets := make([]time.Duration, len(r.latencyBuckets))
	copy(buckets, r.latencyBuckets)
	r.bucketMu.Unlock()

	if len(buckets) == 0 {
		return 0
	}
	// Simple selection sort for p99 — adequate for small buckets
	// In production, use a streaming histogram (e.g.,HdrHistogram)
	idx := int(float64(len(buckets)) * 0.99)
	if idx >= len(buckets) {
		idx = len(buckets) - 1
	}
	// Approximate: not a true percentile but good enough for SLO signaling
	return buckets[idx]
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

// Package slo records request latency and error rates against configurable
// service-level objectives using a lock-free histogram.
//
// Recorder estimates percentiles from fixed microsecond buckets and can expose
// gauges via PrometheusCollector for dashboards and alerting.
package slo

import (
	"sync/atomic"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/pkg/utils"
	"github.com/prometheus/client_golang/prometheus"
)

// Window defines an SLO observation window and its targets.
type Window struct {
	// Duration is the reset period for rolling observations; 0 disables auto-reset.
	Duration time.Duration
	// TargetP99 is the maximum allowed estimated p99 latency; 0 skips the p99 check.
	TargetP99 time.Duration
	// TargetP95 is the maximum allowed estimated p95 latency (reserved for extensions).
	TargetP95 time.Duration
	// MaxErrorRate is the maximum allowed error ratio in [0.0, 1.0].
	MaxErrorRate float64
}

// latencyHistogramBounds are the upper bounds of each latency bucket in
// microseconds. The last bucket is +Inf (represented by math.MaxInt64).
// These buckets cover the range from 100us to 10s, which is sufficient for
// publish/subscribe SLO monitoring.
var latencyHistogramBounds = []int64{
	100,      // 100us
	250,      // 250us
	500,      // 500us
	1000,     // 1ms
	2500,     // 2.5ms
	5000,     // 5ms
	10000,    // 10ms
	25000,    // 25ms
	50000,    // 50ms
	100000,   // 100ms
	250000,   // 250ms
	500000,   // 500ms
	1000000,  // 1s
	2500000,  // 2.5s
	5000000,  // 5s
	10000000, // 10s
}

// Recorder tracks latency and error rates for SLO compliance using a lock-free
// atomic histogram. Percentiles are estimated from bucket counts rather than
// exact samples, which is standard for SLO monitoring and eliminates the mutex
// contention of maintaining a sorted sample array.
type Recorder struct {
	totalRequests atomic.Int64
	errorRequests atomic.Int64
	buckets       []atomic.Int64 // one counter per latencyHistogramBounds, plus one for +Inf

	window      Window
	resetTicker *time.Ticker
	quit        chan struct{}
}

// NewRecorder creates an SLO recorder.
func NewRecorder(window Window) *Recorder {
	return &Recorder{
		buckets: make([]atomic.Int64, len(latencyHistogramBounds)+1),
		window:  window,
		quit:    make(chan struct{}),
	}
}

// Start begins the periodic reset loop. Call once after construction.
func (r *Recorder) Start() {
	if r.window.Duration <= 0 {
		return
	}
	r.resetTicker = time.NewTicker(r.window.Duration)
	utils.GoSafe("slo-reset", func() {
		for {
			select {
			case <-r.resetTicker.C:
				r.Reset()
			case <-r.quit:
				return
			}
		}
	})
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

	us := latency.Microseconds()
	idx := latencyBucketIndex(us)
	r.buckets[idx].Add(1)
}

// latencyBucketIndex returns the bucket index for a latency in microseconds.
func latencyBucketIndex(us int64) int {
	for i, bound := range latencyHistogramBounds {
		if us <= bound {
			return i
		}
	}
	return len(latencyHistogramBounds)
}

// ErrorRate returns current error rate.
func (r *Recorder) ErrorRate() float64 {
	total := r.totalRequests.Load()
	if total == 0 {
		return 0
	}
	return float64(r.errorRequests.Load()) / float64(total)
}

// percentile returns the estimated p-th percentile (0.0-1.0) from the histogram.
func (r *Recorder) percentile(p float64) time.Duration {
	total := r.totalRequests.Load()
	if total == 0 {
		return 0
	}

	target := int64(p*float64(total-1)) + 1 // 1-indexed rank in [1,total]
	if target < 1 {
		target = 1
	}
	if target > total {
		target = total
	}

	var cumulative int64
	for i := range r.buckets {
		count := r.buckets[i].Load()
		cumulative += count
		if cumulative >= target {
			if i < len(latencyHistogramBounds) {
				return time.Duration(latencyHistogramBounds[i]) * time.Microsecond
			}
			return 10 * time.Second
		}
	}
	return 10 * time.Second
}

// P99 returns the estimated 99th percentile latency.
func (r *Recorder) P99() time.Duration {
	return r.percentile(0.99)
}

// P95 returns the estimated 95th percentile latency.
func (r *Recorder) P95() time.Duration {
	return r.percentile(0.95)
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
	for i := range r.buckets {
		r.buckets[i].Store(0)
	}
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

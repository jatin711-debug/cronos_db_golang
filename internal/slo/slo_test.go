package slo

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func TestNewRecorder(t *testing.T) {
	w := Window{
		Duration:     time.Hour,
		TargetP99:    100 * time.Millisecond,
		TargetP95:    50 * time.Millisecond,
		MaxErrorRate: 0.01,
	}
	r := NewRecorder(w)
	if r == nil {
		t.Fatal("NewRecorder should not return nil")
	}
	if r.window.Duration != time.Hour {
		t.Error("window not set correctly")
	}
}

func TestRecorder_Record(t *testing.T) {
	w := Window{Duration: time.Hour, MaxErrorRate: 0.01}
	r := NewRecorder(w)

	r.Record(10*time.Millisecond, false)
	r.Record(20*time.Millisecond, false)
	r.Record(30*time.Millisecond, true)

	if r.totalRequests.Load() != 3 {
		t.Errorf("expected 3 total requests, got %d", r.totalRequests.Load())
	}
	if r.errorRequests.Load() != 1 {
		t.Errorf("expected 1 error request, got %d", r.errorRequests.Load())
	}
}

func TestRecorder_ErrorRate(t *testing.T) {
	w := Window{Duration: time.Hour, MaxErrorRate: 0.01}
	r := NewRecorder(w)

	if r.ErrorRate() != 0 {
		t.Error("expected 0 error rate with no requests")
	}

	r.Record(10*time.Millisecond, false)
	r.Record(10*time.Millisecond, true)

	if r.ErrorRate() != 0.5 {
		t.Errorf("expected 0.5 error rate, got %f", r.ErrorRate())
	}
}

func TestRecorder_P99(t *testing.T) {
	w := Window{Duration: time.Hour, MaxErrorRate: 0.01}
	r := NewRecorder(w)

	if r.P99() != 0 {
		t.Error("expected 0 p99 with no data")
	}

	for i := 0; i < 100; i++ {
		r.Record(time.Duration(i)*time.Millisecond, false)
	}

	p99 := r.P99()
	// With 100 samples sorted 0..99ms, P99 falls in the 100ms bucket.
	if p99 < 95*time.Millisecond || p99 > 100*time.Millisecond {
		t.Errorf("expected p99 ~100ms, got %v", p99)
	}
}

func TestRecorder_P95(t *testing.T) {
	w := Window{Duration: time.Hour, MaxErrorRate: 0.01}
	r := NewRecorder(w)

	if r.P95() != 0 {
		t.Error("expected 0 p95 with no data")
	}

	for i := 0; i < 100; i++ {
		r.Record(time.Duration(i)*time.Millisecond, false)
	}

	p95 := r.P95()
	// With 100 samples sorted 0..99ms, P95 falls in the 100ms bucket.
	if p95 < 93*time.Millisecond || p95 > 100*time.Millisecond {
		t.Errorf("expected p95 ~100ms, got %v", p95)
	}
}

func TestRecorder_P99_BucketOverflow(t *testing.T) {
	w := Window{Duration: time.Hour, MaxErrorRate: 0.01}
	r := NewRecorder(w)

	for i := 0; i < 100000; i++ {
		r.Record(time.Duration(i%100)*time.Millisecond, false)
	}

	total := r.totalRequests.Load()
	if total != 100000 {
		t.Errorf("expected 100000 requests, got %d", total)
	}
	// Histogram buckets are atomic counters, so no overflow / dropping occurs.
	var sum int64
	for i := range r.buckets {
		sum += r.buckets[i].Load()
	}
	if sum != total {
		t.Errorf("expected bucket sum %d, got %d", total, sum)
	}
}

func TestRecorder_Compliant(t *testing.T) {
	w := Window{
		Duration:     time.Hour,
		TargetP99:    100 * time.Millisecond,
		MaxErrorRate: 0.01,
	}
	r := NewRecorder(w)

	if !r.Compliant() {
		t.Error("should be compliant with no data")
	}

	// Exceed error rate
	for i := 0; i < 100; i++ {
		r.Record(10*time.Millisecond, i < 50) // 50% errors
	}

	if r.Compliant() {
		t.Error("should not be compliant with high error rate")
	}
}

func TestRecorder_Compliant_P99Exceeded(t *testing.T) {
	w := Window{
		Duration:     time.Hour,
		TargetP99:    10 * time.Millisecond,
		MaxErrorRate: 1.0, // Allow all errors
	}
	r := NewRecorder(w)

	for i := 0; i < 100; i++ {
		r.Record(100*time.Millisecond, false)
	}

	if r.Compliant() {
		t.Error("should not be compliant when p99 exceeded")
	}
}

func TestRecorder_Reset(t *testing.T) {
	w := Window{Duration: time.Hour, MaxErrorRate: 0.01}
	r := NewRecorder(w)

	r.Record(10*time.Millisecond, false)
	r.Record(20*time.Millisecond, true)

	r.Reset()

	if r.totalRequests.Load() != 0 {
		t.Errorf("expected 0 total requests after reset, got %d", r.totalRequests.Load())
	}
	if r.errorRequests.Load() != 0 {
		t.Errorf("expected 0 errors after reset, got %d", r.errorRequests.Load())
	}
	var sum int64
	for i := range r.buckets {
		sum += r.buckets[i].Load()
	}
	if sum != 0 {
		t.Errorf("expected 0 buckets after reset, got %d", sum)
	}
}

func TestRecorder_Concurrent(t *testing.T) {
	w := Window{Duration: time.Hour, MaxErrorRate: 0.01}
	r := NewRecorder(w)

	done := make(chan struct{}, 10)
	for i := 0; i < 10; i++ {
		go func() {
			for j := 0; j < 100; j++ {
				r.Record(time.Duration(j)*time.Millisecond, j%10 == 0)
			}
			done <- struct{}{}
		}()
	}

	for i := 0; i < 10; i++ {
		<-done
	}

	if r.totalRequests.Load() != 1000 {
		t.Errorf("expected 1000 requests, got %d", r.totalRequests.Load())
	}
	if r.errorRequests.Load() != 100 {
		t.Errorf("expected 100 errors, got %d", r.errorRequests.Load())
	}
}

func TestRecorder_StartStop(t *testing.T) {
	w := Window{Duration: 100 * time.Millisecond, MaxErrorRate: 0.01}
	r := NewRecorder(w)

	r.Record(10*time.Millisecond, false)
	if r.totalRequests.Load() != 1 {
		t.Fatal("expected 1 request before start")
	}

	r.Start()
	time.Sleep(200 * time.Millisecond)
	r.Stop()

	// After auto-reset, should be cleared
	if r.totalRequests.Load() != 0 {
		t.Errorf("expected 0 requests after auto-reset, got %d", r.totalRequests.Load())
	}
}

func TestWindow_Fields(t *testing.T) {
	w := Window{
		Duration:     5 * time.Minute,
		TargetP99:    200 * time.Millisecond,
		TargetP95:    100 * time.Millisecond,
		MaxErrorRate: 0.05,
	}
	if w.Duration != 5*time.Minute {
		t.Error("duration mismatch")
	}
	if w.MaxErrorRate != 0.05 {
		t.Error("maxErrorRate mismatch")
	}
}

func TestRecorder_PrometheusCollector(t *testing.T) {
	w := Window{
		Duration:     time.Hour,
		TargetP99:    10 * time.Millisecond,
		MaxErrorRate: 0.01,
	}
	r := NewRecorder(w)

	// Record a couple of requests
	r.Record(5*time.Millisecond, false)
	r.Record(15*time.Millisecond, true)

	collector := r.PrometheusCollector()
	if collector == nil {
		t.Fatal("PrometheusCollector returned nil")
	}

	// Create registry and register
	reg := prometheus.NewRegistry()
	if err := reg.Register(collector); err != nil {
		t.Fatalf("failed to register collector: %v", err)
	}

	// Gather metrics
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("failed to gather metrics: %v", err)
	}

	expectedNames := map[string]bool{
		"cronos_slo_p99_latency_ms": false,
		"cronos_slo_p95_latency_ms": false,
		"cronos_slo_error_rate":     false,
		"cronos_slo_compliant":      false,
	}

	for _, mf := range mfs {
		name := mf.GetName()
		if _, ok := expectedNames[name]; ok {
			expectedNames[name] = true
			if len(mf.GetMetric()) != 1 {
				t.Errorf("expected 1 metric for %s, got %d", name, len(mf.GetMetric()))
			}
		}
	}

	for name, found := range expectedNames {
		if !found {
			t.Errorf("expected metric %s not gathered", name)
		}
	}
}

package slo

import (
	"testing"
	"time"
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
	if p99 == 0 {
		t.Error("expected positive p99")
	}
}

func TestRecorder_P99_BucketOverflow(t *testing.T) {
	w := Window{Duration: time.Hour, MaxErrorRate: 0.01}
	r := NewRecorder(w)
	r.maxBuckets = 10

	for i := 0; i < 20; i++ {
		r.Record(time.Duration(i)*time.Millisecond, false)
	}

	if len(r.latencyBuckets) > r.maxBuckets {
		t.Errorf("expected buckets <= %d, got %d", r.maxBuckets, len(r.latencyBuckets))
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
	if len(r.latencyBuckets) != 0 {
		t.Errorf("expected 0 buckets after reset, got %d", len(r.latencyBuckets))
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

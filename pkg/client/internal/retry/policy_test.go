package retry

import (
	"testing"
	"time"
)

func TestDefaultPolicy(t *testing.T) {
	p := DefaultPolicy()
	if p.MaxAttempts != 4 {
		t.Errorf("expected MaxAttempts 4, got %d", p.MaxAttempts)
	}
	if p.MinBackoff != 50*time.Millisecond {
		t.Errorf("expected MinBackoff 50ms, got %v", p.MinBackoff)
	}
	if p.MaxBackoff != 2*time.Second {
		t.Errorf("expected MaxBackoff 2s, got %v", p.MaxBackoff)
	}
	if p.Jitter != 0.2 {
		t.Errorf("expected Jitter 0.2, got %f", p.Jitter)
	}
}

func TestPolicy_Backoff(t *testing.T) {
	p := Policy{
		MaxAttempts: 5,
		MinBackoff:  100 * time.Millisecond,
		MaxBackoff:  10 * time.Second,
		Jitter:      0.0, // No jitter for deterministic test
	}

	b0 := p.Backoff(0)
	if b0 != p.MinBackoff {
		t.Errorf("attempt 0: expected %v, got %v", p.MinBackoff, b0)
	}

	b1 := p.Backoff(1)
	expected1 := 200 * time.Millisecond
	if b1 != expected1 {
		t.Errorf("attempt 1: expected %v, got %v", expected1, b1)
	}

	b2 := p.Backoff(2)
	expected2 := 400 * time.Millisecond
	if b2 != expected2 {
		t.Errorf("attempt 2: expected %v, got %v", expected2, b2)
	}

	// Should cap at MaxBackoff
	b10 := p.Backoff(10)
	if b10 != p.MaxBackoff {
		t.Errorf("attempt 10: expected %v (max), got %v", p.MaxBackoff, b10)
	}
}

func TestPolicy_Backoff_WithJitter(t *testing.T) {
	p := Policy{
		MaxAttempts: 5,
		MinBackoff:  100 * time.Millisecond,
		MaxBackoff:  10 * time.Second,
		Jitter:      0.5,
	}

	b1 := p.Backoff(1)
	base := 200 * time.Millisecond
	min := time.Duration(float64(base) * (1 - p.Jitter))
	max := time.Duration(float64(base) * (1 + p.Jitter))

	if b1 < min || b1 > max {
		t.Errorf("jittered backoff %v out of range [%v, %v]", b1, min, max)
	}
}

func TestPolicy_Backoff_NegativeAttempt(t *testing.T) {
	p := DefaultPolicy()
	b := p.Backoff(-1)
	if b != p.MinBackoff {
		t.Errorf("negative attempt should return min backoff, got %v", b)
	}
}

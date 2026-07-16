package circuitbreaker

import (
	"testing"
	"time"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.FailureThreshold != 5 {
		t.Errorf("expected FailureThreshold 5, got %d", cfg.FailureThreshold)
	}
	if cfg.SuccessThreshold != 2 {
		t.Errorf("expected SuccessThreshold 2, got %d", cfg.SuccessThreshold)
	}
	if cfg.Timeout != 10*time.Second {
		t.Errorf("expected Timeout 10s, got %v", cfg.Timeout)
	}
}

// TestRecordFailure_ThresholdZeroDisabled verifies that a non-positive
// FailureThreshold disables the breaker (it never opens), instead of the previous
// bug where `failures >= 0` opened it on the very first failure.
func TestRecordFailure_ThresholdZeroDisabled(t *testing.T) {
	cb := New(Config{FailureThreshold: 0, SuccessThreshold: 1, Timeout: time.Minute})
	for i := 0; i < 100; i++ {
		cb.RecordFailure()
	}
	if cb.CurrentState() != StateClosed {
		t.Fatalf("breaker with threshold 0 must stay closed (disabled), got %v", cb.CurrentState())
	}
	if !cb.Allow() {
		t.Fatal("disabled breaker must allow calls")
	}
}

func TestNew(t *testing.T) {
	cb := New(DefaultConfig())
	if cb == nil {
		t.Fatal("New should not return nil")
	}
	if cb.CurrentState() != StateClosed {
		t.Errorf("expected initial state Closed, got %v", cb.CurrentState())
	}
}

func TestCircuitBreaker_Allow(t *testing.T) {
	cb := New(DefaultConfig())
	if !cb.Allow() {
		t.Error("should allow when closed")
	}
}

func TestCircuitBreaker_RecordFailure(t *testing.T) {
	cfg := Config{FailureThreshold: 2, SuccessThreshold: 1, Timeout: 50 * time.Millisecond}
	cb := New(cfg)

	cb.RecordFailure()
	if cb.CurrentState() != StateClosed {
		t.Error("should still be closed after 1 failure")
	}

	cb.RecordFailure()
	if cb.CurrentState() != StateOpen {
		t.Errorf("expected Open after %d failures, got %v", cfg.FailureThreshold, cb.CurrentState())
	}

	if cb.Allow() {
		t.Error("should not allow when open")
	}
}

func TestCircuitBreaker_RecordSuccess(t *testing.T) {
	cfg := Config{FailureThreshold: 2, SuccessThreshold: 1, Timeout: 50 * time.Millisecond}
	cb := New(cfg)

	cb.RecordFailure()
	cb.RecordFailure()
	if cb.CurrentState() != StateOpen {
		t.Fatal("should be open")
	}

	// Wait for timeout to transition to half-open
	time.Sleep(60 * time.Millisecond)
	if !cb.Allow() {
		t.Error("should allow when half-open")
	}
	if cb.CurrentState() != StateHalfOpen {
		t.Errorf("expected HalfOpen, got %v", cb.CurrentState())
	}

	cb.RecordSuccess()
	if cb.CurrentState() != StateClosed {
		t.Errorf("expected Closed after success, got %v", cb.CurrentState())
	}
}

func TestCircuitBreaker_HalfOpenFailAgain(t *testing.T) {
	cfg := Config{FailureThreshold: 1, SuccessThreshold: 1, Timeout: 50 * time.Millisecond}
	cb := New(cfg)

	cb.RecordFailure()
	if cb.CurrentState() != StateOpen {
		t.Fatal("should be open")
	}

	time.Sleep(60 * time.Millisecond)
	cb.Allow() // transition to half-open

	cb.RecordFailure()
	if cb.CurrentState() != StateOpen {
		t.Errorf("expected Open after half-open failure, got %v", cb.CurrentState())
	}
}

func TestCircuitBreaker_MultipleSuccesses(t *testing.T) {
	cfg := Config{FailureThreshold: 5, SuccessThreshold: 3, Timeout: 1 * time.Hour}
	cb := New(cfg)

	for i := 0; i < 10; i++ {
		cb.RecordSuccess()
	}
	if cb.CurrentState() != StateClosed {
		t.Error("should remain closed with only successes")
	}
	if !cb.Allow() {
		t.Error("should allow when closed")
	}
}

func TestState_String(t *testing.T) {
	if StateClosed.String() != "closed" {
		t.Errorf("expected closed, got %s", StateClosed.String())
	}
	if StateOpen.String() != "open" {
		t.Errorf("expected open, got %s", StateOpen.String())
	}
	if StateHalfOpen.String() != "half-open" {
		t.Errorf("expected half-open, got %s", StateHalfOpen.String())
	}
}

func TestManager_ForAddress(t *testing.T) {
	mgr := NewManager(DefaultConfig())
	cb1 := mgr.ForAddress("node-1:8080")
	if cb1 == nil {
		t.Fatal("ForAddress should not return nil")
	}

	// Same address should return same breaker
	cb2 := mgr.ForAddress("node-1:8080")
	if cb1 != cb2 {
		t.Error("same address should return same circuit breaker")
	}

	// Different address should return different breaker
	cb3 := mgr.ForAddress("node-2:8080")
	if cb1 == cb3 {
		t.Error("different address should return different circuit breaker")
	}
}

func TestManager_Concurrent(t *testing.T) {
	mgr := NewManager(DefaultConfig())
	done := make(chan struct{}, 10)
	for i := 0; i < 10; i++ {
		go func(idx int) {
			addr := "node"
			cb := mgr.ForAddress(addr)
			if cb == nil {
				t.Error("should not return nil")
			}
			done <- struct{}{}
		}(i)
	}
	for i := 0; i < 10; i++ {
		<-done
	}
}

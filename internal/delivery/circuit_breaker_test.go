package delivery

import (
	"testing"
	"time"
)

func TestCircuitBreakerClosed(t *testing.T) {
	cb := NewCircuitBreaker()
	if cb.State() != CircuitClosed {
		t.Fatalf("expected closed, got %d", cb.State())
	}
	if !cb.CanTry() {
		t.Fatal("expected CanTry=true when closed")
	}
}

func TestCircuitBreakerTripAndRecover(t *testing.T) {
	cb := NewCircuitBreaker()

	// Record failures to trip (threshold 0.5, minAttempts 2, open for 100ms)
	cb.RecordFailure(0.5, 2, 100)
	if cb.State() != CircuitClosed {
		t.Fatal("should not trip after 1 failure (minAttempts=2)")
	}

	cb.RecordFailure(0.5, 2, 100)
	if cb.State() != CircuitOpen {
		t.Fatalf("expected open after 2 failures, got %d", cb.State())
	}
	if cb.CanTry() {
		t.Fatal("expected CanTry=false when open")
	}

	// Wait for open duration
	time.Sleep(150 * time.Millisecond)
	if !cb.CanTry() {
		t.Fatal("expected CanTry=true after open duration (half-open)")
	}
	if cb.State() != CircuitHalfOpen {
		t.Fatalf("expected half-open, got %d", cb.State())
	}

	// Success should close
	cb.RecordSuccess()
	if cb.State() != CircuitClosed {
		t.Fatalf("expected closed after success in half-open, got %d", cb.State())
	}
}

func TestCircuitBreakerHalfOpenFailsAgain(t *testing.T) {
	cb := NewCircuitBreaker()

	// Trip
	cb.RecordFailure(0.5, 1, 50)
	if cb.State() != CircuitOpen {
		t.Fatalf("expected open, got %d", cb.State())
	}

	// Wait
	time.Sleep(100 * time.Millisecond)
	cb.CanTry() // triggers half-open transition

	if cb.State() != CircuitHalfOpen {
		t.Fatalf("expected half-open, got %d", cb.State())
	}

	// Failure in half-open goes back to open
	cb.RecordFailure(0.5, 1, 50)
	if cb.State() != CircuitOpen {
		t.Fatalf("expected open after half-open failure, got %d", cb.State())
	}
}

func TestCircuitBreakerSuccessesPreventTrip(t *testing.T) {
	cb := NewCircuitBreaker()

	// 5 successes, then 1 failure — shouldn't trip at 50% threshold
	for i := 0; i < 5; i++ {
		cb.RecordSuccess()
	}
	cb.RecordFailure(0.5, 2, 100)
	if cb.State() != CircuitClosed {
		t.Fatalf("expected closed (1 failure / 6 total < 50%%), got %d", cb.State())
	}
}

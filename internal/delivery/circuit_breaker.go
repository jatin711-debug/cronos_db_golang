package delivery

import (
	"sync/atomic"
	"time"
)

// CircuitState is the finite state of a CircuitBreaker.
type CircuitState int32

const (
	// CircuitClosed allows all requests through (normal operation).
	CircuitClosed CircuitState = 0
	// CircuitOpen rejects requests until openUntilTS elapses.
	CircuitOpen CircuitState = 1
	// CircuitHalfOpen allows trial requests after an open period.
	CircuitHalfOpen CircuitState = 2
)

// CircuitBreaker implements per-subscription circuit breaking.
// When the failure rate exceeds a threshold after minAttempts, the circuit
// opens and fails fast until openDuration elapses, then transitions to
// half-open for a trial request.
type CircuitBreaker struct {
	state         atomic.Int32 // current CircuitState
	openUntilTS   atomic.Int64 // Unix ms — when to transition from Open to HalfOpen
	failures      atomic.Int64 // failure count in the current closed window
	successes     atomic.Int64 // success count in the current closed window
	lastFailureTS atomic.Int64 // Unix ms of last recorded failure
}

// NewCircuitBreaker creates a new circuit breaker.
func NewCircuitBreaker() *CircuitBreaker {
	cb := &CircuitBreaker{}
	cb.state.Store(int32(CircuitClosed))
	return cb
}

// State returns the current circuit state.
func (cb *CircuitBreaker) State() CircuitState {
	return CircuitState(cb.state.Load())
}

// CanTry returns true if the circuit allows a request through.
// Closed: always true. Open: false until openDuration expires. HalfOpen: true for trial.
func (cb *CircuitBreaker) CanTry() bool {
	state := cb.State()
	switch state {
	case CircuitClosed:
		return true
	case CircuitOpen:
		// Check if open duration has elapsed
		if time.Now().UnixMilli() >= cb.openUntilTS.Load() {
			// Transition to half-open
			if cb.state.CompareAndSwap(int32(CircuitOpen), int32(CircuitHalfOpen)) {
				cb.resetCounts()
			}
			return true
		}
		return false
	case CircuitHalfOpen:
		return true
	default:
		return true
	}
}

// RecordSuccess records a successful request.
func (cb *CircuitBreaker) RecordSuccess() {
	state := cb.State()
	switch state {
	case CircuitHalfOpen:
		// Trial succeeded — close the circuit
		cb.state.Store(int32(CircuitClosed))
		cb.resetCounts()
	case CircuitClosed:
		cb.successes.Add(1)
	}
}

// RecordFailure records a failed request and may trip the breaker.
// threshold: failure rate 0.0-1.0 to trip. minAttempts: minimum attempts before tripping.
// openDurationMs: how long to stay open.
func (cb *CircuitBreaker) RecordFailure(threshold float64, minAttempts int64, openDurationMs int64) {
	now := time.Now().UnixMilli()
	cb.lastFailureTS.Store(now)

	state := cb.State()
	switch state {
	case CircuitHalfOpen:
		// Trial failed — go back to open
		cb.openUntilTS.Store(now + openDurationMs)
		cb.state.Store(int32(CircuitOpen))
	case CircuitClosed:
		f := cb.failures.Add(1)
		s := cb.successes.Load()
		total := f + s
		if total >= minAttempts && float64(f)/float64(total) >= threshold {
			// Trip the breaker
			cb.openUntilTS.Store(now + openDurationMs)
			cb.state.Store(int32(CircuitOpen))
		}
	}
}

// resetCounts resets failure/success counters.
func (cb *CircuitBreaker) resetCounts() {
	cb.failures.Store(0)
	cb.successes.Store(0)
}

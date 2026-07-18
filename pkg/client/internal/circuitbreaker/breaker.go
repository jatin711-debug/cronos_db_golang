package circuitbreaker

import (
	"sync"
	"sync/atomic"
	"time"
)

// State represents the circuit breaker state.
type State int32

const (
	StateClosed   State = iota // Normal operation
	StateOpen                  // Failing, reject fast
	StateHalfOpen              // Testing if recovered
)

// String returns a stable name for the circuit state (closed/open/half-open).
func (s State) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateOpen:
		return "open"
	case StateHalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

// Config controls circuit breaker behavior.
// A FailureThreshold of 0 disables tripping (Allow always returns true).
type Config struct {
	// FailureThreshold is consecutive failures required to open the circuit.
	// 0 disables the breaker (never opens).
	FailureThreshold int
	// SuccessThreshold is consecutive successes in half-open required to close.
	SuccessThreshold int
	// Timeout is how long the breaker stays open before probing half-open.
	Timeout time.Duration
}

// DefaultConfig returns safe defaults.
func DefaultConfig() Config {
	return Config{
		FailureThreshold: 5,
		SuccessThreshold: 2,
		Timeout:          10 * time.Second,
	}
}

// CircuitBreaker is a per-target resilience gate (closed → open → half-open).
type CircuitBreaker struct {
	cfg Config // trip thresholds and open timeout

	state      atomic.Int32 // State enum stored as int32
	failures   atomic.Int32 // consecutive failures in closed/open evaluation
	successes  atomic.Int32 // consecutive successes while half-open
	lastFailAt atomic.Int64 // unix nano of last recorded failure
}

// New creates a circuit breaker.
func New(cfg Config) *CircuitBreaker {
	cb := &CircuitBreaker{cfg: cfg}
	cb.state.Store(int32(StateClosed))
	return cb
}

// Allow returns true if the request should proceed.
func (cb *CircuitBreaker) Allow() bool {
	switch State(cb.state.Load()) {
	case StateClosed:
		return true
	case StateOpen:
		lastFail := time.Unix(0, cb.lastFailAt.Load())
		if time.Since(lastFail) > cb.cfg.Timeout {
			// Try half-open
			if cb.state.CompareAndSwap(int32(StateOpen), int32(StateHalfOpen)) {
				cb.failures.Store(0)
				cb.successes.Store(0)
			}
			return true
		}
		return false
	case StateHalfOpen:
		return true
	default:
		return true
	}
}

// RecordSuccess marks a successful call.
func (cb *CircuitBreaker) RecordSuccess() {
	switch State(cb.state.Load()) {
	case StateHalfOpen:
		if cb.successes.Add(1) >= int32(cb.cfg.SuccessThreshold) {
			cb.state.Store(int32(StateClosed))
			cb.failures.Store(0)
			cb.successes.Store(0)
		}
	case StateClosed:
		cb.failures.Store(0)
	}
}

// RecordFailure marks a failed call.
func (cb *CircuitBreaker) RecordFailure() {
	// A non-positive FailureThreshold disables the breaker. Previously the trip
	// condition `failures >= FailureThreshold` was satisfied on the very first
	// failure when the threshold was 0 (the documented "disabled" default), so the
	// breaker opened immediately — the inverse of the intended behavior.
	if cb.cfg.FailureThreshold <= 0 {
		return
	}

	cb.lastFailAt.Store(time.Now().UnixNano())

	switch State(cb.state.Load()) {
	case StateHalfOpen:
		cb.state.Store(int32(StateOpen))
	case StateClosed:
		if cb.failures.Add(1) >= int32(cb.cfg.FailureThreshold) {
			cb.state.Store(int32(StateOpen))
		}
	}
}

// State returns current state (for observability).
func (cb *CircuitBreaker) CurrentState() State {
	return State(cb.state.Load())
}

// Manager holds per-address circuit breakers.
// Manager owns a map of per-address CircuitBreakers sharing one Config.
type Manager struct {
	mu       sync.RWMutex               // guards breakers map
	breakers map[string]*CircuitBreaker // address → breaker
	cfg      Config                     // template config for new breakers
}

// NewManager creates a breaker manager with empty per-address state.
func NewManager(cfg Config) *Manager {
	return &Manager{
		breakers: make(map[string]*CircuitBreaker),
		cfg:      cfg,
	}
}

// ForAddress returns the breaker for a target address.
func (m *Manager) ForAddress(addr string) *CircuitBreaker {
	m.mu.RLock()
	cb, ok := m.breakers[addr]
	m.mu.RUnlock()
	if ok {
		return cb
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	cb, ok = m.breakers[addr]
	if !ok {
		cb = New(m.cfg)
		m.breakers[addr] = cb
	}
	return cb
}

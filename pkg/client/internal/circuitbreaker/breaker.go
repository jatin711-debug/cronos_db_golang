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
type Config struct {
	FailureThreshold int           // Consecutive failures to trip
	SuccessThreshold int           // Successes in half-open to close
	Timeout          time.Duration // How long to stay open before half-open
}

// DefaultConfig returns safe defaults.
func DefaultConfig() Config {
	return Config{
		FailureThreshold: 5,
		SuccessThreshold: 2,
		Timeout:          10 * time.Second,
	}
}

// CircuitBreaker is a per-target resilience gate.
type CircuitBreaker struct {
	cfg Config

	state      atomic.Int32
	failures   atomic.Int32
	successes  atomic.Int32
	lastFailAt atomic.Int64 // unix nano
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
type Manager struct {
	mu       sync.RWMutex
	breakers map[string]*CircuitBreaker
	cfg      Config
}

// NewManager creates a breaker manager.
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

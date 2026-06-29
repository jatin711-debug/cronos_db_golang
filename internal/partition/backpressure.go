package partition

import (
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// TokenBucket implements a simple token bucket rate limiter.
type TokenBucket struct {
	tokens    atomic.Int64
	maxTokens int64
	refillRate int64     // tokens per second
	lastRefill atomic.Int64 // Unix nano timestamp
	mu         sync.Mutex
}

// NewTokenBucket creates a new token bucket.
func NewTokenBucket(maxTokens, refillRate int64) *TokenBucket {
	tb := &TokenBucket{
		maxTokens:  maxTokens,
		refillRate: refillRate,
	}
	tb.tokens.Store(maxTokens)
	tb.lastRefill.Store(time.Now().UnixNano())
	return tb
}

// TryConsume attempts to consume n tokens. Returns true if successful.
func (tb *TokenBucket) TryConsume(n int64) bool {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	// Refill tokens based on elapsed time
	now := time.Now().UnixNano()
	last := tb.lastRefill.Load()
	elapsed := now - last
	if elapsed > 0 {
		refill := (elapsed * tb.refillRate) / 1e9
		if refill > 0 {
			current := tb.tokens.Load()
			newTokens := current + refill
			if newTokens > tb.maxTokens {
				newTokens = tb.maxTokens
			}
			tb.tokens.Store(newTokens)
			tb.lastRefill.Store(now)
		}
	}

	// Try to consume
	current := tb.tokens.Load()
	if current < n {
		return false
	}
	tb.tokens.Store(current - n)
	return true
}

// MemoryMonitor tracks memory usage and provides backpressure signals.
type MemoryMonitor struct {
	maxPercent   float64
	checkInterval time.Duration
	lastCheck    atomic.Int64
	overLimit    atomic.Bool
}

// NewMemoryMonitor creates a memory monitor.
func NewMemoryMonitor(maxPercent float64, checkIntervalMs int64) *MemoryMonitor {
	if maxPercent <= 0 {
		return nil // Disabled
	}
	return &MemoryMonitor{
		maxPercent:    maxPercent,
		checkInterval: time.Duration(checkIntervalMs) * time.Millisecond,
	}
}

// IsOverLimit returns true if memory usage exceeds the threshold.
// Uses cached result to avoid frequent syscalls.
func (m *MemoryMonitor) IsOverLimit() bool {
	if m == nil {
		return false
	}

	now := time.Now().UnixMilli()
	last := m.lastCheck.Load()
	if now-last < m.checkInterval.Milliseconds() {
		return m.overLimit.Load()
	}

	// Update cache
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	// Calculate memory usage percentage
	// Use Sys as total allocated from OS (includes unused but reserved)
	usedPercent := (float64(memStats.Sys) / float64(memStats.HeapSys)) * 100
	if memStats.HeapSys == 0 {
		usedPercent = 0
	}

	over := usedPercent >= m.maxPercent
	m.overLimit.Store(over)
	m.lastCheck.Store(now)

	return over
}

// BackpressureManager combines rate limiting and memory monitoring for admission control.
type BackpressureManager struct {
	memoryMonitor *MemoryMonitor
	rateLimiters  map[int32]*TokenBucket
	mu            sync.RWMutex
}

// NewBackpressureManager creates a backpressure manager.
func NewBackpressureManager(maxMemoryPercent float64, memoryCheckIntervalMs int64) *BackpressureManager {
	return &BackpressureManager{
		memoryMonitor: NewMemoryMonitor(maxMemoryPercent, memoryCheckIntervalMs),
		rateLimiters:  make(map[int32]*TokenBucket),
	}
}

// SetRateLimiter sets a token bucket rate limiter for a partition.
func (bp *BackpressureManager) SetRateLimiter(partitionID int32, maxRate, burstSize int64) {
	if maxRate <= 0 || burstSize <= 0 {
		return // Disabled
	}
	bp.mu.Lock()
	defer bp.mu.Unlock()
	bp.rateLimiters[partitionID] = NewTokenBucket(burstSize, maxRate)
}

// CanAccept checks if a partition can accept new events considering all backpressure signals.
func (bp *BackpressureManager) CanAccept(partitionID int32) bool {
	// Check memory first (global backpressure)
	if bp.memoryMonitor != nil && bp.memoryMonitor.IsOverLimit() {
		return false
	}

	// Check rate limiter (per-partition backpressure)
	bp.mu.RLock()
	limiter, exists := bp.rateLimiters[partitionID]
	bp.mu.RUnlock()

	if exists && !limiter.TryConsume(1) {
		return false
	}

	return true
}

// GetMemoryUsage returns current memory usage percentage for metrics.
func (bp *BackpressureManager) GetMemoryUsage() float64 {
	if bp.memoryMonitor == nil {
		return 0
	}
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	if memStats.HeapSys == 0 {
		return 0
	}
	return (float64(memStats.Sys) / float64(memStats.HeapSys)) * 100
}

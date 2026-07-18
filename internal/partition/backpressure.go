package partition

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/shirou/gopsutil/v3/mem"
)

// TokenBucket implements a simple token bucket rate limiter for ingest admission.
type TokenBucket struct {
	tokens     atomic.Int64 // currently available tokens
	maxTokens  int64        // burst capacity (max tokens held)
	refillRate int64        // tokens added per second
	lastRefill atomic.Int64 // Unix nano of last refill
	mu         sync.Mutex
}

// NewTokenBucket creates a token bucket with the given burst capacity (maxTokens)
// and steady refill rate (tokens per second). Starts full.
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

// MemoryMonitor tracks OS-level memory usage and provides backpressure signals
// when usage exceeds a configured percentage threshold.
type MemoryMonitor struct {
	maxPercent    float64       // trip threshold as UsedPercent (0–100)
	checkInterval time.Duration // minimum time between syscalls
	lastCheck     atomic.Int64  // Unix ms of last VirtualMemory sample
	overLimit     atomic.Bool   // cached result of last check
}

// NewMemoryMonitor creates a memory monitor. maxPercent is the OS memory
// UsedPercent threshold (e.g. 85). checkIntervalMs is the cache TTL in
// milliseconds. Returns nil (disabled) when maxPercent <= 0.
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

	// Update cache using gopsutil for the actual OS-level memory usage. The
	// previous formula (Sys/HeapSys) was inverted and always >= 100%, which made
	// backpressure trip immediately with any reasonable threshold.
	usedPercent := 0.0
	if vm, err := mem.VirtualMemory(); err == nil {
		usedPercent = vm.UsedPercent
	}

	over := usedPercent >= m.maxPercent
	m.overLimit.Store(over)
	m.lastCheck.Store(now)

	return over
}

// BackpressureManager combines global memory monitoring and per-partition
// token-bucket rate limiting for publish admission control.
type BackpressureManager struct {
	memoryMonitor *MemoryMonitor         // nil when memory backpressure is disabled
	rateLimiters  map[int32]*TokenBucket // partitionID -> limiter
	mu            sync.RWMutex
}

// NewBackpressureManager creates a manager. maxMemoryPercent and
// memoryCheckIntervalMs configure the optional MemoryMonitor (disabled when
// maxMemoryPercent <= 0). Per-partition rate limiters are added via SetRateLimiter.
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
	return bp.CanAcceptN(partitionID, 1)
}

// CanAcceptN checks if a partition can accept count events. Batch admission
// consumes the full rate-limit cost so batching cannot bypass backpressure.
func (bp *BackpressureManager) CanAcceptN(partitionID int32, count int64) bool {
	if count <= 0 {
		return true
	}
	// Check memory first (global backpressure)
	if bp.memoryMonitor != nil && bp.memoryMonitor.IsOverLimit() {
		return false
	}

	// Check rate limiter (per-partition backpressure)
	bp.mu.RLock()
	limiter, exists := bp.rateLimiters[partitionID]
	bp.mu.RUnlock()

	if exists && !limiter.TryConsume(count) {
		return false
	}

	return true
}

// GetMemoryUsage returns current memory usage percentage for metrics.
func (bp *BackpressureManager) GetMemoryUsage() float64 {
	if bp.memoryMonitor == nil {
		return 0
	}
	vm, err := mem.VirtualMemory()
	if err != nil {
		return 0
	}
	return vm.UsedPercent
}

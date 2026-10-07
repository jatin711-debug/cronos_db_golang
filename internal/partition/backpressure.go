package partition

import (
	"log"
	"os"
	"runtime"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/internal/sysmem"
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

// MemoryMonitor says when this process holds too large a share of the memory
// it may use, so that publishes can be refused while there is still room for
// what the node already took in. A node that runs out is killed, and comes
// back to the same backlog.
//
// It compares what the process holds with the process's own limit: the
// container's, when there is one. It used to compare the memory in use on the
// whole machine with a percentage, which says nothing inside a container, and
// it was off unless asked for.
type MemoryMonitor struct {
	maxPercent    float64       // share of limit, 0-100, at which publishes are refused
	limit         uint64        // bytes the process may use
	checkInterval time.Duration // how long a measurement is used
	measure       func() sysmem.Sample
	// setRuntimeLimit tells the Go runtime how much memory it may use before
	// it collects; nil when that is not this monitor's to decide.
	setRuntimeLimit func(bytes int64)
	runtimeLimit    int64 // what it was last told

	lastCheck   atomic.Int64 // Unix ms of the last measurement
	lastCollect atomic.Int64 // Unix ms of the last collection run for a measurement
	overLimit   atomic.Bool  // result of the last measurement
	lastUsed    atomic.Uint64
	measuring   sync.Mutex
}

// collectInterval is the least time between garbage collections that the
// monitor runs to see what the process holds without its garbage.
const collectInterval = 10 * time.Second

// NewMemoryMonitor creates a memory monitor. maxPercent is the share of the
// memory limit (e.g. 80) at which publishes are refused, checkIntervalMs how
// long a measurement is used, and limitBytes the limit when the operator sets
// one; with 0 it is the container's limit, or the machine's memory. It
// returns nil (disabled) when maxPercent <= 0 or no limit can be found.
func NewMemoryMonitor(maxPercent float64, checkIntervalMs int64, limitBytes uint64) *MemoryMonitor {
	if maxPercent <= 0 {
		return nil // Disabled
	}
	limit, source := sysmem.Limit(limitBytes)
	if limit == 0 {
		log.Printf("[BACKPRESSURE] No memory limit can be determined; publishes are not refused for memory. Set --memory-limit")
		return nil
	}
	log.Printf("[BACKPRESSURE] Publishes are refused while this process holds %.0f%% or more of its memory limit of %d MiB (%s)",
		maxPercent, limit>>20, source)
	m := &MemoryMonitor{
		maxPercent:    maxPercent,
		limit:         limit,
		checkInterval: time.Duration(checkIntervalMs) * time.Millisecond,
		measure:       sysmem.Measure,
	}
	if os.Getenv("GOMEMLIMIT") == "" {
		m.setRuntimeLimit = func(bytes int64) { debug.SetMemoryLimit(bytes) }
	}
	// From the start, not from the first publish: a node that only follows
	// takes none and still fills its heap.
	m.paceRuntime(uint64(float64(limit)*maxPercent/100), m.measure())
	return m
}

// runtimeShare is the part of the room below the threshold that the Go
// runtime may fill before it collects. The rest is slack: the runtime
// overshoots its limit a little, and garbage must not reach the threshold.
const runtimeShare = 0.9

// minRuntimeLimit keeps the runtime's limit from being set so low that it
// does nothing but collect.
const minRuntimeLimit = 64 << 20

// paceRuntime tells the Go runtime to collect before the process reaches the
// threshold. Left alone, the runtime lets the heap grow to twice what is live
// and knows nothing of a container's limit, so a node that held little would
// cross the threshold on garbage and refuse publishes it had room for.
func (m *MemoryMonitor) paceRuntime(threshold uint64, sample sysmem.Sample) {
	if m.setRuntimeLimit == nil {
		return
	}
	target := int64(float64(threshold)*runtimeShare) - int64(sample.Outside)
	target = max(target, minRuntimeLimit)
	if change := target - m.runtimeLimit; change > target/20 || change < -target/20 {
		m.runtimeLimit = target
		m.setRuntimeLimit(target)
	}
}

// IsOverLimit returns true if the process holds more than the threshold. A
// measurement is used for the check interval.
func (m *MemoryMonitor) IsOverLimit() bool {
	if m == nil {
		return false
	}

	now := time.Now().UnixMilli()
	if now-m.lastCheck.Load() < m.checkInterval.Milliseconds() || !m.measuring.TryLock() {
		return m.overLimit.Load()
	}
	defer m.measuring.Unlock()

	threshold := uint64(float64(m.limit) * m.maxPercent / 100)
	sample := m.measure()
	m.paceRuntime(threshold, sample)
	used := sample.Held
	if used >= threshold && now-m.lastCollect.Load() >= collectInterval.Milliseconds() {
		// What is above the threshold may still be garbage the collector has
		// not got to. It is collected before publishes are refused for it,
		// at most every few seconds.
		m.lastCollect.Store(now)
		runtime.GC()
		used = m.measure().Held
	}

	over := used >= threshold
	if was := m.overLimit.Swap(over); was != over {
		if over {
			log.Printf("[BACKPRESSURE] This process holds %d MiB of its %d MiB memory limit; publishes are refused until it holds less than %.0f%%",
				used>>20, m.limit>>20, m.maxPercent)
		} else {
			log.Printf("[BACKPRESSURE] This process holds %d MiB of its %d MiB memory limit; publishes are accepted again", used>>20, m.limit>>20)
		}
	}
	m.lastUsed.Store(used)
	m.lastCheck.Store(now)
	return over
}

// Usage returns what the process held at the last measurement and its limit,
// in bytes.
func (m *MemoryMonitor) Usage() (used, limit uint64) {
	if m == nil {
		return 0, 0
	}
	return m.lastUsed.Load(), m.limit
}

// BackpressureManager combines global memory monitoring and per-partition
// token-bucket rate limiting for publish admission control.
type BackpressureManager struct {
	memoryMonitor *MemoryMonitor         // nil when memory backpressure is disabled
	rateLimiters  map[int32]*TokenBucket // partitionID -> limiter
	mu            sync.RWMutex
}

// NewBackpressureManager creates a manager. maxMemoryPercent,
// memoryCheckIntervalMs and memoryLimitBytes configure the optional
// MemoryMonitor (disabled when maxMemoryPercent <= 0). Per-partition rate
// limiters are added via SetRateLimiter.
func NewBackpressureManager(maxMemoryPercent float64, memoryCheckIntervalMs int64, memoryLimitBytes uint64) *BackpressureManager {
	return &BackpressureManager{
		memoryMonitor: NewMemoryMonitor(maxMemoryPercent, memoryCheckIntervalMs, memoryLimitBytes),
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

// MemoryUsage returns what the process held when publishes last asked, and
// the limit that is compared with, in bytes. Both are 0 when publishes are
// not refused for memory.
func (bp *BackpressureManager) MemoryUsage() (used, limit uint64) {
	return bp.memoryMonitor.Usage()
}

package tenant

import (
	"sync"
	"sync/atomic"
	"time"
)

// ID identifies a tenant.
type ID string

// Limits defines resource caps for a tenant.
type Limits struct {
	MaxEventsPerSecond float64
	MaxInFlight        int64
	MaxStorageBytes    int64
}

// tokenBucket implements a classic token bucket for rate limiting.
type tokenBucket struct {
	rate     float64   // tokens per second
	capacity float64   // max tokens
	tokens   float64   // current tokens
	last     time.Time // last update time
	mu       sync.Mutex
}

func newTokenBucket(rate, capacity float64) *tokenBucket {
	return &tokenBucket{
		rate:     rate,
		capacity: capacity,
		tokens:   capacity,
		last:     time.Now(),
	}
}

// tryConsume attempts to consume n tokens. Returns true if allowed.
func (tb *tokenBucket) tryConsume(n float64) bool {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(tb.last).Seconds()
	tb.tokens = min(tb.capacity, tb.tokens+elapsed*tb.rate)
	tb.last = now

	if tb.tokens >= n {
		tb.tokens -= n
		return true
	}
	return false
}

// Accountant tracks per-tenant resource usage.
type Accountant struct {
	mu      sync.RWMutex
	limits  map[ID]Limits
	usage   map[ID]*Usage
	buckets map[ID]*tokenBucket
}

// Usage tracks current consumption.
type Usage struct {
	InFlight     atomic.Int64
	StorageBytes atomic.Int64
}

// NewAccountant creates a tenant resource accountant.
func NewAccountant() *Accountant {
	return &Accountant{
		limits:  make(map[ID]Limits),
		usage:   make(map[ID]*Usage),
		buckets: make(map[ID]*tokenBucket),
	}
}

// SetLimits configures limits for a tenant.
func (a *Accountant) SetLimits(tenant ID, limits Limits) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.limits[tenant] = limits
	if _, ok := a.usage[tenant]; !ok {
		a.usage[tenant] = &Usage{}
	}
	if limits.MaxEventsPerSecond > 0 {
		a.buckets[tenant] = newTokenBucket(limits.MaxEventsPerSecond, limits.MaxEventsPerSecond)
	} else {
		delete(a.buckets, tenant)
	}
}

// AllowPublish checks if a tenant can publish.
func (a *Accountant) AllowPublish(tenant ID) bool {
	a.mu.RLock()
	limits, ok := a.limits[tenant]
	usage, hasUsage := a.usage[tenant]
	bucket, hasBucket := a.buckets[tenant]
	a.mu.RUnlock()

	if !ok {
		return true // No limits = allow
	}
	if !hasUsage {
		return true
	}

	if hasBucket {
		if !bucket.tryConsume(1) {
			return false
		}
	}
	if limits.MaxInFlight > 0 {
		if usage.InFlight.Load() >= limits.MaxInFlight {
			// Refund the token since we're rejecting
			if hasBucket {
				bucket.tryConsume(-1) // refund
			}
			return false
		}
	}
	if limits.MaxStorageBytes > 0 {
		if usage.StorageBytes.Load() >= limits.MaxStorageBytes {
			// Refund the token since we're rejecting
			if hasBucket {
				bucket.tryConsume(-1) // refund
			}
			return false
		}
	}
	return true
}

// RecordPublish records a publish attempt.
func (a *Accountant) RecordPublish(tenant ID, bytes int64) {
	a.mu.RLock()
	usage, ok := a.usage[tenant]
	a.mu.RUnlock()
	if !ok {
		a.mu.Lock()
		usage = &Usage{}
		a.usage[tenant] = usage
		a.mu.Unlock()
	}
	usage.InFlight.Add(1)
	usage.StorageBytes.Add(bytes)
}

// RecordDelivery records a delivery completion.
func (a *Accountant) RecordDelivery(tenant ID) {
	a.mu.RLock()
	usage, ok := a.usage[tenant]
	a.mu.RUnlock()
	if ok {
		usage.InFlight.Add(-1)
	}
}

// GetUsage returns current usage for a tenant.
func (a *Accountant) GetUsage(tenant ID) (inFlight int64, storageBytes int64) {
	a.mu.RLock()
	usage, ok := a.usage[tenant]
	a.mu.RUnlock()
	if !ok {
		return 0, 0
	}
	return usage.InFlight.Load(), usage.StorageBytes.Load()
}

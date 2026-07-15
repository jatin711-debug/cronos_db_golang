package tenant

import (
	"sort"
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

// refund restores n tokens back to the bucket, ensuring it doesn't exceed capacity.
func (tb *tokenBucket) refund(n float64) {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(tb.last).Seconds()
	tb.tokens = min(tb.capacity, tb.tokens+elapsed*tb.rate)
	tb.last = now

	tb.tokens = min(tb.capacity, tb.tokens+n)
}

// Accountant tracks per-tenant resource usage.
type Accountant struct {
	mu         sync.RWMutex
	limits     map[ID]Limits
	usage      map[ID]*Usage
	buckets    map[ID]*tokenBucket
	configured atomic.Bool
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

// HasConfiguredLimits reports whether any tenant has an explicit limit. The
// API hot path uses this to avoid per-event accounting when the accountant is
// installed only for delivery callbacks and no quotas are configured.
func (a *Accountant) HasConfiguredLimits() bool {
	return a.configured.Load()
}

// SetLimits configures limits for a tenant.
func (a *Accountant) SetLimits(tenant ID, limits Limits) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.limits[tenant] = limits
	a.configured.Store(true)
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
	return a.AllowPublishBatch(tenant, 1, 0)
}

// AllowPublishBatch checks a whole publish batch in one accountant operation.
// Rate-limit tokens are consumed for the complete batch and refunded if any
// quota rejects it. Usage counters are checked using the batch cardinality and
// byte total instead of performing one map lookup per event.
func (a *Accountant) AllowPublishBatch(tenant ID, eventCount, bytes int64) bool {
	if eventCount <= 0 {
		return true
	}

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
		if !bucket.tryConsume(float64(eventCount)) {
			return false
		}
	}
	if limits.MaxInFlight > 0 {
		if usage.InFlight.Load()+eventCount > limits.MaxInFlight {
			// Refund the token since we're rejecting
			if hasBucket {
				bucket.refund(float64(eventCount))
			}
			return false
		}
	}
	if limits.MaxStorageBytes > 0 {
		if usage.StorageBytes.Load()+bytes > limits.MaxStorageBytes {
			// Refund the token since we're rejecting
			if hasBucket {
				bucket.refund(float64(eventCount))
			}
			return false
		}
	}
	return true
}

// ReservePublishBatch atomically admits and accounts for a publish batch. It
// is the safe operation for concurrent producers: unlike AllowPublishBatch
// followed by RecordPublishBatch, two callers cannot both pass a MaxInFlight
// or MaxStorageBytes check before recording their usage.
func (a *Accountant) ReservePublishBatch(tenant ID, eventCount, bytes int64) bool {
	if eventCount <= 0 {
		return true
	}

	a.mu.RLock()
	limits, ok := a.limits[tenant]
	usage, hasUsage := a.usage[tenant]
	bucket, hasBucket := a.buckets[tenant]
	a.mu.RUnlock()

	if !ok || !hasUsage {
		return true
	}

	refund := func() {
		if hasBucket {
			bucket.refund(float64(eventCount))
		}
	}
	if hasBucket && !bucket.tryConsume(float64(eventCount)) {
		return false
	}

	if limits.MaxInFlight > 0 {
		for {
			current := usage.InFlight.Load()
			if current > limits.MaxInFlight || eventCount > limits.MaxInFlight-current {
				refund()
				return false
			}
			if usage.InFlight.CompareAndSwap(current, current+eventCount) {
				break
			}
		}
	} else {
		usage.InFlight.Add(eventCount)
	}

	if limits.MaxStorageBytes > 0 {
		for {
			current := usage.StorageBytes.Load()
			if current > limits.MaxStorageBytes || bytes > limits.MaxStorageBytes-current {
				usage.InFlight.Add(-eventCount)
				refund()
				return false
			}
			if usage.StorageBytes.CompareAndSwap(current, current+bytes) {
				break
			}
		}
	} else {
		usage.StorageBytes.Add(bytes)
	}

	return true
}

// RecordPublish records a publish attempt.
func (a *Accountant) RecordPublish(tenant ID, bytes int64) {
	a.RecordPublishBatch(tenant, 1, bytes)
}

// RecordPublishBatch records a complete publish batch with one map operation
// and two atomic increments.
func (a *Accountant) RecordPublishBatch(tenant ID, eventCount, bytes int64) {
	if eventCount <= 0 {
		return
	}
	a.mu.RLock()
	usage, ok := a.usage[tenant]
	a.mu.RUnlock()
	if !ok {
		a.mu.Lock()
		usage, ok = a.usage[tenant]
		if !ok {
			usage = &Usage{}
			a.usage[tenant] = usage
		}
		a.mu.Unlock()
	}
	usage.InFlight.Add(eventCount)
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

// GetLimits returns the configured limits for a tenant and whether any limits
// were configured. A nil/empty limits map returns (Limits{}, false).
func (a *Accountant) GetLimits(tenant ID) (Limits, bool) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	limits, ok := a.limits[tenant]
	return limits, ok
}

// AllTenants returns every tenant ID that has limits or usage recorded.
func (a *Accountant) AllTenants() []ID {
	a.mu.RLock()
	defer a.mu.RUnlock()

	seen := make(map[ID]struct{}, len(a.limits)+len(a.usage))
	for id := range a.limits {
		seen[id] = struct{}{}
	}
	for id := range a.usage {
		seen[id] = struct{}{}
	}

	tenants := make([]ID, 0, len(seen))
	for id := range seen {
		tenants = append(tenants, id)
	}
	sort.Slice(tenants, func(i, j int) bool { return tenants[i] < tenants[j] })
	return tenants
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

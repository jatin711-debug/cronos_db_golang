package tenant

import (
	"sync"
	"sync/atomic"
)

// ID identifies a tenant.
type ID string

// Limits defines resource caps for a tenant.
type Limits struct {
	MaxEventsPerSecond float64
	MaxInFlight        int64
	MaxStorageBytes    int64
}

// Accountant tracks per-tenant resource usage.
type Accountant struct {
	mu      sync.RWMutex
	limits  map[ID]Limits
	usage   map[ID]*Usage
}

// Usage tracks current consumption.
type Usage struct {
	EventsPerSecond atomic.Int64
	InFlight        atomic.Int64
	StorageBytes    atomic.Int64
}

// NewAccountant creates a tenant resource accountant.
func NewAccountant() *Accountant {
	return &Accountant{
		limits: make(map[ID]Limits),
		usage:  make(map[ID]*Usage),
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
}

// AllowPublish checks if a tenant can publish.
func (a *Accountant) AllowPublish(tenant ID) bool {
	a.mu.RLock()
	limits, ok := a.limits[tenant]
	usage, hasUsage := a.usage[tenant]
	a.mu.RUnlock()

	if !ok {
		return true // No limits = allow
	}
	if !hasUsage {
		return true
	}

	if limits.MaxEventsPerSecond > 0 {
		if float64(usage.EventsPerSecond.Load()) >= limits.MaxEventsPerSecond {
			return false
		}
	}
	if limits.MaxInFlight > 0 {
		if usage.InFlight.Load() >= limits.MaxInFlight {
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
	usage.EventsPerSecond.Add(1)
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

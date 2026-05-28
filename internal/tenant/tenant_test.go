package tenant

import (
	"testing"
)

func TestNewAccountant(t *testing.T) {
	a := NewAccountant()
	if a == nil {
		t.Fatal("NewAccountant should not return nil")
	}
	if a.limits == nil {
		t.Fatal("limits map should be initialized")
	}
	if a.usage == nil {
		t.Fatal("usage map should be initialized")
	}
}

func TestAccountant_SetLimits(t *testing.T) {
	a := NewAccountant()
	a.SetLimits("tenant-1", Limits{
		MaxEventsPerSecond: 100,
		MaxInFlight:        50,
		MaxStorageBytes:    1024 * 1024,
	})

	a.mu.RLock()
	limits, ok := a.limits["tenant-1"]
	a.mu.RUnlock()

	if !ok {
		t.Fatal("limits should be set")
	}
	if limits.MaxEventsPerSecond != 100 {
		t.Errorf("expected MaxEventsPerSecond 100, got %f", limits.MaxEventsPerSecond)
	}
}

func TestAccountant_SetLimits_CreatesUsage(t *testing.T) {
	a := NewAccountant()
	a.SetLimits("tenant-1", Limits{MaxEventsPerSecond: 100})

	a.mu.RLock()
	usage, ok := a.usage["tenant-1"]
	a.mu.RUnlock()

	if !ok {
		t.Fatal("usage should be created")
	}
	if usage == nil {
		t.Fatal("usage should not be nil")
	}
}

func TestAccountant_AllowPublish_NoLimits(t *testing.T) {
	a := NewAccountant()
	if !a.AllowPublish("unknown-tenant") {
		t.Error("should allow publish when no limits set")
	}
}

func TestAccountant_AllowPublish_WithinLimits(t *testing.T) {
	a := NewAccountant()
	a.SetLimits("tenant-1", Limits{MaxEventsPerSecond: 100})

	if !a.AllowPublish("tenant-1") {
		t.Error("should allow publish within limits")
	}
}

func TestAccountant_AllowPublish_EventsPerSecondExceeded(t *testing.T) {
	a := NewAccountant()
	a.SetLimits("tenant-1", Limits{MaxEventsPerSecond: 5})

	// Simulate 5 events already recorded
	for i := 0; i < 5; i++ {
		a.RecordPublish("tenant-1", 100)
	}

	if a.AllowPublish("tenant-1") {
		t.Error("should deny publish when events per second exceeded")
	}
}

func TestAccountant_AllowPublish_InFlightExceeded(t *testing.T) {
	a := NewAccountant()
	a.SetLimits("tenant-1", Limits{MaxInFlight: 2})

	// Simulate 2 in-flight
	for i := 0; i < 2; i++ {
		a.RecordPublish("tenant-1", 100)
	}

	if a.AllowPublish("tenant-1") {
		t.Error("should deny publish when in-flight exceeded")
	}
}

func TestAccountant_RecordPublish(t *testing.T) {
	a := NewAccountant()
	a.RecordPublish("tenant-1", 1024)

	usage := a.usage["tenant-1"]
	if usage == nil {
		t.Fatal("usage should be created")
	}
	if usage.EventsPerSecond.Load() != 1 {
		t.Errorf("expected 1 event, got %d", usage.EventsPerSecond.Load())
	}
	if usage.InFlight.Load() != 1 {
		t.Errorf("expected 1 in-flight, got %d", usage.InFlight.Load())
	}
	if usage.StorageBytes.Load() != 1024 {
		t.Errorf("expected 1024 bytes, got %d", usage.StorageBytes.Load())
	}
}

func TestAccountant_RecordPublish_CreatesUsage(t *testing.T) {
	a := NewAccountant()
	a.RecordPublish("new-tenant", 100)

	if _, ok := a.usage["new-tenant"]; !ok {
		t.Error("usage should be created for new tenant")
	}
}

func TestAccountant_RecordDelivery(t *testing.T) {
	a := NewAccountant()
	a.SetLimits("tenant-1", Limits{})
	a.RecordPublish("tenant-1", 100)

	if a.usage["tenant-1"].InFlight.Load() != 1 {
		t.Fatal("expected 1 in-flight")
	}

	a.RecordDelivery("tenant-1")

	if a.usage["tenant-1"].InFlight.Load() != 0 {
		t.Errorf("expected 0 in-flight after delivery, got %d", a.usage["tenant-1"].InFlight.Load())
	}
}

func TestAccountant_RecordDelivery_UnknownTenant(t *testing.T) {
	a := NewAccountant()
	// Should not panic
	a.RecordDelivery("unknown-tenant")
}

func TestAccountant_RecordDelivery_NegativeInFlight(t *testing.T) {
	a := NewAccountant()
	a.SetLimits("tenant-1", Limits{})

	// Record delivery without publish — should go negative (as designed)
	a.RecordDelivery("tenant-1")
	if a.usage["tenant-1"].InFlight.Load() != -1 {
		t.Errorf("expected -1 in-flight, got %d", a.usage["tenant-1"].InFlight.Load())
	}
}

func TestAccountant_Concurrent(t *testing.T) {
	a := NewAccountant()
	a.SetLimits("tenant-1", Limits{MaxEventsPerSecond: 10000, MaxInFlight: 10000})

	done := make(chan struct{}, 10)
	for i := 0; i < 10; i++ {
		go func() {
			for j := 0; j < 100; j++ {
				a.RecordPublish("tenant-1", 100)
			}
			done <- struct{}{}
		}()
	}

	for i := 0; i < 10; i++ {
		<-done
	}

	if a.usage["tenant-1"].EventsPerSecond.Load() != 1000 {
		t.Errorf("expected 1000 events, got %d", a.usage["tenant-1"].EventsPerSecond.Load())
	}
	if a.usage["tenant-1"].InFlight.Load() != 1000 {
		t.Errorf("expected 1000 in-flight, got %d", a.usage["tenant-1"].InFlight.Load())
	}
}

func TestID_Type(t *testing.T) {
	var id ID = "tenant-abc"
	if string(id) != "tenant-abc" {
		t.Error("ID mismatch")
	}
}

func TestLimits_Fields(t *testing.T) {
	l := Limits{
		MaxEventsPerSecond: 100,
		MaxInFlight:        50,
		MaxStorageBytes:    1024,
	}
	if l.MaxEventsPerSecond != 100 {
		t.Error("MaxEventsPerSecond mismatch")
	}
	if l.MaxInFlight != 50 {
		t.Error("MaxInFlight mismatch")
	}
	if l.MaxStorageBytes != 1024 {
		t.Error("MaxStorageBytes mismatch")
	}
}

func TestUsage_Fields(t *testing.T) {
	u := &Usage{}
	u.EventsPerSecond.Store(10)
	u.InFlight.Store(5)
	u.StorageBytes.Store(1024)

	if u.EventsPerSecond.Load() != 10 {
		t.Error("EventsPerSecond mismatch")
	}
	if u.InFlight.Load() != 5 {
		t.Error("InFlight mismatch")
	}
	if u.StorageBytes.Load() != 1024 {
		t.Error("StorageBytes mismatch")
	}
}

package tenant

import (
	"testing"
	"time"
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
	if a.buckets == nil {
		t.Fatal("buckets map should be initialized")
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

func TestAccountant_AllowPublish_RateExceeded(t *testing.T) {
	a := NewAccountant()
	a.SetLimits("tenant-1", Limits{MaxEventsPerSecond: 2})

	// Consume both tokens immediately
	if !a.AllowPublish("tenant-1") {
		t.Error("should allow first publish")
	}
	if !a.AllowPublish("tenant-1") {
		t.Error("should allow second publish")
	}
	if a.AllowPublish("tenant-1") {
		t.Error("should deny publish when rate exceeded")
	}

	// Wait for token refill
	time.Sleep(600 * time.Millisecond)
	if !a.AllowPublish("tenant-1") {
		t.Error("should allow after token refill")
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

func TestAccountant_AllowPublish_StorageExceeded(t *testing.T) {
	a := NewAccountant()
	a.SetLimits("tenant-1", Limits{MaxStorageBytes: 100})

	a.RecordPublish("tenant-1", 200)

	if a.AllowPublish("tenant-1") {
		t.Error("should deny publish when storage exceeded")
	}
}

func TestAccountant_RecordPublish(t *testing.T) {
	a := NewAccountant()
	a.RecordPublish("tenant-1", 1024)

	usage := a.usage["tenant-1"]
	if usage == nil {
		t.Fatal("usage should be created")
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

	if a.usage["tenant-1"].InFlight.Load() != 1000 {
		t.Errorf("expected 1000 in-flight, got %d", a.usage["tenant-1"].InFlight.Load())
	}
}

func TestTokenBucket_Refill(t *testing.T) {
	tb := newTokenBucket(10, 10) // 10/sec, capacity 10

	// Consume all 10 tokens
	for i := 0; i < 10; i++ {
		if !tb.tryConsume(1) {
			t.Fatalf("should consume token %d", i)
		}
	}
	if tb.tryConsume(1) {
		t.Error("should deny when empty")
	}

	// Wait for 0.5 seconds = 5 tokens
	time.Sleep(550 * time.Millisecond)
	for i := 0; i < 5; i++ {
		if !tb.tryConsume(1) {
			t.Fatalf("should consume refilled token %d", i)
		}
	}
	if tb.tryConsume(1) {
		t.Error("should deny after refilled tokens consumed")
	}
}

func TestTokenBucket_Capacity(t *testing.T) {
	tb := newTokenBucket(1, 2) // 1/sec, capacity 2

	// Wait 10 seconds — should cap at 2, not accumulate 10+
	time.Sleep(2 * time.Second)
	if !tb.tryConsume(1) {
		t.Error("should have 1 token after cap")
	}
	if !tb.tryConsume(1) {
		t.Error("should have 2nd token after cap")
	}
	if tb.tryConsume(1) {
		t.Error("should be empty after 2 consumes")
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

func TestTokenBucket_RefundCapacity(t *testing.T) {
	tb := newTokenBucket(10, 5) // 10/sec, capacity 5
	// Initial state: pre-filled to 5

	// Refund when already full - should stay at 5
	tb.refund(1)
	tb.mu.Lock()
	tokens := tb.tokens
	tb.mu.Unlock()
	if tokens > 5 {
		t.Errorf("expected tokens capped at 5, got %f", tokens)
	}

	// Consume 3 tokens -> now 2
	if !tb.tryConsume(3) {
		t.Fatal("should consume 3 tokens")
	}

	// Refund 2 tokens -> now 4
	tb.refund(2)
	tb.mu.Lock()
	tokens = tb.tokens
	tb.mu.Unlock()
	if tokens > 4.0001 || tokens < 3.9999 {
		t.Errorf("expected tokens close to 4, got %f", tokens)
	}

	// Refund another 3 tokens -> should cap at 5, not go to 7
	tb.refund(3)
	tb.mu.Lock()
	tokens = tb.tokens
	tb.mu.Unlock()
	if tokens > 5 {
		t.Errorf("expected tokens capped at 5 after over-refund, got %f", tokens)
	}
}

func TestAccountant_AllowPublish_Refund(t *testing.T) {
	a := NewAccountant()
	a.SetLimits("tenant-1", Limits{
		MaxEventsPerSecond: 10,
		MaxInFlight:        2,
	})

	// Fill in-flight to max (2)
	a.RecordPublish("tenant-1", 100)
	a.RecordPublish("tenant-1", 100)

	// Next AllowPublish should be rejected due to in-flight limits,
	// but the rate-limiting token must be refunded so that rate-limiting capacity remains at capacity
	if a.AllowPublish("tenant-1") {
		t.Fatal("should deny publish when in-flight exceeded")
	}

	// Wait a tiny bit, and verify the bucket has not lost a token (or that we can still consume up to capacity once in-flight drops)
	a.RecordDelivery("tenant-1") // drop in-flight to 1
	a.RecordDelivery("tenant-1") // drop in-flight to 0

	// We should be able to consume up to 10 publishes immediately if no tokens were lost due to rejected publishes!
	for i := 0; i < 10; i++ {
		if !a.AllowPublish("tenant-1") {
			t.Fatalf("should allow publish %d", i)
		}
	}
}

func TestAccountant_ReservePublishBatch_IsAtomic(t *testing.T) {
	a := NewAccountant()
	a.SetLimits("tenant-1", Limits{MaxInFlight: 4, MaxStorageBytes: 400})

	const workers = 32
	results := make(chan bool, workers)
	for i := 0; i < workers; i++ {
		go func() {
			results <- a.ReservePublishBatch("tenant-1", 1, 100)
		}()
	}

	accepted := 0
	for i := 0; i < workers; i++ {
		if <-results {
			accepted++
		}
	}
	if accepted != 4 {
		t.Fatalf("expected exactly 4 reservations, got %d", accepted)
	}
	if inFlight, bytes := a.GetUsage("tenant-1"); inFlight != 4 || bytes != 400 {
		t.Fatalf("unexpected reserved usage: inFlight=%d bytes=%d", inFlight, bytes)
	}
}

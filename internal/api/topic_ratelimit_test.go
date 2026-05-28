package api

import (
	"context"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

func TestTopicRateLimiter_Allow(t *testing.T) {
	rl := NewTopicRateLimiter(100.0, 50.0) // 100 rps, burst 50

	// Should allow up to burst
	for i := range 50 {
		if !rl.allow("subject1", "topic1", 1) {
			t.Errorf("request %d should be allowed", i)
		}
	}

	// Next request should be rate limited
	if rl.allow("subject1", "topic1", 1) {
		t.Error("request should be rate limited after burst exhausted")
	}
}

func TestTopicRateLimiter_Refill(t *testing.T) {
	rl := NewTopicRateLimiter(100.0, 10.0) // 100 rps, burst 10

	// Exhaust burst
	for range 10 {
		rl.allow("subject1", "topic1", 1)
	}

	// Should be limited now
	if rl.allow("subject1", "topic1", 1) {
		t.Error("should be rate limited immediately after burst")
	}

	// Wait for refill (100 rps = ~10ms per token, 10 tokens = ~100ms for full refill)
	time.Sleep(150 * time.Millisecond)

	// Should have refilled
	if !rl.allow("subject1", "topic1", 1) {
		t.Error("should be allowed after refill")
	}
}

func TestTopicRateLimiter_MultiTopic(t *testing.T) {
	rl := NewTopicRateLimiter(100.0, 10.0)

	// Exhaust topic1
	for i := 0; i < 10; i++ {
		rl.allow("subject1", "topic1", 1)
	}

	// topic2 should still work
	if !rl.allow("subject1", "topic2", 1) {
		t.Error("topic2 should not be affected by topic1 exhaustion")
	}
}

func TestTopicRateLimiter_MultiSubject(t *testing.T) {
	rl := NewTopicRateLimiter(100.0, 10.0)

	// Exhaust subject1
	for range 10 {
		rl.allow("subject1", "topic1", 1)
	}

	// subject2 should still work
	if !rl.allow("subject2", "topic1", 1) {
		t.Error("subject2 should not be affected by subject1 exhaustion")
	}
}

func TestTopicRateLimiter_Batch(t *testing.T) {
	rl := NewTopicRateLimiter(100.0, 10.0)

	// Single batch of 10 should exhaust
	if !rl.allow("subject1", "topic1", 10) {
		t.Error("batch of 10 should be allowed")
	}

	// Another single should be limited
	if rl.allow("subject1", "topic1", 1) {
		t.Error("should be limited after batch exhaustion")
	}
}

func TestTopicRateLimiter_BatchPartial(t *testing.T) {
	rl := NewTopicRateLimiter(100.0, 10.0)

	// 5 should leave room for 5 more
	if !rl.allow("subject1", "topic1", 5) {
		t.Error("batch of 5 should be allowed")
	}

	// Another 5 should also work
	if !rl.allow("subject1", "topic1", 5) {
		t.Error("batch of 5 should be allowed")
	}

	// Now exhausted
	if rl.allow("subject1", "topic1", 1) {
		t.Error("should be limited")
	}
}

func TestTopicRateLimiter_Cleanup(t *testing.T) {
	rl := NewTopicRateLimiter(100.0, 50.0)

	// Use some buckets
	rl.allow("subject1", "topic1", 1)
	rl.allow("subject2", "topic2", 1)

	if len(rl.buckets) != 2 {
		t.Errorf("expected 2 buckets, got %d", len(rl.buckets))
	}

	// Manually trigger cleanup with old timestamps
	// The cleanup removes buckets with lastRefillTS before now-10min
	// In test we simulate this by directly manipulating

	// For proper cleanup testing we'd need to mock time, but we can at least
	// call cleanup directly to ensure it doesn't panic
	rl.cleanup()
}

func TestTopicRateLimiter_ZeroRate(t *testing.T) {
	// Zero rate means no rate limiting (tokens never refill)
	rl := NewTopicRateLimiter(0.0, 10.0)

	// First 10 should work (initial burst)
	for i := 0; i < 10; i++ {
		if !rl.allow("subject1", "topic1", 1) {
			t.Errorf("request %d should be allowed", i)
		}
	}

	// After burst, no more (rate is 0 so no refill)
	if rl.allow("subject1", "topic1", 1) {
		t.Error("should be limited with zero rate")
	}
}

func TestTopicRateLimiter_CapacityCapped(t *testing.T) {
	rl := NewTopicRateLimiter(10000.0, 100.0) // Very high rate, burst 100

	// Should never exceed capacity
	for i := 0; i < 200; i++ {
		rl.allow("subject1", "topic1", 1)
		time.Sleep(1 * time.Millisecond)
	}

	// Get the bucket
	rl.mu.RLock()
	bucket := rl.buckets["subject1:topic1"]
	rl.mu.RUnlock()

	if bucket == nil {
		t.Fatal("bucket should exist")
	}

	bucket.mu.Lock()
	defer bucket.mu.Unlock()
	if bucket.tokens > 100.0 {
		t.Errorf("tokens should be capped at capacity 100, got %f", bucket.tokens)
	}
}

func TestTopicRateLimiter_Concurrent(t *testing.T) {
	rl := NewTopicRateLimiter(1000.0, 100.0)

	done := make(chan bool)
	for i := 0; i < 10; i++ {
		go func() {
			for j := 0; j < 100; j++ {
				rl.allow("subject1", "topic1", 1)
			}
			done <- true
		}()
	}

	for i := 0; i < 10; i++ {
		<-done
	}
	// Just verify no panic or deadlock occurred
}

func TestTopicRateLimiter_UnmarshalTopicFromPublishRequest(t *testing.T) {
	rl := NewTopicRateLimiter(100.0, 10.0)
	interceptor := rl.UnaryInterceptor()

	// Test that interceptor extracts topic from PublishRequest
	ctx := context.Background()
	req := &types.PublishRequest{
		Event: &types.Event{
			Topic:   "test-topic",
			Payload: []byte("data"),
		},
	}

	// With 10 burst, first request should pass
	called := false
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		called = true
		return nil, nil
	}

	_, err := interceptor(ctx, req, nil, handler)
	if err != nil {
		t.Errorf("interceptor returned error: %v", err)
	}
	if !called {
		t.Error("handler was not called")
	}
}

func TestTopicRateLimiter_UnmarshalTopicFromBatchRequest(t *testing.T) {
	rl := NewTopicRateLimiter(100.0, 10.0)
	interceptor := rl.UnaryInterceptor()

	ctx := context.Background()
	req := &types.PublishBatchRequest{
		Events: []*types.Event{
			{Topic: "batch-topic", Payload: []byte("data1")},
			{Topic: "batch-topic", Payload: []byte("data2")},
		},
	}

	called := false
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		called = true
		return nil, nil
	}

	_, err := interceptor(ctx, req, nil, handler)
	if err != nil {
		t.Errorf("interceptor returned error: %v", err)
	}
	if !called {
		t.Error("handler was not called")
	}
}

func TestTopicRateLimitInterceptor_Passthrough(t *testing.T) {
	// When rl is nil, interceptor should pass through
	interceptor := TopicRateLimitInterceptor(nil)

	ctx := context.Background()
	handlerCalled := false
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		handlerCalled = true
		return &types.PublishResponse{}, nil
	}

	result, err := interceptor(ctx, "test", nil, handler)
	if err != nil {
		t.Fatalf("passthrough interceptor failed: %v", err)
	}
	if !handlerCalled {
		t.Error("handler should have been called")
	}
	if result == nil {
		t.Error("result should not be nil")
	}
}
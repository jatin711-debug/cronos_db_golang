package api

import (
	"testing"
	"time"
)

func TestTokenBucketOutOfOrderRequests(t *testing.T) {
	start := time.Now()
	bucket := newTokenBucket(2, start)
	if !bucket.tryConsume(start, 2, 2) || !bucket.tryConsume(start.Add(time.Second), 2, 2) {
		t.Fatal("initial requests were refused")
	}

	// A request may capture its timestamp before another request and enter
	// the limiter after it. It must still finish and consume only one token.
	done := make(chan bool, 1)
	go func() { done <- bucket.tryConsume(start.Add(500*time.Millisecond), 2, 2) }()
	select {
	case allowed := <-done:
		if !allowed {
			t.Fatal("out-of-order request was refused despite an available token")
		}
	case <-time.After(time.Second):
		t.Fatal("out-of-order request stalled in the rate limiter")
	}
	if bucket.tryConsume(start.Add(time.Second), 2, 2) {
		t.Fatal("out-of-order request moved the refill time backwards")
	}
	if !bucket.tryConsume(start.Add(1500*time.Millisecond), 2, 2) {
		t.Fatal("bucket did not refill after the most recent timestamp")
	}
}

func TestTokenBucketConcurrentRequestsShareOneRefill(t *testing.T) {
	start := time.Now()
	const capacity, requests = 64, 256
	bucket := newTokenBucket(0, start)
	ready := make(chan struct{})
	results := make(chan bool, requests)
	for range requests {
		go func() {
			<-ready
			results <- bucket.tryConsume(start.Add(time.Second), capacity, capacity)
		}()
	}
	close(ready)
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	allowed := 0
	for range requests {
		select {
		case ok := <-results:
			if ok {
				allowed++
			}
		case <-deadline.C:
			t.Fatal("concurrent requests stalled in the rate limiter")
		}
	}
	if allowed != capacity {
		t.Fatalf("allowed %d requests from one refill, want %d", allowed, capacity)
	}
	if bucket.tryConsume(start.Add(time.Second), capacity, capacity) {
		t.Fatal("exhausted bucket allowed another request without a refill")
	}
}

func TestTokenBucketRetainsPartialRefills(t *testing.T) {
	start := time.Now()
	bucket := newTokenBucket(0, start)
	for _, elapsed := range []time.Duration{250, 500, 750} {
		if bucket.tryConsume(start.Add(elapsed*time.Millisecond), 1, 1) {
			t.Fatalf("bucket allowed a request after only %d milliseconds", elapsed)
		}
	}
	if !bucket.tryConsume(start.Add(time.Second), 1, 1) {
		t.Fatal("rejected requests discarded partial refills")
	}
	// Idle time may refill up to the burst capacity, never beyond it.
	if !bucket.tryConsume(start.Add(time.Hour), 1, 1) || bucket.tryConsume(start.Add(time.Hour), 1, 1) {
		t.Fatal("refill did not respect the burst capacity")
	}
}

func TestRateLimiterCleanupKeepsActiveClients(t *testing.T) {
	now := time.Now()
	limiter := &rateLimiter{rate: 1, capacity: 1}
	limiter.clients.Store("inactive", newTokenBucket(1, now.Add(-11*time.Minute)))
	limiter.clients.Store("active", newTokenBucket(1, now))
	limiter.cleanup()
	if _, exists := limiter.clients.Load("inactive"); exists {
		t.Fatal("inactive client was retained")
	}
	if _, exists := limiter.clients.Load("active"); !exists {
		t.Fatal("active client was removed")
	}
}

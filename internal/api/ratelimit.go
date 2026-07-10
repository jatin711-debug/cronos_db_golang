package api

import (
	"context"
	"net"
	"sync"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/pkg/utils"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

type rateLimiter struct {
	clients  sync.Map // ip -> *tokenBucket
	rate     float64
	capacity float64
}

type tokenBucket struct {
	mu           sync.Mutex
	tokens       float64
	lastRefillTS time.Time
}

func newRateLimiter(rate float64, capacity float64) *rateLimiter {
	rl := &rateLimiter{
		rate:     rate,
		capacity: capacity,
	}

	// Cleanup routine to prevent memory leaks from inactive IPs
	utils.GoSafe("rate-limiter-cleanup", func() {
		for {
			time.Sleep(5 * time.Minute)
			rl.cleanup()
		}
	})

	return rl
}

func (rl *rateLimiter) cleanup() {
	cutoff := time.Now().Add(-10 * time.Minute)
	rl.clients.Range(func(key, value interface{}) bool {
		tb := value.(*tokenBucket)
		tb.mu.Lock()
		lastRefill := tb.lastRefillTS
		tb.mu.Unlock()
		if lastRefill.Before(cutoff) {
			rl.clients.Delete(key)
		}
		return true
	})
}

// tryConsume refills tokens and attempts to consume one.
// Extracted to avoid code duplication between Load and LoadOrStore paths.
func (tb *tokenBucket) tryConsume(now time.Time, rate, capacity float64) bool {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	elapsed := now.Sub(tb.lastRefillTS).Seconds()
	tb.tokens += elapsed * rate
	if tb.tokens > capacity {
		tb.tokens = capacity
	}
	tb.lastRefillTS = now

	if tb.tokens >= 1 {
		tb.tokens -= 1
		return true
	}
	return false
}

func (rl *rateLimiter) allow(ip string) bool {
	now := time.Now()

	// Fast path: Load existing bucket without allocation.
	// sync.Map.LoadOrStore always allocates the *tokenBucket argument on the heap
	// even when the key already exists. For steady-state traffic (same IPs),
	// this Load-first pattern eliminates ~1 heap alloc per request.
	if v, ok := rl.clients.Load(ip); ok {
		return v.(*tokenBucket).tryConsume(now, rl.rate, rl.capacity)
	}

	// Slow path: new IP, allocate bucket
	v, _ := rl.clients.LoadOrStore(ip, &tokenBucket{
		tokens:       rl.capacity,
		lastRefillTS: now,
	})
	return v.(*tokenBucket).tryConsume(now, rl.rate, rl.capacity)
}

// RateLimitInterceptor creates a gRPC unary interceptor for rate limiting
func RateLimitInterceptor(requestsPerSecond float64, burstCapacity float64) grpc.UnaryServerInterceptor {
	rl := newRateLimiter(requestsPerSecond, burstCapacity)
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		p, ok := peer.FromContext(ctx)
		if !ok {
			return handler(ctx, req)
		}

		host, _, err := net.SplitHostPort(p.Addr.String())
		if err != nil {
			host = p.Addr.String()
		}

		if !rl.allow(host) {
			return nil, status.Errorf(codes.ResourceExhausted, "rate limit exceeded for IP %s", host)
		}

		return handler(ctx, req)
	}
}

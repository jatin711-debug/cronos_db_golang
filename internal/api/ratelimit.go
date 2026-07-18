package api

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/pkg/utils"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

const tokenScale = 1_000_000 // micro-tokens per token; allows integer atomic ops

type rateLimiter struct {
	clients  sync.Map // ip -> *tokenBucket
	rate     float64  // tokens per second
	capacity float64  // max tokens
}

// tokenBucket is a lock-free token bucket. Tokens are stored as micro-tokens in
// an atomic.Int64; lastRefillNS stores the last refill timestamp in Unix
// nanoseconds. Refill and consumption use a compare-and-swap loop so many
// concurrent requests can share one bucket without a mutex.
type tokenBucket struct {
	tokens       atomic.Int64 // micro-tokens
	lastRefillNS atomic.Int64 // Unix nanoseconds
}

func newTokenBucket(tokens float64, now time.Time) *tokenBucket {
	tb := &tokenBucket{}
	tb.tokens.Store(int64(tokens * tokenScale))
	tb.lastRefillNS.Store(now.UnixNano())
	return tb
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
	cutoffNS := cutoff.UnixNano()
	rl.clients.Range(func(key, value interface{}) bool {
		tb := value.(*tokenBucket)
		if tb.lastRefillNS.Load() < cutoffNS {
			rl.clients.Delete(key)
		}
		return true
	})
}

// tryConsume refills tokens and attempts to consume one. It retries with a
// compare-and-swap loop until it either consumes a token or confirms that no
// token is available.
func (tb *tokenBucket) tryConsume(now time.Time, rate, capacity float64) bool {
	nowNS := now.UnixNano()
	capacityMicro := int64(capacity * tokenScale)
	// micro-tokens added per nanosecond = rate * tokenScale / 1e9 = rate / 1000
	refillPerNS := rate / 1000.0

	for {
		lastNS := tb.lastRefillNS.Load()
		if lastNS > nowNS {
			// Clock moved backwards; cap refill to zero for this attempt.
			lastNS = nowNS
		}
		elapsedNS := nowNS - lastNS
		currentMicro := tb.tokens.Load()

		refillMicro := int64(float64(elapsedNS) * refillPerNS)
		availableMicro := currentMicro + refillMicro
		if availableMicro > capacityMicro {
			availableMicro = capacityMicro
		}

		if availableMicro < tokenScale {
			// Not enough for one token. Advance lastRefillNS so we don't
			// re-count the elapsed time on the next call, then fail.
			if tb.lastRefillNS.CompareAndSwap(lastNS, nowNS) {
				return false
			}
			continue
		}

		remainingMicro := availableMicro - tokenScale
		if tb.tokens.CompareAndSwap(currentMicro, remainingMicro) &&
			tb.lastRefillNS.CompareAndSwap(lastNS, nowNS) {
			return true
		}
		// Another goroutine mutated state; retry with fresh values.
	}
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
	v, _ := rl.clients.LoadOrStore(ip, newTokenBucket(rl.capacity, now))
	return v.(*tokenBucket).tryConsume(now, rl.rate, rl.capacity)
}

// RateLimitInterceptor creates a gRPC unary interceptor that rate-limits by client IP
// using a token bucket of requestsPerSecond with burstCapacity.
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

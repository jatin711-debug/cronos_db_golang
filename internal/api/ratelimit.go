package api

import (
	"context"
	"net"
	"sync"
	"time"

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
	go func() {
		for {
			time.Sleep(5 * time.Minute)
			rl.cleanup()
		}
	}()

	return rl
}

func (rl *rateLimiter) cleanup() {
	cutoff := time.Now().Add(-10 * time.Minute)
	rl.clients.Range(func(key, value interface{}) bool {
		ip := key.(string)
		tb := value.(*tokenBucket)
		tb.mu.Lock()
		lastRefill := tb.lastRefillTS
		tb.mu.Unlock()
		if lastRefill.Before(cutoff) {
			rl.clients.Delete(ip)
		}
		return true
	})
}

func (rl *rateLimiter) allow(ip string) bool {
	now := time.Now()
	v, _ := rl.clients.LoadOrStore(ip, &tokenBucket{
		tokens:       rl.capacity,
		lastRefillTS: now,
	})
	tb := v.(*tokenBucket)

	tb.mu.Lock()
	defer tb.mu.Unlock()

	elapsed := now.Sub(tb.lastRefillTS).Seconds()
	tb.tokens += elapsed * rl.rate
	if tb.tokens > rl.capacity {
		tb.tokens = rl.capacity
	}
	tb.lastRefillTS = now

	if tb.tokens >= 1 {
		tb.tokens -= 1
		return true
	}
	return false
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

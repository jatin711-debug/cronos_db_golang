package api

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/internal/auth"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TopicRateLimiter provides per-subject, per-topic token bucket rate limiting.
// This prevents one tenant or one topic from saturating the cluster.
type TopicRateLimiter struct {
	mu       sync.RWMutex
	buckets  map[string]*topicBucket // key: "subject:topic" or ":topic" for anonymous
	rate     float64
	capacity float64
}

type topicBucket struct {
	mu           sync.Mutex
	tokens       float64
	lastRefillTS time.Time
}

// NewTopicRateLimiter creates a topic-level rate limiter.
// rate = events per second per (subject, topic). capacity = burst size.
func NewTopicRateLimiter(rate, capacity float64) *TopicRateLimiter {
	rl := &TopicRateLimiter{
		buckets:  make(map[string]*topicBucket),
		rate:     rate,
		capacity: capacity,
	}
	go rl.cleanupLoop()
	return rl
}

func (rl *TopicRateLimiter) cleanupLoop() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		rl.cleanup()
	}
}

func (rl *TopicRateLimiter) cleanup() {
	cutoff := time.Now().Add(-10 * time.Minute)
	rl.mu.Lock()
	defer rl.mu.Unlock()
	for key, b := range rl.buckets {
		b.mu.Lock()
		last := b.lastRefillTS
		b.mu.Unlock()
		if last.Before(cutoff) {
			delete(rl.buckets, key)
		}
	}
}

func (rl *TopicRateLimiter) allow(subject, topic string, count int) bool {
	key := fmt.Sprintf("%s:%s", subject, topic)

	rl.mu.RLock()
	b, ok := rl.buckets[key]
	rl.mu.RUnlock()
	if !ok {
		rl.mu.Lock()
		b, ok = rl.buckets[key]
		if !ok {
			b = &topicBucket{tokens: rl.capacity, lastRefillTS: time.Now()}
			rl.buckets[key] = b
		}
		rl.mu.Unlock()
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(b.lastRefillTS).Seconds()
	b.tokens += elapsed * rl.rate
	if b.tokens > rl.capacity {
		b.tokens = rl.capacity
	}
	b.lastRefillTS = now

	if b.tokens >= float64(count) {
		b.tokens -= float64(count)
		return true
	}
	return false
}

// UnaryInterceptor returns a gRPC interceptor for topic rate limiting.
func (rl *TopicRateLimiter) UnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		// Extract topic from request
		var topic string
		var count int = 1

		switch r := req.(type) {
		case *types.PublishRequest:
			if r.Event != nil {
				topic = r.Event.Topic
			}
		case *types.PublishBatchRequest:
			count = len(r.Events)
			if count > 0 && len(r.Events) > 0 {
				topic = r.Events[0].Topic
			}
		}

		subject := "anonymous"
		if claims, ok := auth.ClaimsFromContext(ctx); ok {
			subject = claims.Subject
		}

		if topic != "" && !rl.allow(subject, topic, count) {
			return nil, status.Errorf(codes.ResourceExhausted,
				"topic rate limit exceeded for subject=%s topic=%s", subject, topic)
		}

		return handler(ctx, req)
	}
}

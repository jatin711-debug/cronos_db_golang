package hedging

import (
	"context"
	"sync"
	"time"
)

// Policy controls request hedging behavior.
type Policy struct {
	Enabled   bool
	Delay     time.Duration // How long to wait before sending hedge request
	MaxHedges int           // Max additional requests (1 = one hedge)
}

// DefaultPolicy returns safe defaults (disabled).
func DefaultPolicy() Policy {
	return Policy{
		Enabled:   false,
		Delay:     15 * time.Millisecond, // Hedge if original takes > p50
		MaxHedges: 1,
	}
}

// Do executes fn with hedging. Returns the first successful result.
// Cancel is called for slower requests when a winner is found.
func Do[T any](ctx context.Context, policy Policy, fn func(context.Context) (T, error)) (T, error) {
	if !policy.Enabled {
		return fn(ctx)
	}

	// Fast path: no hedging if context already has deadline < delay
	if deadline, ok := ctx.Deadline(); ok {
		if time.Until(deadline) <= policy.Delay {
			return fn(ctx)
		}
	}

	type result struct {
		val T
		err error
	}

	results := make(chan result, policy.MaxHedges+1)
	var wg sync.WaitGroup
	cancelFns := make([]context.CancelFunc, 0, policy.MaxHedges+1)
	var mu sync.Mutex

	launch := func(delay time.Duration) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if delay > 0 {
				select {
				case <-time.After(delay):
				case <-ctx.Done():
					return
				}
			}
			childCtx, cancel := context.WithCancel(ctx)
			mu.Lock()
			cancelFns = append(cancelFns, cancel)
			mu.Unlock()

			val, err := fn(childCtx)
			select {
			case results <- result{val: val, err: err}:
			default:
				// Someone else won; cancel this context
				cancel()
			}
		}()
	}

	launch(0)
	for i := 0; i < policy.MaxHedges; i++ {
		launch(policy.Delay)
	}

	// Wait for first result
	res := <-results

	// Cancel in-flight requests
	mu.Lock()
	for _, cancel := range cancelFns {
		cancel()
	}
	mu.Unlock()

	// Drain remaining to avoid goroutine leaks
	go func() {
		for range results {
		}
	}()
	go func() {
		wg.Wait()
		close(results)
	}()

	return res.val, res.err
}

package hedging

import (
	"context"
	"sync"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/pkg/utils"
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
		utils.GoSafe("hedge-request", func() {
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
		})
	}

	launch(0)
	for i := 0; i < policy.MaxHedges; i++ {
		launch(policy.Delay)
	}

	// Close results once every attempt has finished so the range below terminates
	// even if some hedges returned early (e.g. their delay was cut short by ctx
	// cancellation) and never sent a value. The buffer capacity equals the number
	// of attempts, so sends never block.
	utils.GoSafe("hedge-wait-group", func() {
		wg.Wait()
		close(results)
	})

	// Return the first SUCCESSFUL result. Previously this took the first result off
	// the channel regardless of error, so a fast failure would beat a slow success
	// — defeating the purpose of hedging. We now consume until one succeeds; if
	// none do, res holds the last error seen.
	var res result
	var got bool
	for r := range results {
		res = r
		got = true
		if r.err == nil {
			break // first success wins
		}
	}
	if !got {
		// No attempt produced a result (all cancelled before sending). Run the
		// primary inline as a fallback so we never return a zero value with nil err.
		val, err := fn(ctx)
		res = result{val: val, err: err}
	}

	// Cancel in-flight requests.
	mu.Lock()
	for _, cancel := range cancelFns {
		cancel()
	}
	mu.Unlock()

	// Drain any results still buffered (the range may have broken early on success)
	// so producing goroutines are not leaked.
	utils.GoSafe("hedge-drain-results", func() {
		for range results {
		}
	})

	return res.val, res.err
}

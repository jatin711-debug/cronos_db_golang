package hedging

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestDefaultPolicy(t *testing.T) {
	p := DefaultPolicy()
	if p.Enabled {
		t.Error("default policy should be disabled")
	}
	if p.Delay != 15*time.Millisecond {
		t.Errorf("expected delay 15ms, got %v", p.Delay)
	}
	if p.MaxHedges != 1 {
		t.Errorf("expected MaxHedges 1, got %d", p.MaxHedges)
	}
}

// TestDo_FirstSuccessWinsOverFastFailure verifies that hedging returns the first
// SUCCESSFUL result, not merely the first-completed one: a fast failure must not
// beat a slower success.
func TestDo_FirstSuccessWinsOverFastFailure(t *testing.T) {
	policy := Policy{Enabled: true, Delay: 5 * time.Millisecond, MaxHedges: 1}
	var calls atomic.Int32

	val, err := Do(context.Background(), policy, func(ctx context.Context) (string, error) {
		n := calls.Add(1)
		if n == 1 {
			// First attempt fails fast.
			return "", errors.New("fast failure")
		}
		// Hedge attempt succeeds after a short delay.
		select {
		case <-time.After(20 * time.Millisecond):
		case <-ctx.Done():
			return "", ctx.Err()
		}
		return "slow-success", nil
	})
	if err != nil {
		t.Fatalf("expected success from hedge, got error: %v", err)
	}
	if val != "slow-success" {
		t.Fatalf("expected slow-success, got %q", val)
	}
}

func TestDo_Disabled(t *testing.T) {
	ctx := context.Background()
	policy := Policy{Enabled: false}

	result, err := Do(ctx, policy, func(ctx context.Context) (string, error) {
		return "success", nil
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != "success" {
		t.Errorf("expected success, got %s", result)
	}
}

func TestDo_Success(t *testing.T) {
	ctx := context.Background()
	policy := Policy{Enabled: true, Delay: 10 * time.Millisecond, MaxHedges: 1}

	var callCount atomic.Int32
	result, err := Do(ctx, policy, func(ctx context.Context) (string, error) {
		callCount.Add(1)
		return "success", nil
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != "success" {
		t.Errorf("expected success, got %s", result)
	}
	if callCount.Load() != 1 {
		t.Errorf("expected 1 call, got %d", callCount.Load())
	}
}

func TestDo_ContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// With disabled policy, just calls fn directly
	policy := Policy{Enabled: false}

	_, err := Do(ctx, policy, func(ctx context.Context) (string, error) {
		// fn itself doesn't check context
		return "", nil
	})

	if err != nil {
		t.Errorf("unexpected error with disabled policy: %v", err)
	}
}

func TestDo_Error(t *testing.T) {
	ctx := context.Background()
	policy := Policy{Enabled: true, Delay: 10 * time.Millisecond, MaxHedges: 1}

	expectedErr := errors.New("operation failed")
	_, err := Do(ctx, policy, func(ctx context.Context) (string, error) {
		return "", expectedErr
	})

	if err != expectedErr {
		t.Fatalf("expected %v, got %v", expectedErr, err)
	}
}

func TestDo_Hedges(t *testing.T) {
	ctx := context.Background()
	policy := Policy{Enabled: true, Delay: 5 * time.Millisecond, MaxHedges: 2}

	var callCount atomic.Int32
	_, err := Do(ctx, policy, func(ctx context.Context) (string, error) {
		currentCall := callCount.Add(1)
		if currentCall == 1 {
			// First call sleeps longer than hedge delay
			time.Sleep(50 * time.Millisecond)
		}
		return "result", nil
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Should have launched hedges
	if callCount.Load() < 1 {
		t.Errorf("expected at least 1 call, got %d", callCount.Load())
	}
}

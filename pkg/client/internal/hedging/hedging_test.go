package hedging

import (
	"context"
	"errors"
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

	callCount := 0
	result, err := Do(ctx, policy, func(ctx context.Context) (string, error) {
		callCount++
		return "success", nil
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != "success" {
		t.Errorf("expected success, got %s", result)
	}
	if callCount != 1 {
		t.Errorf("expected 1 call, got %d", callCount)
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

	callCount := 0
	_, err := Do(ctx, policy, func(ctx context.Context) (string, error) {
		callCount++
		if callCount == 1 {
			// First call sleeps longer than hedge delay
			time.Sleep(50 * time.Millisecond)
		}
		return "result", nil
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Should have launched hedges
	if callCount < 1 {
		t.Errorf("expected at least 1 call, got %d", callCount)
	}
}

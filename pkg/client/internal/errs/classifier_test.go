package errs

import (
	"errors"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestIsRetryable(t *testing.T) {
	// Retryable gRPC codes
	retryableCodes := []codes.Code{
		codes.Unavailable,
		codes.DeadlineExceeded,
		codes.ResourceExhausted,
	}

	for _, c := range retryableCodes {
		err := status.Error(c, "test error")
		if !IsRetryable(err) {
			t.Errorf("expected retryable for code %v", c)
		}
	}

	// Non-retryable codes
	nonRetryable := []codes.Code{
		codes.OK,
		codes.InvalidArgument,
		codes.NotFound,
		codes.PermissionDenied,
	}

	for _, c := range nonRetryable {
		err := status.Error(c, "test error")
		if IsRetryable(err) {
			t.Errorf("expected non-retryable for code %v", c)
		}
	}

	// Non-gRPC error — errs package treats all non-gRPC errors as retryable
	if !IsRetryable(errors.New("random error")) {
		t.Error("random error should be retryable (non-gRPC = retryable)")
	}

	// Nil error
	if IsRetryable(nil) {
		t.Error("nil error should not be retryable")
	}
}

func TestIsLeaderRelated(t *testing.T) {
	leaderErrors := []string{
		"not leader",
		"leader changed",
		"no leader",
		"leader election",
	}

	for _, msg := range leaderErrors {
		err := errors.New(msg)
		if !IsLeaderRelated(err) {
			t.Errorf("expected leader-related for %q", msg)
		}
	}

	nonLeader := []string{
		"connection refused",
		"timeout",
		"random error",
	}

	for _, msg := range nonLeader {
		err := errors.New(msg)
		if IsLeaderRelated(err) {
			t.Errorf("expected non-leader for %q", msg)
		}
	}

	// Nil error
	if IsLeaderRelated(nil) {
		t.Error("nil should not be leader-related")
	}
}

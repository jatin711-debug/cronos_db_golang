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
		codes.Aborted,
	}

	for _, c := range retryableCodes {
		err := status.Error(c, "test error")
		if !IsRetryable(err) {
			t.Errorf("expected retryable for code %v", c)
		}
	}

	// ResourceExhausted typically indicates quota exhaustion and should not be retried.
	if IsRetryable(status.Error(codes.ResourceExhausted, "quota exceeded")) {
		t.Error("expected ResourceExhausted to be non-retryable")
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

	// Non-gRPC errors are treated as non-retryable by default so application
	// failures (serialization, validation, etc.) fail fast.
	if IsRetryable(errors.New("random error")) {
		t.Error("random non-gRPC error should be non-retryable")
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

// Package errs classifies gRPC and message errors for client retry and routing decisions.
package errs

import (
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// IsRetryable reports whether an RPC error is transient/retryable.
// Non-gRPC errors are treated as non-retryable by default so that application-level
// failures (serialization, validation, etc.) fail fast instead of being retried.
func IsRetryable(err error) bool {
	if err == nil {
		return false
	}
	st, ok := status.FromError(err)
	if !ok {
		return false
	}

	switch st.Code() {
	case codes.Unavailable, codes.DeadlineExceeded, codes.Aborted:
		return true
	case codes.ResourceExhausted:
		// ResourceExhausted typically indicates tenant quota exhaustion.
		// Retry amplifies the problem; callers should back off or shed load.
		return false
	default:
		return false
	}
}

// IsLeaderRelated reports whether an error likely indicates stale routing.
func IsLeaderRelated(err error) bool {
	if err == nil {
		return false
	}
	st, ok := status.FromError(err)
	if ok && (st.Code() == codes.FailedPrecondition || st.Code() == codes.Unavailable) {
		return IsLeaderRelatedMessage(st.Message())
	}
	if ok {
		return false
	}
	return IsLeaderRelatedMessage(err.Error())
}

// IsLeaderRelatedMessage reports whether a raw error message contains wording
// that suggests the request was sent to the wrong node (e.g. stale leader metadata).
func IsLeaderRelatedMessage(message string) bool {
	m := strings.ToLower(message)
	phrases := []string{
		"not leader",
		"not the leader",
		"wrong leader",
		"stale leader",
		"leader changed",
		"leader not found",
		"no leader",
		"leader mismatch",
		"not current leader",
		"leader id mismatch",
		"leader election",
	}
	for _, p := range phrases {
		if strings.Contains(m, p) {
			return true
		}
	}
	return false
}

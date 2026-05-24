package errs

import (
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// IsRetryable reports whether an RPC error is transient/retryable.
func IsRetryable(err error) bool {
	if err == nil {
		return false
	}
	st, ok := status.FromError(err)
	if !ok {
		return true
	}

	switch st.Code() {
	case codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted, codes.Aborted:
		return true
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
		msg := strings.ToLower(st.Message())
		return strings.Contains(msg, "leader") || strings.Contains(msg, "partition")
	}
	if ok {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "leader") || strings.Contains(msg, "partition")
}

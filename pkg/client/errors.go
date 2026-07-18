package client

import (
	"errors"
	"fmt"
)

// ErrorKind classifies client errors for retry and policy decisions.
type ErrorKind string

const (
	// ErrorKindValidation indicates invalid client configuration or arguments.
	ErrorKindValidation ErrorKind = "validation"
	// ErrorKindTimeout indicates a deadline was exceeded.
	ErrorKindTimeout ErrorKind = "timeout"
	// ErrorKindAuth indicates authentication or authorization failure.
	ErrorKindAuth ErrorKind = "auth"
	// ErrorKindUnavailable indicates the target is temporarily unreachable.
	ErrorKindUnavailable ErrorKind = "unavailable"
	// ErrorKindLeaderChange indicates partition leadership moved; refresh metadata and retry.
	ErrorKindLeaderChange ErrorKind = "leader_change"
	// ErrorKindMetadataStale indicates cached routing metadata is out of date.
	ErrorKindMetadataStale ErrorKind = "metadata_stale"
	// ErrorKindTransport indicates a low-level gRPC/network failure.
	ErrorKindTransport ErrorKind = "transport"
	// ErrorKindInternal indicates an unexpected client or server internal error.
	ErrorKindInternal ErrorKind = "internal"
)

// Error is a typed client error with operation name and classification.
type Error struct {
	// Kind classifies the failure for retry/policy logic.
	Kind ErrorKind
	// Op is a short operation name (e.g. "producer.send") for diagnostics.
	Op string
	// Err is the underlying cause.
	Err error
}

// Error implements the error interface.
func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	if e.Op == "" {
		return fmt.Sprintf("%s: %v", e.Kind, e.Err)
	}
	return fmt.Sprintf("%s (%s): %v", e.Op, e.Kind, e.Err)
}

// Unwrap returns the underlying cause for errors.Is / errors.As.
func (e *Error) Unwrap() error { return e.Err }

// wrapError wraps err as a typed Error unless it is already one.
func wrapError(op string, kind ErrorKind, err error) error {
	if err == nil {
		return nil
	}
	var ce *Error
	if errors.As(err, &ce) {
		return err
	}
	return &Error{
		Kind: kind,
		Op:   op,
		Err:  err,
	}
}

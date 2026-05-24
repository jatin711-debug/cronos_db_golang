package client

import (
	"errors"
	"fmt"
)

// ErrorKind classifies client errors for policy decisions.
type ErrorKind string

const (
	ErrorKindValidation    ErrorKind = "validation"
	ErrorKindTimeout       ErrorKind = "timeout"
	ErrorKindAuth          ErrorKind = "auth"
	ErrorKindUnavailable   ErrorKind = "unavailable"
	ErrorKindLeaderChange  ErrorKind = "leader_change"
	ErrorKindMetadataStale ErrorKind = "metadata_stale"
	ErrorKindTransport     ErrorKind = "transport"
	ErrorKindInternal      ErrorKind = "internal"
)

// Error is a typed client error.
type Error struct {
	Kind ErrorKind
	Op   string
	Err  error
}

func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	if e.Op == "" {
		return fmt.Sprintf("%s: %v", e.Kind, e.Err)
	}
	return fmt.Sprintf("%s (%s): %v", e.Op, e.Kind, e.Err)
}

func (e *Error) Unwrap() error { return e.Err }

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

package types

import (
	"errors"
	"fmt"
)

// Sentinel errors shared across packages. Callers should use errors.Is for comparison.
var (
	// ErrNotFound indicates a generic missing resource.
	ErrNotFound = errors.New("not found")
	// ErrAlreadyExists indicates a create conflict for a resource that already exists.
	ErrAlreadyExists = errors.New("already exists")
	// ErrInvalidArgument indicates client-supplied arguments failed validation.
	ErrInvalidArgument = errors.New("invalid argument")
	// ErrTimeout indicates an operation exceeded its deadline.
	ErrTimeout = errors.New("timeout")
	// ErrPartitionNotFound indicates the requested partition ID does not exist.
	ErrPartitionNotFound = errors.New("partition not found")
	// ErrLeaderNotFound indicates no leader is known for a partition or cluster.
	ErrLeaderNotFound = errors.New("leader not found")
	// ErrFollowerNotFound indicates a referenced follower is unknown.
	ErrFollowerNotFound = errors.New("follower not found")
	// ErrConsumerGroupNotFound indicates the consumer group does not exist.
	ErrConsumerGroupNotFound = errors.New("consumer group not found")
	// ErrMessageIDExists indicates a publish was rejected as a duplicate message ID.
	ErrMessageIDExists = errors.New("message ID already exists")
	// ErrOffsetOutOfRange indicates a read/seek offset is outside available log bounds.
	ErrOffsetOutOfRange = errors.New("offset out of range")
	// ErrSegmentNotFound indicates a WAL segment file could not be located.
	ErrSegmentNotFound = errors.New("segment not found")
	// ErrIndexCorrupted indicates a sparse index failed integrity checks.
	ErrIndexCorrupted = errors.New("index corrupted")
	// ErrWALCorrupted indicates a WAL record failed CRC or structural validation.
	ErrWALCorrupted = errors.New("WAL corrupted")
	// ErrLeaderChange indicates leadership moved; clients should refresh metadata and retry.
	ErrLeaderChange = errors.New("leader change detected")
	// ErrReplicationLag indicates followers are too far behind for the requested op.
	ErrReplicationLag = errors.New("replication lag too high")
	// ErrSchedulerLag indicates the scheduler is behind wall-clock expectations.
	ErrSchedulerLag = errors.New("scheduler lag too high")
	// ErrDeliveryTimeout indicates a delivery was not acked before its deadline.
	ErrDeliveryTimeout = errors.New("delivery timeout")
	// ErrMaxRetriesExceeded indicates delivery retries were exhausted (often DLQ path).
	ErrMaxRetriesExceeded = errors.New("max retries exceeded")
	// ErrInvalidChecksum indicates a payload or batch checksum mismatch.
	ErrInvalidChecksum = errors.New("invalid checksum")
)

// ErrorWithCode is a structured error carrying a machine-readable code and message,
// optionally wrapping an underlying cause for errors.Is / errors.As.
type ErrorWithCode struct {
	// Code is a short machine-readable error code (e.g. for API mapping).
	Code string
	// Message is a human-readable description of the failure.
	Message string
	// Err is the optional wrapped cause.
	Err error
}

// Error implements the error interface as "code: message".
func (e *ErrorWithCode) Error() string {
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

// Unwrap returns the underlying cause for errors.Is / errors.As chains.
func (e *ErrorWithCode) Unwrap() error {
	return e.Err
}

// NewError constructs an ErrorWithCode with the given code, message, and optional cause.
func NewError(code, message string, err error) *ErrorWithCode {
	return &ErrorWithCode{
		Code:    code,
		Message: message,
		Err:     err,
	}
}

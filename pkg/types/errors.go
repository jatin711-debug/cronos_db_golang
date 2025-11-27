package types

import "errors"
import "fmt"
// Common error types
var (
	ErrNotFound         = errors.New("not found")
	ErrAlreadyExists    = errors.New("already exists")
	ErrInvalidArgument  = errors.New("invalid argument")
	ErrTimeout          = errors.New("timeout")
	ErrPartitionNotFound = errors.New("partition not found")
	ErrLeaderNotFound   = errors.New("leader not found")
	ErrFollowerNotFound = errors.New("follower not found")
	ErrConsumerGroupNotFound = errors.New("consumer group not found")
	ErrMessageIDExists = errors.New("message ID already exists")
	ErrOffsetOutOfRange = errors.New("offset out of range")
	ErrSegmentNotFound = errors.New("segment not found")
	ErrIndexCorrupted = errors.New("index corrupted")
	ErrWALCorrupted = errors.New("WAL corrupted")
	ErrLeaderChange = errors.New("leader change detected")
	ErrReplicationLag = errors.New("replication lag too high")
	ErrSchedulerLag = errors.New("scheduler lag too high")
	ErrDeliveryTimeout = errors.New("delivery timeout")
	ErrMaxRetriesExceeded = errors.New("max retries exceeded")
	ErrInvalidChecksum = errors.New("invalid checksum")
)

// ErrorWithCode represents an error with a code
type ErrorWithCode struct {
	Code    string
	Message string
	Err     error
}

// Error returns the error message
func (e *ErrorWithCode) Error() string {
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

// Unwrap returns the underlying error
func (e *ErrorWithCode) Unwrap() error {
	return e.Err
}

// NewError creates a new error with code
func NewError(code, message string, err error) *ErrorWithCode {
	return &ErrorWithCode{
		Code:    code,
		Message: message,
		Err:     err,
	}
}

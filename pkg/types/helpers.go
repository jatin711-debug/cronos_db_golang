package types

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
)

// generateDeliveryID creates a unique delivery ID
func GenerateDeliveryID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// OffsetRange represents a range of offsets
type OffsetRange struct {
	Start int64
	End   int64
}

// Contains checks if offset is within range
func (r OffsetRange) Contains(offset int64) bool {
	return offset >= r.Start && offset <= r.End
}

// String returns string representation
func (r OffsetRange) String() string {
	return fmt.Sprintf("[%d-%d]", r.Start, r.End)
}

// TimestampRange represents a range of timestamps
type TimestampRange struct {
	Start int64
	End   int64
}

// Contains checks if timestamp is within range
func (r TimestampRange) Contains(ts int64) bool {
	return ts >= r.Start && ts <= r.End
}

// String returns string representation
func (r TimestampRange) String() string {
	return fmt.Sprintf("[%d-%d]", r.Start, r.End)
}

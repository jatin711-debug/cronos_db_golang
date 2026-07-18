package types

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
)

// GenerateDeliveryID returns a new random 128-bit delivery ID as a hex string.
// Used to correlate deliveries with acks across the dispatcher and clients.
func GenerateDeliveryID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// OffsetRange is an inclusive range of log offsets [Start, End].
type OffsetRange struct {
	// Start is the first offset in the range (inclusive).
	Start int64
	// End is the last offset in the range (inclusive).
	End int64
}

// Contains reports whether offset is within [Start, End].
func (r OffsetRange) Contains(offset int64) bool {
	return offset >= r.Start && offset <= r.End
}

// String formats the range as "[start-end]".
func (r OffsetRange) String() string {
	return fmt.Sprintf("[%d-%d]", r.Start, r.End)
}

// TimestampRange is an inclusive range of Unix timestamps in milliseconds [Start, End].
type TimestampRange struct {
	// Start is the first timestamp in the range (inclusive, Unix ms).
	Start int64
	// End is the last timestamp in the range (inclusive, Unix ms).
	End int64
}

// Contains reports whether ts is within [Start, End].
func (r TimestampRange) Contains(ts int64) bool {
	return ts >= r.Start && ts <= r.End
}

// String formats the range as "[start-end]".
func (r TimestampRange) String() string {
	return fmt.Sprintf("[%d-%d]", r.Start, r.End)
}

package dedup

// Outcome is how far the publish that claimed a message ID is known to have got.
// A retry of the same ID is answered from it: an accepted publish is a
// duplicate, an appended one is finished rather than appended again, and a
// claimed one is still in flight.
type Outcome int

const (
	// Claimed means a publish holds the ID and its event is not known to be in
	// the log.
	Claimed Outcome = iota
	// Appended means the event is in the log, but the publish was not accepted:
	// the log entry was not confirmed replicated, made durable, or scheduled.
	Appended
	// Accepted means the publish completed.
	Accepted
)

// ClaimedOffset is the offset stored with a claim that has no log position yet.
const ClaimedOffset int64 = -1

// AppendedAt returns the value to store for a message ID whose event is in the
// log at offset while its publish is not yet accepted. Accepted publishes
// store the offset itself, so the two cannot be confused.
func AppendedAt(offset int64) int64 { return -offset - 2 }

// DecodeOffset splits a stored value into the outcome it records and the log
// offset of the event, which is -1 for a claim.
func DecodeOffset(stored int64) (Outcome, int64) {
	switch {
	case stored >= 0:
		return Accepted, stored
	case stored == ClaimedOffset:
		return Claimed, -1
	default:
		return Appended, -stored - 2
	}
}

// Outcome reports how far the publish that claimed messageID got and the log
// offset of its event. found is false when the ID is not recorded.
func (m *Manager) Outcome(messageID string) (outcome Outcome, offset int64, found bool, err error) {
	stored, found, err := m.store.GetOffset(messageID)
	if err != nil || !found {
		return Claimed, -1, false, err
	}
	outcome, offset = DecodeOffset(stored)
	return outcome, offset, true, nil
}

// ReleaseIf removes messageID only while its stored value is still stored, the
// value the caller read. It lets a retry discard a record of an event that is
// no longer in the log without removing a claim another publish has made since.
// It reports whether the record was removed; stores without conditional
// removal never remove.
func (m *Manager) ReleaseIf(messageID string, stored int64) (bool, error) {
	if store, ok := m.store.(interface {
		DeleteIf(messageID string, stored int64) (bool, error)
	}); ok {
		return store.DeleteIf(messageID, stored)
	}
	return false, nil
}

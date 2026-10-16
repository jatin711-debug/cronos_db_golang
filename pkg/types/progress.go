package types

// ProgressCursor is a leader-local continuation for completion replication.
// NextOffset=-1 advances past Group; the zero cursor starts a new round.
type ProgressCursor struct {
	Group      string
	NextOffset int64
}

// DeadLetterCursor is a leader-local position in the append-only DLQ journal.
type DeadLetterCursor struct{ Segment, Position int64 }

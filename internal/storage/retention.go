package storage

import (
	"context"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

// PruneOptions selects closed segments. Every selected event must also pass the
// completion predicate; neither Force nor an operator cursor bypasses it.
type PruneOptions struct {
	Filename        string
	BeforeOffset    int64
	BeforeTimestamp int64
	MaxBytes        int64
	AllCompleted    bool
}

// Prune deletes completed segments under the same lock used by append, reads,
// checkpoint and rotation. Memory during verification is bounded by one record.
// eligible must not call back into this WAL. A nil predicate fails closed.
func (w *WAL) Prune(ctx context.Context, opts PruneOptions, eligible func(*types.Event) bool) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if eligible == nil {
		return 0, nil
	}
	var total int64
	for _, seg := range w.segments {
		total += seg.GetSize()
	}
	deleted := 0
	now := time.Now().UnixMilli()
	for i := 0; i < len(w.segments); {
		seg := w.segments[i]
		selected := opts.AllCompleted || (opts.Filename != "" && seg.filename == opts.Filename) ||
			(opts.BeforeOffset > 0 && seg.lastOffset < opts.BeforeOffset) || opts.BeforeTimestamp > 0 ||
			(opts.MaxBytes > 0 && total > opts.MaxBytes)
		if seg == w.activeSegment || !selected {
			i++
			continue
		}
		ok, err := seg.allEventsMatch(ctx, func(event *types.Event) bool {
			event.PartitionId = w.partitionID // partition identity is implicit in the WAL directory
			return event.ScheduleTs <= now && (opts.BeforeTimestamp <= 0 || event.ScheduleTs < opts.BeforeTimestamp) && eligible(event)
		})
		if err != nil {
			return deleted, fmt.Errorf("verify retention of %s: %w", seg.filename, err)
		}
		if !ok {
			i++
			continue
		}
		size := seg.GetSize()
		if err := seg.Delete(); err != nil {
			return deleted, err
		}
		copy(w.segments[i:], w.segments[i+1:])
		w.segments[len(w.segments)-1] = nil
		w.segments = w.segments[:len(w.segments)-1]
		total -= size
		deleted++
	}
	return deleted, nil
}

// allEventsMatch verifies CRCs, contiguous offsets and the complete logical
// segment, rather than treating a damaged record as the end of a valid prefix.
func (s *Segment) allEventsMatch(ctx context.Context, eligible func(*types.Event) bool) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return false, fmt.Errorf("segment closed")
	}
	if s.lastOffset < s.firstOffset {
		return false, nil
	}
	var lengthBytes [4]byte
	var record []byte
	next := s.firstOffset
	for pos := int64(64); pos < s.sizeBytes; {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		if _, err := s.segmentFile.ReadAt(lengthBytes[:], pos); err != nil {
			return false, err
		}
		length := int64(binary.BigEndian.Uint32(lengthBytes[:]))
		if length < 32 || length > 10*1024*1024 || length > s.sizeBytes-pos {
			return false, fmt.Errorf("invalid record length %d at %d", length, pos)
		}
		if cap(record) < int(length-4) {
			record = make([]byte, length-4)
		}
		record = record[:length-4]
		if _, err := s.segmentFile.ReadAt(record, pos+4); err != nil {
			return false, err
		}
		plain, err := s.decryptRecord(record, pos+4)
		if err != nil {
			return false, err
		}
		event, err := parseEventRecordWithoutLength(plain)
		if err != nil {
			return false, err
		}
		if event.Offset != next {
			return false, fmt.Errorf("offset gap: got %d want %d", event.Offset, next)
		}
		if !eligible(event) {
			return false, nil
		}
		next++
		pos += length
	}
	if next != s.lastOffset+1 {
		return false, fmt.Errorf("incomplete segment: next %d last %d", next, s.lastOffset)
	}
	return true, nil
}

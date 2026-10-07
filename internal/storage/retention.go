package storage

import (
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"slices"
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
	// PrefixOnly lets only the oldest segments go: pruning stops at the first
	// segment that has to stay, so that the log remains one unbroken range of
	// offsets. A replicated log is pruned this way, because its replicas are
	// kept in step by where the log starts and where it ends.
	PrefixOnly bool
}

// Prune deletes completed segments. Memory during verification is bounded by
// one record. eligible must not call back into this WAL. A nil predicate fails
// closed.
//
// A segment is read through without the lock that appends take. Only closed
// segments are candidates, and a closed segment does not gain entries; reading
// a whole segment under that lock stopped every publish to the partition for
// as long as the read took.
func (w *WAL) Prune(ctx context.Context, opts PruneOptions, eligible func(*types.Event) bool) (int, error) {
	// Held throughout: a checkpoint being taken must not lose a file it has
	// listed.
	w.checkpointMu.Lock()
	defer w.checkpointMu.Unlock()
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if eligible == nil {
		return 0, nil
	}

	w.mu.RLock()
	candidates := slices.Clone(w.segments)
	active := w.activeSegment
	var total int64
	for _, seg := range candidates {
		total += seg.GetSize()
	}
	w.mu.RUnlock()

	deleted := 0
	now := time.Now().UnixMilli()
	for _, seg := range candidates {
		selected := opts.AllCompleted || (opts.Filename != "" && seg.filename == opts.Filename) ||
			(opts.BeforeOffset > 0 && seg.lastOffset < opts.BeforeOffset) || opts.BeforeTimestamp > 0 ||
			(opts.MaxBytes > 0 && total > opts.MaxBytes)
		removed := false
		if seg != active && selected {
			ok, err := seg.allEventsMatch(ctx, func(event *types.Event) bool {
				event.PartitionId = w.partitionID // partition identity is implicit in the WAL directory
				// The caller is asked first, so that it learns which entry
				// is in the way even when the log would keep it regardless.
				return eligible(event) && event.ScheduleTs <= now && (opts.BeforeTimestamp <= 0 || event.ScheduleTs < opts.BeforeTimestamp)
			})
			if err != nil {
				return deleted, fmt.Errorf("verify retention of %s: %w", seg.filename, err)
			}
			if ok {
				size := seg.GetSize()
				if removed, err = w.unlinkSegment(seg); err != nil {
					return deleted, err
				}
				if removed {
					total -= size
					deleted++
				}
			}
		}
		if !removed && opts.PrefixOnly {
			break
		}
	}
	return deleted, nil
}

// unlinkSegment takes a closed segment out of the log and deletes its files.
// It reports false, and does nothing, when the segment left the log or became
// the one being appended to since the caller looked.
func (w *WAL) unlinkSegment(seg *Segment) (bool, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	index := slices.Index(w.segments, seg)
	if index < 0 || seg == w.activeSegment {
		return false, nil
	}
	if err := seg.Delete(); err != nil {
		return false, err
	}
	w.segments = slices.Delete(w.segments, index, index+1)
	return true, nil
}

// DropBelow removes the closed segments that lie wholly below offset, oldest
// first, and returns how many. It is how a replica follows the start of its
// leader's log: the leader has decided that those entries are no longer
// needed, so they are not examined here.
func (w *WAL) DropBelow(offset int64) (int, error) {
	w.checkpointMu.Lock()
	defer w.checkpointMu.Unlock()
	w.mu.Lock()
	defer w.mu.Unlock()

	dropped := 0
	for len(w.segments) > 0 {
		seg := w.segments[0]
		if seg == w.activeSegment || seg.GetLastOffset() < 0 || seg.GetLastOffset() >= offset {
			break
		}
		if err := seg.Delete(); err != nil {
			return dropped, fmt.Errorf("delete segment %s: %w", seg.GetFilename(), err)
		}
		w.segments = slices.Delete(w.segments, 0, 1)
		dropped++
	}
	return dropped, nil
}

// ResetTo empties the log and makes it start at offset: the next entry
// appended gets that offset. A replica does this when its log ends before the
// leader's begins. What lies between was released by the leader and cannot be
// sent any more, and nothing will ask for it again.
//
// Segments are removed oldest first, so a crash on the way leaves the end of
// the old log, which still ends before offset, and the reset happens again.
func (w *WAL) ResetTo(offset int64) error {
	w.checkpointMu.Lock()
	defer w.checkpointMu.Unlock()
	w.mu.Lock()
	defer w.mu.Unlock()

	if offset < 0 {
		return fmt.Errorf("log cannot start at offset %d", offset)
	}
	removed := w.nextOffset.Load() - func() int64 {
		if len(w.segments) == 0 {
			return w.nextOffset.Load()
		}
		return w.segments[0].GetFirstOffset()
	}()
	for len(w.segments) > 0 {
		seg := w.segments[0]
		if err := seg.Delete(); err != nil {
			return fmt.Errorf("delete segment %s: %w", seg.GetFilename(), err)
		}
		w.segments = slices.Delete(w.segments, 0, 1)
	}
	w.activeSegment = nil
	active, err := NewSegmentWithSize(w.dataDir, offset, true, w.cipher, w.config.SegmentSizeBytes)
	if err != nil {
		return fmt.Errorf("create segment at offset %d: %w", offset, err)
	}
	w.segments = append(w.segments, active)
	w.activeSegment = active
	w.nextOffset.Store(offset)
	w.appendSeq.Store(offset)
	w.highWatermark = offset - 1
	w.dirty.Store(true)

	log.Printf("[WAL-%d] Log restarted at offset %d; %d earlier entries removed", w.partitionID, offset, removed)
	return nil
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

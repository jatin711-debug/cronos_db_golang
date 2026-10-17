package partition

import (
	"context"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/internal/lifecycle"
	"github.com/jatin711-debug/cronos_db_golang/internal/storage"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

func (p *Partition) terminalEvent(e *types.Event) bool {
	if p.lifecycleIndex == nil {
		return false
	}
	active, err := p.lifecycleIndex.Active(e)
	if err != nil {
		p.Wal.Fail(err)
		return false
	}
	return !active
}

// evacuateFuture moves only never-due work, keeping consumer completion tied
// to its physical offsets. Called under pruneMu and lifecycleMu exclusively.
func (p *Partition) evacuateFuture(ctx context.Context, opts storage.PruneOptions, eligible func(*types.Event) bool) error {
	if p.lifecycleIndex == nil || p.relocationBytes <= 0 || opts.BeforeTimestamp > 0 {
		return nil
	}
	segments := p.Wal.GetSegments()
	if len(segments) < 2 {
		return nil
	}
	first := segments[0]
	var total int64
	for _, seg := range segments {
		total += seg.GetSize()
	}
	selected := opts.AllCompleted || opts.Filename == first.GetFilename() || (opts.BeforeOffset > 0 && first.GetLastOffset() < opts.BeforeOffset) || (opts.MaxBytes > 0 && total > opts.MaxBytes)
	if !selected {
		return nil
	}
	end := first.GetLastOffset()
	now := time.Now().UnixMilli()
	walk := func(visit func(*types.Event) (bool, error)) error {
		for from := first.GetFirstOffset(); from <= end; {
			if err := ctx.Err(); err != nil {
				return err
			}
			events, err := p.Wal.ReadEventsWithin(from, min(from+255, end), 4<<20)
			if err != nil {
				return err
			}
			if len(events) == 0 {
				return nil
			}
			from = events[len(events)-1].Offset + 1
			for _, e := range events {
				more, err := visit(e)
				if err != nil {
					return err
				}
				if !more {
					return nil
				}
			}
		}
		return nil
	}
	// Do not copy future work from a segment pinned by an unfinished due event.
	blocked := false
	if err := walk(func(e *types.Event) (bool, error) {
		active, err := p.lifecycleIndex.Active(e)
		if err != nil {
			return false, err
		}
		if active && e.ScheduleTs <= now && !eligible(e) {
			blocked = true
			return false, nil
		}
		return true, nil
	}); err != nil {
		return err
	}
	if blocked {
		return nil
	}
	remaining := p.relocationBytes
	return walk(func(e *types.Event) (bool, error) {
		active, err := p.lifecycleIndex.Active(e)
		if err != nil {
			return false, err
		}
		if !active || e.ScheduleTs <= time.Now().UnixMilli() {
			return true, nil
		}
		next, err := lifecycle.Replacement(e, lifecycle.Relocate, "", e.ScheduleTs)
		if err != nil {
			return false, err
		}
		// Include the actual WAL framing and the maximum cipher overhead.
		cost := int64(storage.EventRecordSize(next) + storage.MaxRecordLength - storage.MaxEventRecordSize)
		if cost > remaining {
			return false, nil
		}
		if p.diskAdmission != nil {
			if err = p.diskAdmission(); err != nil {
				return false, err
			}
		}
		if err = p.appendLifecycle(next, e); err != nil {
			return false, err
		}
		remaining -= cost
		return remaining > 0, nil
	})
}

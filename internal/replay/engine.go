// Package replay provides time-range and offset-based event replay over a WAL,
// including streaming delivery suitable for gRPC replay APIs.
package replay

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/internal/storage"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

// ReplayEngine reads historical events from a partition WAL for offline or
// online replay by time range or offset range.
type ReplayEngine struct {
	mu  sync.RWMutex
	wal *storage.WAL
}

// NewReplayEngine creates a ReplayEngine backed by the given WAL.
func NewReplayEngine(wal *storage.WAL) *ReplayEngine {
	return &ReplayEngine{
		wal: wal,
	}
}

// ReplayByTimeRange returns events whose schedule timestamps fall in [startTS, endTS].
// It prefers the WAL time index and falls back to a batched offset scan on failure.
func (r *ReplayEngine) ReplayByTimeRange(ctx context.Context, startTS, endTS int64) ([]*types.Event, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// Get all segments and scan for time range
	events, err := r.wal.ReadEventsByTime(startTS, endTS)
	if err != nil {
		// Fall back to offset-based scan in chunks if time index lookup fails.
		// FIX: Filter events by timestamp DURING the scan to prevent OOM
		// on large WALs. Previously all events were loaded into memory first.
		lastOffset := r.wal.GetLastOffset()
		if lastOffset < 0 {
			return []*types.Event{}, nil
		}

		const scanBatchSize int64 = 10000
		var filtered []*types.Event
		for batchStart := int64(0); batchStart <= lastOffset; batchStart += scanBatchSize {
			// Check context cancellation between batches
			select {
			case <-ctx.Done():
				return filtered, ctx.Err()
			default:
			}

			batchEnd := batchStart + scanBatchSize - 1
			if batchEnd > lastOffset {
				batchEnd = lastOffset
			}

			batchEvents, readErr := r.wal.ReadEvents(batchStart, batchEnd)
			if readErr != nil {
				return nil, fmt.Errorf("read events %d-%d: %w", batchStart, batchEnd, readErr)
			}

			// Filter in-place during scan instead of accumulating all events
			for _, event := range batchEvents {
				ts := event.GetScheduleTs()
				if ts >= startTS && ts <= endTS {
					filtered = append(filtered, event)
				}
			}
		}

		return filtered, nil
	}

	// Filter by timestamp range
	var filtered []*types.Event
	for _, event := range events {
		select {
		case <-ctx.Done():
			return filtered, ctx.Err()
		default:
		}

		ts := event.GetScheduleTs()
		if ts >= startTS && ts <= endTS {
			filtered = append(filtered, event)
		}
	}

	return filtered, nil
}

// ReplayByOffset returns up to count events starting at startOffset (inclusive),
// capped at the WAL's last available offset.
func (r *ReplayEngine) ReplayByOffset(ctx context.Context, startOffset, count int64) ([]*types.Event, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// Calculate end offset
	endOffset := startOffset + count - 1
	lastOffset := r.wal.GetLastOffset()

	// Cap at last available offset
	if endOffset > lastOffset {
		endOffset = lastOffset
	}

	if startOffset > lastOffset {
		return []*types.Event{}, nil
	}

	events, err := r.wal.ReadEvents(startOffset, endOffset)
	if err != nil {
		return nil, fmt.Errorf("read events: %w", err)
	}

	return events, nil
}

// ReplayStream loads events for req and sends them on eventCh, optionally paced
// by req.Speed. It always closes eventCh when finished.
func (r *ReplayEngine) ReplayStream(ctx context.Context, req *ReplayRequest, eventCh chan<- *ReplayEvent) error {
	defer close(eventCh)

	var events []*types.Event
	var err error

	// Determine replay mode
	if req.StartTS > 0 || req.EndTS > 0 {
		// Time-based replay
		endTS := req.EndTS
		if endTS == 0 {
			endTS = time.Now().UnixMilli()
		}
		events, err = r.ReplayByTimeRange(ctx, req.StartTS, endTS)
	} else {
		// Offset-based replay
		count := req.Count
		if count <= 0 {
			count = 1000 // Default limit
		}
		events, err = r.ReplayByOffset(ctx, req.StartOffset, count)
	}

	if err != nil {
		return fmt.Errorf("replay: %w", err)
	}

	// Stream events with optional speed control
	var delay time.Duration
	if req.Speed > 0 && req.Speed != 1.0 {
		delay = time.Duration(float64(time.Millisecond*10) / req.Speed)
	}

	// FIX: Use a single reusable timer instead of time.After() per event.
	// time.After() creates a new timer/channel/goroutine per call that leaks
	// until the timer fires. For 1M events, that's 1M leaked timers.
	sendTimer := time.NewTimer(30 * time.Second)
	defer sendTimer.Stop()

	for i, event := range events {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		replayEvent := &ReplayEvent{
			Event:        event,
			ReplayOffset: int64(i),
		}

		// Reset the timer for each send attempt
		if !sendTimer.Stop() {
			select {
			case <-sendTimer.C:
			default:
			}
		}
		sendTimer.Reset(30 * time.Second)

		select {
		case eventCh <- replayEvent:
		case <-ctx.Done():
			return ctx.Err()
		case <-sendTimer.C:
			return fmt.Errorf("timeout sending replay event")
		}

		if delay > 0 {
			time.Sleep(delay)
		}
	}

	return nil
}

// ReplayRequest describes a time-based or offset-based replay operation.
type ReplayRequest struct {
	// Topic is the logical topic being replayed (informational for multi-topic setups).
	Topic string
	// PartitionID is the partition whose WAL is replayed.
	PartitionID int32
	// StartTS is the inclusive lower bound schedule timestamp for time-based replay.
	StartTS int64
	// EndTS is the inclusive upper bound schedule timestamp; 0 means "now" for streaming.
	EndTS int64
	// StartOffset is the first WAL offset for offset-based replay.
	StartOffset int64
	// Count is the maximum number of events for offset-based replay (default 1000 when <= 0).
	Count int64
	// ConsumerGroup is an optional consumer group associated with the replay request.
	ConsumerGroup string
	// SubscriptionID is an optional subscription identifier for the replay client.
	SubscriptionID string
	// Speed controls streaming pace: 0 is fastest, 1 is approximately real-time pacing.
	Speed float64
}

// ReplayEvent is a single event emitted during a streaming replay, with its replay sequence index.
type ReplayEvent struct {
	// Event is the original stored event being replayed.
	Event *types.Event
	// ReplayOffset is the zero-based index of this event within the current replay stream.
	ReplayOffset int64
}

package replay

import (
	"context"
	"fmt"
	"sync"
	"time"

	"cronos_db/internal/storage"
	"cronos_db/pkg/types"
)

// ReplayEngine provides event replay functionality
type ReplayEngine struct {
	mu  sync.RWMutex
	wal *storage.WAL
}

// NewReplayEngine creates a new replay engine
func NewReplayEngine(wal *storage.WAL) *ReplayEngine {
	return &ReplayEngine{
		wal: wal,
	}
}

// ReplayByTimeRange replays events in a time range
func (r *ReplayEngine) ReplayByTimeRange(ctx context.Context, startTS, endTS int64) ([]*types.Event, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// Get all segments and scan for time range
	events, err := r.wal.ReadEventsByTime(startTS, endTS)
	if err != nil {
		// Fall back to offset-based scan if time-based not implemented
		// Read all events and filter by timestamp
		lastOffset := r.wal.GetLastOffset()
		if lastOffset < 0 {
			return []*types.Event{}, nil
		}

		events, err = r.wal.ReadEvents(0, lastOffset)
		if err != nil {
			return nil, fmt.Errorf("read events: %w", err)
		}
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

// ReplayByOffset replays events starting from an offset
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

// ReplayStream streams events for replay (for gRPC streaming)
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

		select {
		case eventCh <- replayEvent:
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(30 * time.Second):
			return fmt.Errorf("timeout sending replay event")
		}

		if delay > 0 {
			time.Sleep(delay)
		}
	}

	return nil
}

// ReplayRequest represents replay request
type ReplayRequest struct {
	Topic          string
	PartitionID    int32
	StartTS        int64
	EndTS          int64
	StartOffset    int64
	Count          int64
	ConsumerGroup  string
	SubscriptionID string
	Speed          float64 // 0=fastest, 1=real-time
}

// ReplayEvent represents replayed event
type ReplayEvent struct {
	Event        *types.Event
	ReplayOffset int64
}

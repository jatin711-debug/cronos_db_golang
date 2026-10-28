// Package replay provides time-range and offset-based event replay over a WAL,
// including streaming delivery suitable for gRPC replay APIs.
package replay

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/internal/logcontrol"
	"github.com/jatin711-debug/cronos_db_golang/internal/storage"
	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

// replayChunkBytes bounds how much of the log one read takes into memory. It is
// a byte bound and not a count of events, because one event can be megabytes.
const replayChunkBytes = 4 << 20

// ReplayEngine reads historical events from a partition WAL for offline or
// online replay by time range or offset range.
type ReplayEngine struct {
	mu  sync.RWMutex
	wal *storage.WAL
	// chunkBytes bounds each read of the log. NewReplayEngine sets it to replayChunkBytes.
	chunkBytes int64
}

// NewReplayEngine creates a ReplayEngine backed by the given WAL.
func NewReplayEngine(wal *storage.WAL) *ReplayEngine {
	return &ReplayEngine{
		wal:        wal,
		chunkBytes: replayChunkBytes,
	}
}

// ReplayByTimeRange returns events whose schedule timestamps fall in [startTS, endTS].
// The result is held in memory whole; ReplayStream streams the same events instead.
func (r *ReplayEngine) ReplayByTimeRange(ctx context.Context, startTS, endTS int64) ([]*types.Event, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return collect(ctx, r.timeCursor(startTS, endTS))
}

// ReplayByOffset returns up to count events starting at startOffset (inclusive),
// capped at the WAL's last available offset. The result is held in memory whole;
// ReplayStream streams the same events instead.
func (r *ReplayEngine) ReplayByOffset(ctx context.Context, startOffset, count int64) ([]*types.Event, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return collect(ctx, r.offsetCursor(startOffset, count))
}

// ReplayStream loads events for req and sends them on eventCh, optionally paced
// by req.Speed. It reads the log a chunk at a time, so it holds one chunk and not
// the whole range. It always closes eventCh when finished.
func (r *ReplayEngine) ReplayStream(ctx context.Context, req *ReplayRequest, eventCh chan<- *ReplayEvent) error {
	defer close(eventCh)

	var cursor *replayCursor
	if req.StartTS > 0 || req.EndTS > 0 {
		// Time-based replay
		endTS := req.EndTS
		if endTS == 0 {
			endTS = time.Now().UnixMilli()
		}
		cursor = r.timeCursor(req.StartTS, endTS)
	} else {
		// Offset-based replay
		count := req.Count
		if count <= 0 {
			count = 1000 // Default limit
		}
		cursor = r.offsetCursor(req.StartOffset, count)
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

	// The index counts the events the range yields, topic or not, as it always has.
	var index int64
	for {
		event, err := cursor.Next(ctx)
		if err != nil {
			return fmt.Errorf("replay: %w", err)
		}
		if event == nil {
			return nil
		}
		replayOffset := index
		index++

		if req.Topic != "" && event.GetTopic() != req.Topic {
			continue
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		replayEvent := &ReplayEvent{
			Event:        event,
			ReplayOffset: replayOffset,
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
			// Cancellation ends the pause. A plain sleep kept the replay,
			// its handler and the chunk it holds for the whole delay after
			// the client had gone.
			pause := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				pause.Stop()
				return ctx.Err()
			case <-pause.C:
			}
		}
	}
}

// replayCursor walks the events of a range of the log in offset order. It reads
// the log at most chunkBytes at a time and holds only the chunk it is handing out.
type replayCursor struct {
	wal        *storage.WAL
	chunkBytes int64
	keep       func(*types.Event) bool // nil keeps every event
	// byTime is set when the cursor walks a window of schedule timestamps,
	// [fromTS, toTS]: it then reads only the segments that can hold an event
	// in it (WAL.TimeSpan).
	byTime bool
	fromTS int64
	toTS   int64
	next   int64          // first offset the next read starts at
	last   int64          // last offset of the range, inclusive
	chunk  []*types.Event // events read and not yet handed out
}

// timeCursor walks the events whose schedule timestamps fall in [startTS, endTS],
// up to the last offset of the log now. Events are not in time order, so it
// looks at every event of each segment that can hold one in the window, and
// skips the segments that cannot.
func (r *ReplayEngine) timeCursor(startTS, endTS int64) *replayCursor {
	keep := func(event *types.Event) bool {
		ts := event.GetScheduleTs()
		return ts >= startTS && ts <= endTS
	}
	c := r.newCursor(0, r.wal.GetLastOffset(), keep)
	c.byTime, c.fromTS, c.toTS = true, startTS, endTS
	return c
}

// offsetCursor walks up to count events from startOffset, capped at the last
// offset of the log. Offsets start at zero, so a negative start reads from zero.
func (r *ReplayEngine) offsetCursor(startOffset, count int64) *replayCursor {
	last := r.wal.GetLastOffset()
	start := max(startOffset, 0)
	end := start - 1 // an empty range, unless count and the log say otherwise
	if count > 0 && start <= last {
		end = last
		if count-1 < last-start {
			end = start + count - 1
		}
	}
	return r.newCursor(start, end, nil)
}

// newCursor walks the offsets [start, end]. The range is fixed here, so an event
// appended while the replay runs is not part of it.
func (r *ReplayEngine) newCursor(start, end int64, keep func(*types.Event) bool) *replayCursor {
	return &replayCursor{
		wal:        r.wal,
		chunkBytes: r.chunkBytes,
		keep:       keep,
		next:       start,
		last:       end,
	}
}

// Next returns the next event of the range that the cursor keeps, or nil once
// the range is done. It checks ctx before each read of the log.
func (c *replayCursor) Next(ctx context.Context) (*types.Event, error) {
	for {
		if len(c.chunk) > 0 {
			event := c.chunk[0]
			c.chunk = c.chunk[1:]
			if logcontrol.IsTermStart(event) {
				if err := logcontrol.ValidateTermStart(event); err != nil {
					return nil, err
				}
				continue
			}
			if c.keep == nil || c.keep(event) {
				return event, nil
			}
			continue
		}
		if c.next > c.last {
			return nil, nil
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		end := c.last
		if c.byTime {
			first, spanEnd, ok := c.wal.TimeSpan(c.next, c.last, c.fromTS, c.toTS)
			if !ok {
				// No segment from here on holds an event in the window.
				c.next = c.last + 1
				continue
			}
			c.next, end = first, spanEnd
		}
		events, err := c.wal.ReadEventsWithin(c.next, end, c.chunkBytes)
		if err != nil {
			return nil, fmt.Errorf("read events %d-%d: %w", c.next, end, err)
		}
		if len(events) == 0 {
			// The log holds no event from here to the end of what was read.
			c.next = end + 1
			continue
		}
		c.next = events[len(events)-1].Offset + 1
		c.chunk = events
	}
}

// collect drains a cursor into a slice, which holds the whole range.
func collect(ctx context.Context, cursor *replayCursor) ([]*types.Event, error) {
	events := []*types.Event{}
	for {
		event, err := cursor.Next(ctx)
		if err != nil {
			return nil, err
		}
		if event == nil {
			return events, nil
		}
		events = append(events, event)
	}
}

// ReplayRequest describes a time-based or offset-based replay operation.
type ReplayRequest struct {
	// Topic restricts returned records to this topic. Public callers must set it.
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

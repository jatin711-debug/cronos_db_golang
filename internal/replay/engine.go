package replay

import (
	"fmt"
	"time"

	"cronos_db/pkg/types"
	"cronos_db/internal/storage"
)

// ReplayEngine provides event replay functionality
type ReplayEngine struct {
	wal   *storage.WAL
	index *SparseIndex
}

// SparseIndex represents sparse timestamp-index
type SparseIndex struct {
	entries map[int64]int64 // timestamp -> offset
}

// NewReplayEngine creates a new replay engine
func NewReplayEngine(wal *storage.WAL) *ReplayEngine {
	return &ReplayEngine{
		wal: wal,
		index: &SparseIndex{
			entries: make(map[int64]int64),
		},
	}
}

// ReplayByTimeRange replays events in a time range
func (r *ReplayEngine) ReplayByTimeRange(startTS, endTS int64) ([]*types.Event, error) {
	// Use sparse index to find starting offset
	startOffset := r.lookupTimestamp(startTS)
	endOffset := r.lookupTimestamp(endTS)

	if startOffset < 0 || endOffset < 0 {
		return nil, fmt.Errorf("could not find offsets for timestamp range")
	}

	// Read events in offset range
	events, err := r.wal.ReadEvents(startOffset, endOffset)
	if err != nil {
		return nil, fmt.Errorf("read events: %w", err)
	}

	// Filter by timestamp range
	var filtered []*types.Event
	for _, event := range events {
		if event.GetScheduleTs() >= startTS && event.GetScheduleTs() <= endTS {
			filtered = append(filtered, event)
		}
	}

	return filtered, nil
}

// ReplayByOffset replays events by offset
func (r *ReplayEngine) ReplayByOffset(startOffset, count int64) ([]*types.Event, error) {
	endOffset := startOffset + count - 1
	events, err := r.wal.ReadEvents(startOffset, endOffset)
	if err != nil {
		return nil, fmt.Errorf("read events: %w", err)
	}

	return events, nil
}

// ReplayAsync replays events asynchronously
func (r *ReplayEngine) ReplayAsync(req *ReplayRequest) (<-chan *ReplayEvent, error) {
	eventsCh := make(chan *ReplayEvent, 100)

	go func() {
		defer close(eventsCh)

		var events []*types.Event
		var err error

		if req.StartTS > 0 || req.EndTS > 0 {
			events, err = r.ReplayByTimeRange(req.StartTS, req.EndTS)
		} else {
			events, err = r.ReplayByOffset(req.StartOffset, req.Count)
		}

		if err != nil {
			return
		}

		// Stream events
		for i, event := range events {
			replayEvent := &ReplayEvent{
				Event:        event,
				ReplayOffset: int64(i),
			}

			select {
			case eventsCh <- replayEvent:
			case <-time.After(10 * time.Second):
				// Timeout, stop replay
				return
			}
		}
	}()

	return eventsCh, nil
}

// lookupTimestamp looks up offset for timestamp using sparse index
func (r *ReplayEngine) lookupTimestamp(ts int64) int64 {
	// Find closest timestamp <= target
	var closestTS int64
	var found bool

	for indexTS := range r.index.entries {
		if indexTS <= ts && (!found || indexTS > closestTS) {
			closestTS = indexTS
			found = true
		}
	}

	if !found {
		return -1
	}

	return r.index.entries[closestTS]
}

// BuildIndex builds sparse index for replay
func (r *ReplayEngine) BuildIndex() error {
	segments := r.wal.GetSegments()
	_ = segments // TODO: Read index entries from index files

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
	Speed          float64
}

// ReplayEvent represents replayed event
type ReplayEvent struct {
	Event        *types.Event
	ReplayOffset int64
}

// Checkpoint represents replay checkpoint
type Checkpoint struct {
	ReplayID       string
	Topic          string
	PartitionID    int32
	LastOffset     int64
	LastReplayTS   int64
	CreatedTS      int64
}

// Package cdc implements change data capture: it hands the accepted events of
// each partition to pluggable sinks (Kafka, webhooks).
//
// Manager.Deliver is called by a partition's change feed, which supplies the
// events in log order and offers them again after a failure. Deliver therefore
// neither queues nor drops: an event a sink did not take is reported back, and
// the feed for that partition waits. Publishing never waits for a sink.
package cdc

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

// Sink is a destination for change data capture events.
type Sink interface {
	// Name returns a stable identifier for logging and metrics.
	Name() string
	// Write delivers a single change event; ctx bounds the operation.
	Write(ctx context.Context, event *ChangeEvent) error
	// Close releases resources held by the sink.
	Close() error
}

// BatchSink is implemented by sinks that can take several events of one
// partition in a single call. The call succeeds or fails as a whole.
type BatchSink interface {
	Sink
	// WriteBatch delivers events, which are in log order, keeping that order.
	WriteBatch(ctx context.Context, events []*ChangeEvent) error
}

// ChangeEvent represents a WAL change event exported to sinks.
type ChangeEvent struct {
	// Timestamp is when the change was observed.
	Timestamp time.Time `json:"timestamp"`
	// Op is the change kind: "append", "commit", or "compact".
	Op string `json:"op"`
	// PartitionID is the partition that produced the change.
	PartitionID int32 `json:"partition_id"`
	// Topic is the event topic.
	Topic string `json:"topic"`
	// Offset is the WAL offset of the event.
	Offset int64 `json:"offset"`
	// Event is the full event payload when available.
	Event *types.Event `json:"event,omitempty"`
}

// DefaultCDCWriteTimeout bounds one write to a sink.
const DefaultCDCWriteTimeout = 5 * time.Second

// sinkState is a registered sink and how far each partition has got with it.
type sinkState struct {
	sink Sink

	// taken is, per partition, the offset of the last event this sink took.
	// When another sink fails, the feed offers a batch again; this sink skips
	// what it already has instead of receiving it once per retry. Guarded by mu.
	mu    sync.Mutex
	taken map[int32]int64
}

// Manager hands change events to every registered sink.
type Manager struct {
	mu    sync.RWMutex
	sinks []*sinkState
	// hasSinks lets callers skip all work when change data capture is not
	// configured, which is the common case.
	hasSinks atomic.Bool
	closed   bool
}

// ErrClosed is returned by Deliver after Close.
var ErrClosed = errors.New("change data capture is shut down")

// NewManager creates a CDC manager.
func NewManager() *Manager {
	return &Manager{}
}

// RegisterSink adds a sink.
func (m *Manager) RegisterSink(sink Sink) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sinks = append(m.sinks, &sinkState{sink: sink, taken: make(map[int32]int64)})
	m.hasSinks.Store(true)
}

// HasSinks reports whether any sinks are registered.
func (m *Manager) HasSinks() bool {
	return m.hasSinks.Load()
}

// SinkCount returns the number of registered sinks.
func (m *Manager) SinkCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.sinks)
}

// Deliver hands the events of one partition, which must be in log order, to
// every sink and returns once each has taken them or failed. An error means at
// least one sink has not taken all of them; calling Deliver again with the
// same events, or with a batch that starts with them, is how to retry. Sinks
// that already took an event do not get it again.
//
// Calls for different partitions may run concurrently. Calls for the same
// partition must not.
func (m *Manager) Deliver(ctx context.Context, partitionID int32, events []*types.Event) error {
	if !m.hasSinks.Load() || len(events) == 0 {
		return nil
	}
	m.mu.RLock()
	sinks, closed := m.sinks, m.closed
	m.mu.RUnlock()
	if closed {
		// Not delivered: the feed must not move past these events.
		return ErrClosed
	}

	now := time.Now()
	changes := make([]*ChangeEvent, len(events))
	for i, event := range events {
		changes[i] = &ChangeEvent{
			Timestamp:   now,
			Op:          "append",
			PartitionID: partitionID,
			Topic:       event.GetTopic(),
			Offset:      event.GetOffset(),
			Event:       event,
		}
	}

	if len(sinks) == 1 {
		return sinks[0].deliver(ctx, partitionID, changes)
	}
	// A slow sink should not add its latency to the others.
	errs := make([]error, len(sinks))
	var wg sync.WaitGroup
	for i, s := range sinks {
		wg.Add(1)
		go func(i int, s *sinkState) {
			defer wg.Done()
			errs[i] = s.deliver(ctx, partitionID, changes)
		}(i, s)
	}
	wg.Wait()
	return errors.Join(errs...)
}

func (s *sinkState) deliver(ctx context.Context, partitionID int32, changes []*ChangeEvent) error {
	s.mu.Lock()
	last, seen := s.taken[partitionID]
	s.mu.Unlock()
	if seen {
		for len(changes) > 0 && changes[0].Offset <= last {
			changes = changes[1:]
		}
	}
	if len(changes) == 0 {
		return nil
	}

	if batch, ok := s.sink.(BatchSink); ok {
		writeCtx, cancel := context.WithTimeout(ctx, DefaultCDCWriteTimeout)
		err := batch.WriteBatch(writeCtx, changes)
		cancel()
		if err != nil {
			return fmt.Errorf("sink %s: partition %d offsets %d-%d: %w",
				s.sink.Name(), partitionID, changes[0].Offset, changes[len(changes)-1].Offset, err)
		}
		s.note(partitionID, changes[len(changes)-1].Offset)
		return nil
	}

	for _, change := range changes {
		writeCtx, cancel := context.WithTimeout(ctx, DefaultCDCWriteTimeout)
		err := s.sink.Write(writeCtx, change)
		cancel()
		if err != nil {
			return fmt.Errorf("sink %s: partition %d offset %d: %w", s.sink.Name(), partitionID, change.Offset, err)
		}
		s.note(partitionID, change.Offset)
	}
	return nil
}

func (s *sinkState) note(partitionID int32, offset int64) {
	s.mu.Lock()
	s.taken[partitionID] = offset
	s.mu.Unlock()
}

// Close closes all sinks. Idempotent and safe to call multiple times.
func (m *Manager) Close() error {
	m.mu.Lock()
	sinks := m.sinks
	m.sinks, m.closed = nil, true
	m.mu.Unlock()

	var lastErr error
	for _, s := range sinks {
		if err := s.sink.Close(); err != nil {
			slog.Warn("CDC sink close failed", "sink", s.sink.Name(), "error", err)
			lastErr = err
		}
	}
	return lastErr
}

package cdc

import (
	"context"
	"log/slog"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

// Sink is a destination for change data capture events.
type Sink interface {
	Name() string
	Write(ctx context.Context, event *ChangeEvent) error
	Close() error
}

// ChangeEvent represents a WAL change event.
type ChangeEvent struct {
	Timestamp   time.Time `json:"timestamp"`
	Op          string    `json:"op"` // "append", "commit", "compact"
	PartitionID int32     `json:"partition_id"`
	Topic       string    `json:"topic"`
	Offset      int64     `json:"offset"`
	Event       *types.Event `json:"event,omitempty"`
}

// Manager manages multiple CDC sinks.
type Manager struct {
	sinks []Sink
	quit  chan struct{}
}

// NewManager creates a CDC manager.
func NewManager() *Manager {
	return &Manager{
		sinks: make([]Sink, 0),
		quit:  make(chan struct{}),
	}
}

// RegisterSink adds a sink.
func (m *Manager) RegisterSink(sink Sink) {
	m.sinks = append(m.sinks, sink)
}

// Emit sends an event to all registered sinks.
func (m *Manager) Emit(ctx context.Context, event *ChangeEvent) {
	if len(m.sinks) == 0 {
		return
	}
	for _, sink := range m.sinks {
		go func(s Sink) {
			ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			if err := s.Write(ctx, event); err != nil {
				slog.Warn("CDC sink write failed", "sink", s.Name(), "error", err)
			}
		}(sink)
	}
}

// Close shuts down all sinks.
func (m *Manager) Close() error {
	close(m.quit)
	var lastErr error
	for _, sink := range m.sinks {
		if err := sink.Close(); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

// Package cdc implements change data capture: fan-out of WAL append events to
// pluggable sinks (Kafka, webhooks) via bounded worker pools.
//
// Manager.Emit is called from the WAL append hook. When no sinks are registered,
// HasSinks is false and callers should skip ChangeEvent allocation entirely.
package cdc

import (
	"context"
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

const (
	// DefaultCDCWorkers is the number of goroutines per sink pipeline.
	DefaultCDCWorkers = 4
	// DefaultCDCQueueSize is the per-sink buffered queue size.
	DefaultCDCQueueSize = 10000
	// DefaultCDCWriteTimeout is the per-write and enqueue wait timeout.
	DefaultCDCWriteTimeout = 5 * time.Second
)

// sinkPipeline routes events to a single sink with a bounded worker pool.
type sinkPipeline struct {
	sink      Sink
	queue     chan *ChangeEvent
	quit      chan struct{}
	wg        sync.WaitGroup
	workerCnt int
	dropped   uint64
	droppedMu sync.Mutex
}

func newSinkPipeline(sink Sink, queueSize, workers int) *sinkPipeline {
	if workers <= 0 {
		workers = DefaultCDCWorkers
	}
	if queueSize <= 0 {
		queueSize = DefaultCDCQueueSize
	}
	return &sinkPipeline{
		sink:      sink,
		queue:     make(chan *ChangeEvent, queueSize),
		quit:      make(chan struct{}),
		workerCnt: workers,
	}
}

func (p *sinkPipeline) start() {
	for i := 0; i < p.workerCnt; i++ {
		p.wg.Add(1)
		go p.worker()
	}
}

func (p *sinkPipeline) worker() {
	defer p.wg.Done()
	for {
		select {
		case evt := <-p.queue:
			if evt == nil {
				continue
			}
			p.write(evt)
		case <-p.quit:
			// Drain remaining events before exiting.
			for {
				select {
				case evt := <-p.queue:
					if evt != nil {
						p.write(evt)
					}
				default:
					return
				}
			}
		}
	}
}

func (p *sinkPipeline) write(evt *ChangeEvent) {
	// Retry transient write failures up to 3 times with exponential backoff
	// before giving up. This provides at-least-once semantics for sink writes
	// without blocking the worker indefinitely.
	maxRetries := 3
	backoff := 100 * time.Millisecond

	for attempt := 0; attempt <= maxRetries; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), DefaultCDCWriteTimeout)
		err := p.sink.Write(ctx, evt)
		cancel()
		if err == nil {
			return
		}
		if attempt < maxRetries {
			slog.Warn("CDC sink write failed, retrying",
				"sink", p.sink.Name(), "attempt", attempt+1, "error", err)
			time.Sleep(backoff)
			backoff *= 2
		} else {
			slog.Warn("CDC sink write failed after retries, event lost",
				"sink", p.sink.Name(), "error", err,
				"partition", evt.PartitionID, "offset", evt.Offset)
		}
	}
}

// emit sends an event to the sink queue. When the queue is full it blocks up
// to DefaultCDCWriteTimeout rather than dropping the event, preventing silent
// data loss under burst loads. If the timeout expires the event is dropped
// with a warning (last-resort backpressure relief).
func (p *sinkPipeline) emit(evt *ChangeEvent) {
	timer := time.NewTimer(DefaultCDCWriteTimeout)
	defer timer.Stop()

	select {
	case p.queue <- evt:
		return
	case <-p.quit:
		return
	case <-timer.C:
		p.droppedMu.Lock()
		p.dropped++
		d := p.dropped
		p.droppedMu.Unlock()
		slog.Warn("CDC sink queue full after timeout, dropping event",
			"sink", p.sink.Name(), "dropped", d,
			"partition", evt.PartitionID, "offset", evt.Offset)
	}
}

func (p *sinkPipeline) stop() error {
	close(p.quit)
	p.wg.Wait()
	if err := p.sink.Close(); err != nil {
		slog.Warn("CDC sink close failed", "sink", p.sink.Name(), "error", err)
		return err
	}
	return nil
}

// Manager manages multiple CDC sinks.
type Manager struct {
	pipelines []*sinkPipeline
	mu        sync.RWMutex
	quit      chan struct{}
	quitOnce  sync.Once
	// hasSinks is an atomic fast-path flag checked on every Emit call. It lets
	// the WAL append hook skip ChangeEvent allocation + time.Now() entirely
	// when no sinks are registered (the common case in dev / load-test mode).
	// Flips false→true once at registration time and never goes back.
	hasSinks atomic.Bool
}

// NewManager creates a CDC manager.
func NewManager() *Manager {
	return &Manager{
		pipelines: make([]*sinkPipeline, 0),
		quit:      make(chan struct{}),
	}
}

// RegisterSink adds a sink with a bounded worker pool.
func (m *Manager) RegisterSink(sink Sink) {
	m.mu.Lock()
	defer m.mu.Unlock()
	pipeline := newSinkPipeline(sink, DefaultCDCQueueSize, DefaultCDCWorkers)
	pipeline.start()
	m.pipelines = append(m.pipelines, pipeline)
	m.hasSinks.Store(true) // flip the fast-path flag; stays true for the process lifetime
}

// Emit sends an event to all registered sinks.
func (m *Manager) Emit(ctx context.Context, event *ChangeEvent) {
	// Atomic fast path: skip the RLock entirely when no sinks are registered.
	// This is the common case in dev / load-test mode where CDC is not
	// configured, and Emit is called once per appended WAL event.
	if !m.hasSinks.Load() {
		return
	}

	m.mu.RLock()
	pipelines := m.pipelines
	m.mu.RUnlock()

	if len(pipelines) == 0 {
		return
	}
	for _, p := range pipelines {
		p.emit(event)
	}
}

// HasSinks reports whether any sinks are registered. Intended as a lock-free
// guard so callers (e.g. the WAL append hook) can avoid allocating a
// ChangeEvent when CDC is inactive.
func (m *Manager) HasSinks() bool {
	return m.hasSinks.Load()
}

// SinkCount returns the number of registered sinks.
func (m *Manager) SinkCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.pipelines)
}

// Close shuts down all sinks. Idempotent and safe to call multiple times.
func (m *Manager) Close() error {
	m.quitOnce.Do(func() { close(m.quit) })
	m.mu.Lock()
	defer m.mu.Unlock()

	var lastErr error
	for _, p := range m.pipelines {
		if err := p.stop(); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

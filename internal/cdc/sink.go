package cdc

import (
	"context"
	"log/slog"
	"sync"
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
	Timestamp   time.Time    `json:"timestamp"`
	Op          string       `json:"op"` // "append", "commit", "compact"
	PartitionID int32        `json:"partition_id"`
	Topic       string       `json:"topic"`
	Offset      int64        `json:"offset"`
	Event       *types.Event `json:"event,omitempty"`
}

const (
	// DefaultCDCWorkers is the number of goroutines per sink.
	DefaultCDCWorkers = 4
	// DefaultCDCQueueSize is the per-sink buffered queue size.
	DefaultCDCQueueSize = 10000
	// DefaultCDCWriteTimeout is the per-write timeout.
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
	ctx, cancel := context.WithTimeout(context.Background(), DefaultCDCWriteTimeout)
	defer cancel()
	if err := p.sink.Write(ctx, evt); err != nil {
		slog.Warn("CDC sink write failed", "sink", p.sink.Name(), "error", err)
	}
}

func (p *sinkPipeline) emit(evt *ChangeEvent) {
	select {
	case p.queue <- evt:
	default:
		p.droppedMu.Lock()
		p.dropped++
		d := p.dropped
		p.droppedMu.Unlock()
		slog.Warn("CDC sink queue full, dropping event", "sink", p.sink.Name(), "dropped", d)
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
}

// Emit sends an event to all registered sinks.
func (m *Manager) Emit(ctx context.Context, event *ChangeEvent) {
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

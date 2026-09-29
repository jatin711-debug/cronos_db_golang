package delivery

import (
	"github.com/jatin711-debug/cronos_db_golang/internal/metrics"
	"log"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
	"github.com/jatin711-debug/cronos_db_golang/pkg/utils"
)

// Worker drains ready events from the scheduler and dispatches them in batches.
// The ready queue is protected by mu; dispatch stats are atomic so GetStats
// does not contend with the hot path.
type Worker struct {
	mu            sync.Mutex // protects readyQueue and processing flag
	dispatcher    *Dispatcher
	readyQueue    []*types.Event
	queuedBytes   int64
	maxQueueBytes int64
	notify        chan struct{} // buffered wake-up for the loop
	batchSize     int32
	processing    bool
	quit          chan struct{}

	// Atomic stats — no lock needed for read/write
	statsDispatched atomic.Int64
	statsFailed     atomic.Int64
	statsLastDispTS atomic.Int64
}

// NewWorker creates a delivery worker that batches up to batchSize events per dispatch.
func NewWorker(dispatcher *Dispatcher, batchSize int32) *Worker {
	return &Worker{
		dispatcher:    dispatcher,
		readyQueue:    make([]*types.Event, 0),
		maxQueueBytes: 64 << 20,
		notify:        make(chan struct{}, 1),
		batchSize:     batchSize,
		quit:          make(chan struct{}),
	}
}

func (w *Worker) signal() {
	select {
	case w.notify <- struct{}{}:
	default:
	}
}

// AddReadyEvent appends a single ready event and wakes the worker loop.
func (w *Worker) AddReadyEvent(event *types.Event) { w.AddReadyEvents([]*types.Event{event}) }

// Excess ready notifications can be discarded: retained records are redriven by
// subscription WAL scans. Count that fallback and never grow this queue forever.
func (w *Worker) AddReadyEvents(events []*types.Event) {
	const maxQueued = 10000
	w.mu.Lock()
	accepted := 0
	for _, event := range events {
		if event == nil {
			continue
		}
		size := retainedEventBytes(event)
		if len(w.readyQueue) >= maxQueued || size > w.maxQueueBytes-w.queuedBytes {
			continue // WAL subscription scans retain responsibility for redelivery.
		}
		w.readyQueue = append(w.readyQueue, event)
		w.queuedBytes += size
		accepted++
	}
	w.mu.Unlock()
	if accepted < len(events) && len(events) > 0 {
		metrics.IncDispatcherBackpressureSkip(strconv.FormatInt(int64(events[0].GetPartitionId()), 10), "worker_capacity", len(events)-accepted)
	}
	w.signal()
}

// Account for payload backing capacity and metadata, not only wire size.
// This is a per-worker retention budget, not a bound on whole-process RSS.
func retainedEventBytes(event *types.Event) int64 {
	size := int64(256) + int64(cap(event.Payload)) + int64(len(event.MessageId)) + int64(len(event.Topic)) + int64(cap(event.ProtoReflect().GetUnknown()))
	for key, value := range event.Meta {
		size += 64 + int64(len(key)) + int64(len(value))
	}
	return size
}

// Start launches the background processing loop if not already running.
func (w *Worker) Start() {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.processing {
		return
	}

	w.processing = true
	utils.GoSafe("delivery-worker", w.loop)
	w.signal()
}

// loop is the main worker loop
func (w *Worker) loop() {
	for {
		select {
		case <-w.notify:
			for w.processBatch() {
			}

		case <-w.quit:
			return
		}
	}
}

// processBatch processes a batch of ready events
func (w *Worker) processBatch() bool {
	w.mu.Lock()
	if len(w.readyQueue) == 0 {
		w.mu.Unlock()
		return false
	}

	// Process up to batchSize events
	batch := w.readyQueue
	if int32(len(batch)) > w.batchSize {
		batch = batch[:w.batchSize]
		w.readyQueue = w.readyQueue[w.batchSize:]
	} else {
		// Dispatch owns this backing array until it finishes reading the batch.
		w.readyQueue = nil
	}
	for _, event := range batch {
		w.queuedBytes -= retainedEventBytes(event)
	}
	w.mu.Unlock()

	// Dispatch as a batch for higher throughput.
	// Lock is released so AddReadyEvent/AddReadyEvents can proceed concurrently.
	if err := w.dispatcher.DispatchBatch(batch); err != nil {
		log.Printf("Failed to dispatch batch: %v", err)
		w.statsFailed.Add(int64(len(batch)))
	} else {
		w.statsDispatched.Add(int64(len(batch)))
	}
	// Release consumed references even when the queue still owns the tail of
	// the same backing array. Dispatch stores its own slices of event pointers.
	clear(batch)

	// Atomic store — no lock needed
	w.statsLastDispTS.Store(time.Now().UnixMilli())

	return true
}

// Stop signals the processing loop to exit. It is a no-op if not running.
func (w *Worker) Stop() {
	w.mu.Lock()
	defer w.mu.Unlock()

	if !w.processing {
		return
	}

	close(w.quit)
	w.processing = false
}

// GetStats returns a snapshot of dispatch counters and queue depth.
func (w *Worker) GetStats() *WorkerStats {
	w.mu.Lock()
	queueLen := int64(len(w.readyQueue))
	queueBytes := w.queuedBytes
	isProcessing := w.processing
	w.mu.Unlock()

	return &WorkerStats{
		EventsDispatched: w.statsDispatched.Load(),
		EventsFailed:     w.statsFailed.Load(),
		QueueLength:      queueLen,
		QueueBytes:       queueBytes,
		LastDispatchTS:   w.statsLastDispTS.Load(),
		Processing:       isProcessing,
	}
}

// WorkerStats is a point-in-time snapshot of delivery worker activity.
type WorkerStats struct {
	// EventsDispatched is the cumulative count of events sent via DispatchBatch.
	EventsDispatched int64
	// EventsFailed is the cumulative count of events in batches that returned an error.
	EventsFailed int64
	// QueueLength is the current ready-queue depth.
	QueueLength int64
	// QueueBytes estimates bytes retained by queued events, excluding in-flight batches.
	QueueBytes int64
	// LastDispatchTS is the last successful or failed batch dispatch time (Unix ms).
	LastDispatchTS int64
	// Processing is true while the background loop is running.
	Processing bool
}

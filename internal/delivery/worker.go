package delivery

import (
	"log"
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
	mu         sync.Mutex // protects readyQueue and processing flag
	dispatcher *Dispatcher
	readyQueue []*types.Event
	notify     chan struct{} // buffered wake-up for the loop
	batchSize  int32
	processing bool
	quit       chan struct{}

	// Atomic stats — no lock needed for read/write
	statsDispatched atomic.Int64
	statsFailed     atomic.Int64
	statsLastDispTS atomic.Int64
}

// NewWorker creates a delivery worker that batches up to batchSize events per dispatch.
func NewWorker(dispatcher *Dispatcher, batchSize int32) *Worker {
	return &Worker{
		dispatcher: dispatcher,
		readyQueue: make([]*types.Event, 0),
		notify:     make(chan struct{}, 1),
		batchSize:  batchSize,
		quit:       make(chan struct{}),
	}
}

func (w *Worker) signal() {
	select {
	case w.notify <- struct{}{}:
	default:
	}
}

// AddReadyEvent appends a single ready event and wakes the worker loop.
func (w *Worker) AddReadyEvent(event *types.Event) {
	w.mu.Lock()
	w.readyQueue = append(w.readyQueue, event)
	w.mu.Unlock()
	w.signal()
}

// AddReadyEvents adds multiple ready events in a single lock acquisition.
// Use this when draining the scheduler to reduce lock contention.
func (w *Worker) AddReadyEvents(events []*types.Event) {
	w.mu.Lock()
	w.readyQueue = append(w.readyQueue, events...)
	w.mu.Unlock()
	w.signal()
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
		w.readyQueue = w.readyQueue[:0] // Reuse backing array
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
	isProcessing := w.processing
	w.mu.Unlock()

	return &WorkerStats{
		EventsDispatched: w.statsDispatched.Load(),
		EventsFailed:     w.statsFailed.Load(),
		QueueLength:      queueLen,
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
	// LastDispatchTS is the last successful or failed batch dispatch time (Unix ms).
	LastDispatchTS int64
	// Processing is true while the background loop is running.
	Processing bool
}

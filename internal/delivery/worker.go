package delivery

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

// Worker processes ready events from scheduler
type Worker struct {
	mu         sync.Mutex // Only protects readyQueue and processing flag
	dispatcher *Dispatcher
	readyQueue []*types.Event
	notify     chan struct{}
	batchSize  int32
	processing bool
	quit       chan struct{}

	// Atomic stats — no lock needed for read/write
	statsDispatched atomic.Int64
	statsFailed     atomic.Int64
	statsLastDispTS atomic.Int64
}

// NewWorker creates a new delivery worker
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

// AddReadyEvent adds a ready event to the queue
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

// Start starts the worker
func (w *Worker) Start() {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.processing {
		return
	}

	w.processing = true
	go w.loop()
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

// Stop stops the worker
func (w *Worker) Stop() {
	w.mu.Lock()
	defer w.mu.Unlock()

	if !w.processing {
		return
	}

	close(w.quit)
	w.processing = false
}

// GetStats returns worker statistics
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

// WorkerStats represents worker statistics
type WorkerStats struct {
	EventsDispatched int64
	EventsFailed     int64
	QueueLength      int64
	LastDispatchTS   int64
	Processing       bool
}

package delivery

import (
	"log"
	"sync"
	"time"

	"cronos_db/pkg/types"
)

// Worker processes ready events from scheduler
type Worker struct {
	mu              sync.RWMutex
	dispatcher      *Dispatcher
	readyQueue      []*types.Event
	batchSize       int32
	processing      bool
	quit            chan struct{}
	stats           *WorkerStats
}

// NewWorker creates a new delivery worker
func NewWorker(dispatcher *Dispatcher, batchSize int32) *Worker {
	return &Worker{
		dispatcher: dispatcher,
		readyQueue: make([]*types.Event, 0),
		batchSize:  batchSize,
		quit:       make(chan struct{}),
		stats:      &WorkerStats{},
	}
}

// AddReadyEvent adds a ready event to the queue
func (w *Worker) AddReadyEvent(event *types.Event) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.readyQueue = append(w.readyQueue, event)
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
}

// loop is the main worker loop
func (w *Worker) loop() {
	ticker := time.NewTicker(10 * time.Millisecond) // Process every 10ms
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			w.processBatch()

		case <-w.quit:
			return
		}
	}
}

// processBatch processes a batch of ready events
func (w *Worker) processBatch() {
	w.mu.Lock()
	defer w.mu.Unlock()

	if len(w.readyQueue) == 0 {
		return
	}

	// Process up to batchSize events
	batch := w.readyQueue
	if int32(len(batch)) > w.batchSize {
		batch = batch[:w.batchSize]
		w.readyQueue = w.readyQueue[w.batchSize:]
	} else {
		w.readyQueue = make([]*types.Event, 0)
	}

	// Dispatch events
	for _, event := range batch {
		if err := w.dispatcher.Dispatch(event); err != nil {
			log.Printf("Failed to dispatch event %s: %v", event.GetMessageId(), err)
			w.stats.EventsFailed++
			continue
		}
		w.stats.EventsDispatched++
	}

	w.stats.LastDispatchTS = time.Now().UnixMilli()
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
	w.mu.RLock()
	defer w.mu.RUnlock()

	return &WorkerStats{
		EventsDispatched: w.stats.EventsDispatched,
		EventsFailed:     w.stats.EventsFailed,
		QueueLength:      int64(len(w.readyQueue)),
		LastDispatchTS:   w.stats.LastDispatchTS,
		Processing:       w.processing,
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

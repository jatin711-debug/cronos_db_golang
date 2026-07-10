package audit

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/internal/auth"
)

const (
	DefaultBufferSize = 4096

	// auditSyncEvery is the number of encoded events after which the audit log
	// file is explicitly flushed and fsynced. This keeps fsyncs off the gRPC
	// hot path while bounding durability window.
	auditSyncEvery = 100

	// auditFlushInterval is the maximum time between fsyncs when events are
	// trickling in slowly.
	auditFlushInterval = 1 * time.Second
)

// Event represents a single audit log entry.
type Event struct {
	Timestamp time.Time `json:"timestamp"`
	Action    string    `json:"action"`
	Subject   string    `json:"subject"`
	Resource  string    `json:"resource"`
	Outcome   string    `json:"outcome"`
	Detail    string    `json:"detail,omitempty"`
	SourceIP  string    `json:"source_ip,omitempty"`
	RequestID string    `json:"request_id,omitempty"`
}

// Logger writes audit events to an append-only log file.
//
// Events are buffered in a channel and flushed by a background worker so the
// gRPC handler path never blocks on disk I/O. If the buffer fills up, new events
// are dropped and a warning is logged.
type Logger struct {
	mu        sync.Mutex
	file      *os.File
	bufWriter *bufio.Writer
	encoder   *json.Encoder
	logDir    string
	unflushed int

	events  chan Event
	quit    chan struct{}
	closed  atomic.Bool
	wg      sync.WaitGroup
	dropped atomic.Uint64
}

// NewLogger creates an audit logger and starts the background writer.
func NewLogger(dataDir string) (*Logger, error) {
	logDir := filepath.Join(dataDir, "audit")
	if err := os.MkdirAll(logDir, 0750); err != nil {
		return nil, fmt.Errorf("create audit dir: %w", err)
	}

	logFile := filepath.Join(logDir, time.Now().UTC().Format("2006-01-02")+".ndjson")
	f, err := os.OpenFile(logFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0640)
	if err != nil {
		return nil, fmt.Errorf("open audit log: %w", err)
	}

	l := &Logger{
		file:      f,
		bufWriter: bufio.NewWriterSize(f, 64*1024),
		logDir:    logDir,
		events:    make(chan Event, DefaultBufferSize),
		quit:      make(chan struct{}),
	}
	l.encoder = json.NewEncoder(l.bufWriter)

	l.wg.Add(1)
	go l.worker()

	return l, nil
}

// Log enqueues an audit event. If the buffer is full, the event is dropped and
// a warning is emitted. This method is safe for concurrent use and never
// blocks on disk I/O.
func (l *Logger) Log(evt Event) {
	if l == nil {
		return
	}
	if l.closed.Load() {
		return
	}
	evt.Timestamp = time.Now().UTC()

	select {
	case l.events <- evt:
	default:
		l.dropped.Add(1)
		slog.Warn("audit log buffer full, dropping event",
			"action", evt.Action, "subject", evt.Subject, "dropped", l.dropped.Load())
	}
}

// LogGRPC creates an audit event from a gRPC context.
func (l *Logger) LogGRPC(ctx context.Context, action, resource, outcome, detail string) {
	subject := "anonymous"
	if claims, ok := auth.ClaimsFromContext(ctx); ok {
		subject = claims.Subject
	}
	l.Log(Event{
		Action:   action,
		Subject:  subject,
		Resource: resource,
		Outcome:  outcome,
		Detail:   detail,
	})
}

// Dropped returns the number of events dropped due to a full buffer.
func (l *Logger) Dropped() uint64 {
	if l == nil {
		return 0
	}
	return l.dropped.Load()
}

// Flush blocks until all queued events have been written to disk. It stops and
// restarts the background worker, so it should not be called on the hot path.
func (l *Logger) Flush() error {
	if l == nil || l.closed.Load() {
		return nil
	}

	close(l.quit)
	l.wg.Wait()

	l.quit = make(chan struct{})
	l.wg.Add(1)
	go l.worker()
	return nil
}

// worker drains the event queue and writes each event to disk. It exits when
// quit is closed, flushing any remaining queued events before returning.
func (l *Logger) worker() {
	defer l.wg.Done()
	ticker := time.NewTicker(auditFlushInterval)
	defer ticker.Stop()

	for {
		select {
		case evt := <-l.events:
			l.writeLocked(evt)
		case <-ticker.C:
			l.flushAndSyncLocked()
		case <-l.quit:
			// Drain the remaining buffered events before stopping.
			for {
				select {
				case evt := <-l.events:
					l.writeLocked(evt)
				default:
					l.flushAndSyncLocked()
					return
				}
			}
		}
	}
}

// writeLocked writes a single event to the audit log file under the mutex.
func (l *Logger) writeLocked(evt Event) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.encoder.Encode(evt); err != nil {
		slog.Warn("Audit log encode failed", "error", err)
		return
	}
	l.unflushed++
	if l.unflushed >= auditSyncEvery {
		l.flushAndSyncLocked()
	}
}

// flushAndSyncLocked flushes the buffered audit writer and fsyncs the file.
// l.mu must NOT be held; it acquires the lock internally.
func (l *Logger) flushAndSyncLocked() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.bufWriter == nil {
		return
	}
	if l.unflushed == 0 {
		return
	}
	if err := l.bufWriter.Flush(); err != nil {
		slog.Warn("Audit log flush failed", "error", err)
		return
	}
	if err := l.file.Sync(); err != nil {
		slog.Warn("Audit log sync failed", "error", err)
		return
	}
	l.unflushed = 0
}

// Close signals the background worker to stop, waits for queued events to be
// flushed, and then closes the audit log file. It is safe to call multiple
// times.
func (l *Logger) Close() error {
	if l == nil {
		return nil
	}
	if !l.closed.CompareAndSwap(false, true) {
		return nil
	}

	close(l.quit)
	l.wg.Wait()

	l.mu.Lock()
	defer l.mu.Unlock()
	if l.bufWriter != nil && l.unflushed > 0 {
		if err := l.bufWriter.Flush(); err != nil {
			slog.Warn("Audit log flush on close failed", "error", err)
		} else if err := l.file.Sync(); err != nil {
			slog.Warn("Audit log sync on close failed", "error", err)
		} else {
			l.unflushed = 0
		}
	}
	return l.file.Close()
}

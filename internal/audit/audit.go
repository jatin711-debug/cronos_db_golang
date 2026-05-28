package audit

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/internal/auth"
)

// Event represents a single audit log entry.
type Event struct {
	Timestamp   time.Time `json:"timestamp"`
	Action      string    `json:"action"`
	Subject     string    `json:"subject"`
	Resource    string    `json:"resource"`
	Outcome     string    `json:"outcome"`
	Detail      string    `json:"detail,omitempty"`
	SourceIP    string    `json:"source_ip,omitempty"`
	RequestID   string    `json:"request_id,omitempty"`
}

// Logger writes audit events to an append-only log file.
type Logger struct {
	mu       sync.Mutex
	file     *os.File
	encoder  *json.Encoder
	logDir   string
}

// NewLogger creates an audit logger.
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

	return &Logger{
		file:    f,
		encoder: json.NewEncoder(f),
		logDir:  logDir,
	}, nil
}

// Log writes an audit event asynchronously.
func (l *Logger) Log(evt Event) {
	if l == nil {
		return
	}
	evt.Timestamp = time.Now().UTC()
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.encoder.Encode(evt); err != nil {
		slog.Warn("Audit log encode failed", "error", err)
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

// Close closes the audit log file.
func (l *Logger) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.file.Close()
}

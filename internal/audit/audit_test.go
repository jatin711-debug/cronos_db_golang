package audit

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jatin711-debug/cronos_db_golang/internal/auth"
)

func TestNewLogger(t *testing.T) {
	tmpDir := t.TempDir()
	l, err := NewLogger(tmpDir)
	if err != nil {
		t.Fatalf("NewLogger failed: %v", err)
	}
	defer l.Close()

	if l == nil {
		t.Fatal("logger should not be nil")
	}
	if l.file == nil {
		t.Fatal("file should not be nil")
	}
}

func TestNewLogger_CreateDir(t *testing.T) {
	tmpDir := t.TempDir()
	nested := filepath.Join(tmpDir, "deep", "nested")
	l, err := NewLogger(nested)
	if err != nil {
		t.Fatalf("NewLogger failed: %v", err)
	}
	defer l.Close()

	if _, err := os.Stat(nested); os.IsNotExist(err) {
		t.Fatal("audit dir should be created")
	}
}

func TestLogger_Log(t *testing.T) {
	tmpDir := t.TempDir()
	l, err := NewLogger(tmpDir)
	if err != nil {
		t.Fatalf("NewLogger failed: %v", err)
	}
	defer l.Close()

	evt := Event{
		Action:   "publish",
		Subject:  "user-1",
		Resource: "topic/orders",
		Outcome:  "success",
		Detail:   "event-id-123",
		SourceIP: "127.0.0.1",
	}
	l.Log(evt)

	// Verify log file exists
	files, err := os.ReadDir(l.logDir)
	if err != nil {
		t.Fatalf("read audit dir: %v", err)
	}
	if len(files) != 1 {
		t.Fatalf("expected 1 log file, got %d", len(files))
	}

	data, err := os.ReadFile(filepath.Join(l.logDir, files[0].Name()))
	if err != nil {
		t.Fatalf("read log file: %v", err)
	}
	content := string(data)
	if !strings.Contains(content, "publish") {
		t.Error("log should contain action")
	}
	if !strings.Contains(content, "user-1") {
		t.Error("log should contain subject")
	}
	if !strings.Contains(content, "topic/orders") {
		t.Error("log should contain resource")
	}
}

func TestLogger_Log_NilReceiver(t *testing.T) {
	var l *Logger
	// Should not panic
	l.Log(Event{Action: "test"})
}

func TestLogger_LogGRPC_Anonymous(t *testing.T) {
	tmpDir := t.TempDir()
	l, err := NewLogger(tmpDir)
	if err != nil {
		t.Fatalf("NewLogger failed: %v", err)
	}
	defer l.Close()

	ctx := context.Background()
	l.LogGRPC(ctx, "publish", "topic/orders", "success", "detail")

	files, _ := os.ReadDir(l.logDir)
	if len(files) != 1 {
		t.Fatal("expected log file")
	}
	data, _ := os.ReadFile(filepath.Join(l.logDir, files[0].Name()))
	if !strings.Contains(string(data), "anonymous") {
		t.Error("should use anonymous subject when no claims")
	}
}

func TestLogger_LogGRPC_WithClaims(t *testing.T) {
	tmpDir := t.TempDir()
	l, err := NewLogger(tmpDir)
	if err != nil {
		t.Fatalf("NewLogger failed: %v", err)
	}
	defer l.Close()

	claims := auth.ClaimsWithSubject("user-42")
	ctx := auth.WithClaims(context.Background(), claims)
	l.LogGRPC(ctx, "subscribe", "topic/orders", "denied", "quota exceeded")

	files, _ := os.ReadDir(l.logDir)
	data, _ := os.ReadFile(filepath.Join(l.logDir, files[0].Name()))
	if !strings.Contains(string(data), "user-42") {
		t.Error("should use claims subject")
	}
}

func TestLogger_Close(t *testing.T) {
	tmpDir := t.TempDir()
	l, _ := NewLogger(tmpDir)
	if err := l.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	// Double close should be safe (file already closed)
	_ = l.Close()
}

func TestEvent_Timestamp(t *testing.T) {
	tmpDir := t.TempDir()
	l, _ := NewLogger(tmpDir)
	defer l.Close()

	l.Log(Event{Action: "test"})

	files, _ := os.ReadDir(l.logDir)
	data, _ := os.ReadFile(filepath.Join(l.logDir, files[0].Name()))
	content := string(data)

	// Should have a timestamp field
	if !strings.Contains(content, "timestamp") {
		t.Error("should have timestamp field")
	}
	if strings.Contains(content, "0001-01-01") {
		t.Error("timestamp should be set, not zero value")
	}
}

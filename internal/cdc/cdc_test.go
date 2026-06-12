package cdc

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jatin711-debug/cronos_db_golang/pkg/types"
)

func TestNewManager(t *testing.T) {
	m := NewManager()
	if m == nil {
		t.Fatal("NewManager should not return nil")
	}
	if len(m.sinks) != 0 {
		t.Errorf("expected 0 sinks, got %d", len(m.sinks))
	}
}

func TestManager_RegisterSink(t *testing.T) {
	m := NewManager()
	sink := &mockSink{name: "mock"}
	m.RegisterSink(sink)

	if len(m.sinks) != 1 {
		t.Errorf("expected 1 sink, got %d", len(m.sinks))
	}
}

func TestManager_Emit_NoSinks(t *testing.T) {
	m := NewManager()
	// Should not panic with no sinks
	m.Emit(context.Background(), &ChangeEvent{
		Op:          "append",
		PartitionID: 0,
		Topic:       "test",
		Offset:      1,
	})
}

func TestManager_Emit_SingleSink(t *testing.T) {
	m := NewManager()
	sink := &mockSink{name: "mock"}
	m.RegisterSink(sink)

	m.Emit(context.Background(), &ChangeEvent{
		Op:          "append",
		PartitionID: 0,
		Topic:       "test",
		Offset:      1,
	})

	// Give goroutine time to execute
	time.Sleep(100 * time.Millisecond)

	if sink.writeCount.Load() != 1 {
		t.Errorf("expected 1 write, got %d", sink.writeCount.Load())
	}
}

func TestManager_Emit_MultipleSinks(t *testing.T) {
	m := NewManager()
	sink1 := &mockSink{name: "mock1"}
	sink2 := &mockSink{name: "mock2"}
	m.RegisterSink(sink1)
	m.RegisterSink(sink2)

	m.Emit(context.Background(), &ChangeEvent{
		Op:          "append",
		PartitionID: 0,
		Topic:       "test",
		Offset:      1,
	})

	time.Sleep(100 * time.Millisecond)

	if sink1.writeCount.Load() != 1 {
		t.Errorf("expected sink1 to receive 1 write, got %d", sink1.writeCount.Load())
	}
	if sink2.writeCount.Load() != 1 {
		t.Errorf("expected sink2 to receive 1 write, got %d", sink2.writeCount.Load())
	}
}

func TestManager_Emit_SinkError(t *testing.T) {
	m := NewManager()
	sink := &mockSink{name: "errmock", err: fmt.Errorf("write failed")}
	m.RegisterSink(sink)

	m.Emit(context.Background(), &ChangeEvent{
		Op:          "append",
		PartitionID: 0,
		Topic:       "test",
		Offset:      1,
	})

	time.Sleep(100 * time.Millisecond)
	// Should not panic even if sink errors
	if sink.writeCount.Load() != 1 {
		t.Errorf("expected 1 write attempt, got %d", sink.writeCount.Load())
	}
}

func TestManager_Close(t *testing.T) {
	m := NewManager()
	sink := &mockSink{name: "mock"}
	m.RegisterSink(sink)

	err := m.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	if !sink.closed.Load() {
		t.Error("sink should be closed")
	}
}

func TestManager_Close_Error(t *testing.T) {
	m := NewManager()
	sink := &mockSink{name: "errmock", closeErr: fmt.Errorf("close failed")}
	m.RegisterSink(sink)

	err := m.Close()
	if err == nil {
		t.Error("expected error from sink close")
	}
}

func TestChangeEvent_JSON(t *testing.T) {
	evt := ChangeEvent{
		Timestamp:   time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		Op:          "append",
		PartitionID: 1,
		Topic:       "orders",
		Offset:      42,
		Event:       &types.Event{MessageId: "msg-1"},
	}

	data, err := json.Marshal(evt)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	var decoded ChangeEvent
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if decoded.Op != "append" {
		t.Errorf("expected op append, got %s", decoded.Op)
	}
	if decoded.PartitionID != 1 {
		t.Errorf("expected partition 1, got %d", decoded.PartitionID)
	}
	if decoded.Topic != "orders" {
		t.Errorf("expected topic orders, got %s", decoded.Topic)
	}
	if decoded.Offset != 42 {
		t.Errorf("expected offset 42, got %d", decoded.Offset)
	}
}

func TestMockSink(t *testing.T) {
	s := &mockSink{name: "test"}
	if s.Name() != "test" {
		t.Errorf("expected name test, got %s", s.Name())
	}
	if err := s.Write(context.Background(), &ChangeEvent{}); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

// mockSink is a test helper implementing Sink

type mockSink struct {
	name       string
	err        error
	closeErr   error
	writeCount atomic.Int64
	closed     atomic.Bool
}

func (m *mockSink) Name() string { return m.name }

func (m *mockSink) Write(ctx context.Context, event *ChangeEvent) error {
	m.writeCount.Add(1)
	return m.err
}

func (m *mockSink) Close() error {
	m.closed.Store(true)
	return m.closeErr
}

func TestWebhookSink(t *testing.T) {
	var received bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received = true
		if r.Header.Get("Content-Type") != "application/json" {
			t.Error("expected Content-Type application/json")
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	sink := NewWebhookSink(server.URL)
	if sink.Name() != "webhook" {
		t.Errorf("expected name webhook, got %s", sink.Name())
	}

	evt := &ChangeEvent{Op: "append", Topic: "test"}
	err := sink.Write(context.Background(), evt)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if !received {
		t.Error("server should have received request")
	}

	if err := sink.Close(); err != nil {
		t.Errorf("Close failed: %v", err)
	}
}

func TestWebhookSink_ErrorStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	sink := NewWebhookSink(server.URL)
	err := sink.Write(context.Background(), &ChangeEvent{})
	if err == nil {
		t.Error("expected error for 500 status")
	}
}

func TestWebhookSink_NetworkError(t *testing.T) {
	sink := NewWebhookSink("http://127.0.0.1:1") // unreachable
	err := sink.Write(context.Background(), &ChangeEvent{})
	if err == nil {
		t.Error("expected network error")
	}
}

func TestKafkaSink_Name(t *testing.T) {
	sink := NewKafkaSink([]string{"localhost:9092"}, "cdc-topic")
	if sink.Name() != "kafka" {
		t.Errorf("expected name kafka, got %s", sink.Name())
	}
}

func TestKafkaSink_EmptyBrokers(t *testing.T) {
	sink := NewKafkaSink([]string{}, "topic")
	err := sink.Write(context.Background(), &ChangeEvent{Topic: "test"})
	if err == nil {
		t.Error("expected error for empty brokers")
	}
}

func TestKafkaSink_Close(t *testing.T) {
	sink := NewKafkaSink([]string{"localhost:9092"}, "topic")
	if err := sink.Close(); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

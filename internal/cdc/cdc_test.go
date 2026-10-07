package cdc

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
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
	if m.SinkCount() != 0 || m.HasSinks() {
		t.Errorf("a new manager has %d sinks", m.SinkCount())
	}
}

func TestManager_RegisterSink(t *testing.T) {
	m := NewManager()
	m.RegisterSink(&mockSink{name: "mock"})
	if m.SinkCount() != 1 || !m.HasSinks() {
		t.Errorf("expected 1 sink, got %d", m.SinkCount())
	}
}

// events returns log entries with consecutive offsets starting at from.
func events(from int64, n int) []*types.Event {
	out := make([]*types.Event, n)
	for i := range out {
		out[i] = &types.Event{Offset: from + int64(i), Topic: "orders", MessageId: fmt.Sprintf("order-%d", from+int64(i))}
	}
	return out
}

func TestManager_Deliver_NoSinks(t *testing.T) {
	if err := NewManager().Deliver(context.Background(), 0, events(0, 3)); err != nil {
		t.Fatalf("delivering with no sinks: %v", err)
	}
}

// Every sink gets every event of a partition, in log order, described as an
// append on that partition.
func TestManager_Deliver_InOrderToEverySink(t *testing.T) {
	m := NewManager()
	first, second := &mockSink{name: "first"}, &mockSink{name: "second"}
	m.RegisterSink(first)
	m.RegisterSink(second)

	if err := m.Deliver(context.Background(), 7, events(10, 5)); err != nil {
		t.Fatal(err)
	}
	for _, sink := range []*mockSink{first, second} {
		got := sink.offsets()
		if fmt.Sprint(got) != "[10 11 12 13 14]" {
			t.Errorf("sink %s received offsets %v, want 10 to 14 in order", sink.name, got)
		}
		change := sink.first()
		if change.Op != "append" || change.PartitionID != 7 || change.Topic != "orders" || change.Event.GetMessageId() != "order-10" || change.Timestamp.IsZero() {
			t.Errorf("sink %s received %+v", sink.name, change)
		}
	}
}

// A sink that fails stops the delivery where it failed. Offering the events
// again gives that sink the rest, and gives the sink that already took them
// nothing a second time.
func TestManager_Deliver_RetryResumesWithoutRepeating(t *testing.T) {
	m := NewManager()
	healthy := &mockSink{name: "healthy"}
	flaky := &mockSink{name: "flaky", failAt: 12}
	m.RegisterSink(healthy)
	m.RegisterSink(flaky)

	err := m.Deliver(context.Background(), 0, events(10, 5))
	if err == nil {
		t.Fatal("a delivery in which a sink failed reported success")
	}
	if got := fmt.Sprint(flaky.offsets()); got != "[10 11]" {
		t.Fatalf("the failing sink holds offsets %s, want what preceded the failure", got)
	}

	flaky.heal()
	// The feed offers the same events again, with more behind them.
	if err := m.Deliver(context.Background(), 0, events(10, 7)); err != nil {
		t.Fatalf("retry: %v", err)
	}
	for _, sink := range []*mockSink{healthy, flaky} {
		if got := fmt.Sprint(sink.offsets()); got != "[10 11 12 13 14 15 16]" {
			t.Errorf("sink %s holds offsets %s, want 10 to 16 once each", sink.name, got)
		}
	}
}

// Progress is kept per partition: the same offsets of another partition are
// different events.
func TestManager_Deliver_PartitionsAreIndependent(t *testing.T) {
	m := NewManager()
	sink := &mockSink{name: "mock"}
	m.RegisterSink(sink)
	for _, partition := range []int32{0, 1} {
		if err := m.Deliver(context.Background(), partition, events(0, 2)); err != nil {
			t.Fatal(err)
		}
	}
	if got := fmt.Sprint(sink.offsets()); got != "[0 1 0 1]" {
		t.Errorf("offsets received: %s, want both partitions in full", got)
	}
}

// A sink that takes batches gets one call per delivery, and nothing is
// recorded as taken when that call fails.
func TestManager_Deliver_UsesBatchWrites(t *testing.T) {
	m := NewManager()
	sink := &mockBatchSink{mockSink: mockSink{name: "batch"}}
	m.RegisterSink(sink)

	sink.failBatch = true
	if err := m.Deliver(context.Background(), 0, events(0, 4)); err == nil {
		t.Fatal("a failed batch write reported success")
	}
	sink.failBatch = false
	if err := m.Deliver(context.Background(), 0, events(0, 4)); err != nil {
		t.Fatal(err)
	}
	if sink.batches != 2 || sink.writeCount.Load() != 0 {
		t.Errorf("%d batch calls and %d single writes, want 2 and 0", sink.batches, sink.writeCount.Load())
	}
	if got := fmt.Sprint(sink.offsets()); got != "[0 1 2 3]" {
		t.Errorf("offsets received: %s, want 0 to 3 once", got)
	}
}

// After Close nothing can be delivered, and Deliver must say so: reporting
// success would let the feed move past events no sink has seen.
func TestManager_Deliver_AfterClose(t *testing.T) {
	m := NewManager()
	sink := &mockSink{name: "mock"}
	m.RegisterSink(sink)
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	if err := m.Deliver(context.Background(), 0, events(0, 1)); err == nil {
		t.Fatal("a delivery after Close reported success")
	}
	if len(sink.offsets()) != 0 {
		t.Error("a closed sink received an event")
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

// mockSink is a test helper implementing Sink. It records what it was given,
// and fails every write from offset failAt on until healed.
type mockSink struct {
	name       string
	err        error
	closeErr   error
	failAt     int64
	writeCount atomic.Int64
	closed     atomic.Bool

	mu       sync.Mutex
	received []*ChangeEvent
}

func (m *mockSink) Name() string { return m.name }

func (m *mockSink) Write(ctx context.Context, event *ChangeEvent) error {
	m.writeCount.Add(1)
	if m.err != nil {
		return m.err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failAt > 0 && event.Offset >= m.failAt {
		return fmt.Errorf("sink %s refuses offset %d", m.name, event.Offset)
	}
	m.received = append(m.received, event)
	return nil
}

func (m *mockSink) Close() error {
	m.closed.Store(true)
	return m.closeErr
}

func (m *mockSink) heal() {
	m.mu.Lock()
	m.failAt = 0
	m.mu.Unlock()
}

func (m *mockSink) offsets() []int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]int64, len(m.received))
	for i, change := range m.received {
		out[i] = change.Offset
	}
	return out
}

func (m *mockSink) first() *ChangeEvent {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.received[0]
}

// mockBatchSink also takes whole batches.
type mockBatchSink struct {
	mockSink
	batches   int
	failBatch bool
}

func (m *mockBatchSink) WriteBatch(ctx context.Context, events []*ChangeEvent) error {
	m.batches++
	if m.failBatch {
		return fmt.Errorf("batch refused")
	}
	m.mu.Lock()
	m.received = append(m.received, events...)
	m.mu.Unlock()
	return nil
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

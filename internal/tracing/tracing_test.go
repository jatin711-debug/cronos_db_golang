package tracing

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
)

func TestInitTracing_Disabled(t *testing.T) {
	cfg := &Config{Enabled: false}
	err := InitTracing(cfg)
	if err != nil {
		t.Fatalf("InitTracing failed: %v", err)
	}
	if Tracer != nil {
		t.Error("Tracer should be nil when disabled")
	}
	if TracerProvider != nil {
		t.Error("TracerProvider should be nil when disabled")
	}
}

func TestInitTracing_NilConfig(t *testing.T) {
	err := InitTracing(nil)
	if err != nil {
		t.Fatalf("InitTracing failed: %v", err)
	}
	if Tracer != nil {
		t.Error("Tracer should be nil with nil config")
	}
}

func TestInitTracing_Stdout(t *testing.T) {
	// Save globals
	oldTracer := Tracer
	oldProvider := TracerProvider
	defer func() {
		Tracer = oldTracer
		TracerProvider = oldProvider
	}()

	cfg := &Config{
		Enabled:      true,
		ExporterType: "stdout",
		ServiceName:  "test-service",
		SampleRatio:  1.0,
	}
	err := InitTracing(cfg)
	if err != nil {
		t.Fatalf("InitTracing failed: %v", err)
	}
	if Tracer == nil {
		t.Error("Tracer should be set")
	}
	if TracerProvider == nil {
		t.Error("TracerProvider should be set")
	}

	// Cleanup
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = Shutdown(ctx)
}

func TestInitTracing_None(t *testing.T) {
	// Save globals
	oldTracer := Tracer
	oldProvider := TracerProvider
	defer func() {
		Tracer = oldTracer
		TracerProvider = oldProvider
	}()

	cfg := &Config{
		Enabled:      true,
		ExporterType: "none",
		ServiceName:  "test-service",
	}
	err := InitTracing(cfg)
	if err != nil {
		t.Fatalf("InitTracing failed: %v", err)
	}
	if Tracer != nil {
		t.Error("Tracer should be nil with none exporter")
	}
}

func TestInitTracing_UnsupportedExporter(t *testing.T) {
	cfg := &Config{
		Enabled:      true,
		ExporterType: "unsupported",
	}
	err := InitTracing(cfg)
	if err == nil {
		t.Error("expected error for unsupported exporter")
	}
}

func TestInitTracing_SampleRatio(t *testing.T) {
	// Save globals
	oldTracer := Tracer
	oldProvider := TracerProvider
	defer func() {
		Tracer = oldTracer
		TracerProvider = oldProvider
	}()

	for _, ratio := range []float64{-0.5, 0.0, 0.5, 1.0, 1.5} {
		cfg := &Config{
			Enabled:      true,
			ExporterType: "none",
			SampleRatio:  ratio,
		}
		err := InitTracing(cfg)
		if err != nil {
			t.Fatalf("InitTracing failed for ratio %f: %v", ratio, err)
		}
	}
}

func TestStartSpan_NilTracer(t *testing.T) {
	Tracer = nil
	ctx, span := StartSpan(context.Background(), "test-op")
	if span != nil {
		t.Error("span should be nil when tracer is nil")
	}
	if ctx == nil {
		t.Error("context should not be nil")
	}
}

func TestAddEvent_NilSpan(t *testing.T) {
	// Should not panic with nil tracer
	Tracer = nil
	AddEvent(context.Background(), "event")
}

func TestSetAttributes_NilSpan(t *testing.T) {
	Tracer = nil
	SetAttributes(context.Background(), attribute.String("key", "value"))
}

func TestRecordError_NilSpan(t *testing.T) {
	Tracer = nil
	RecordError(context.Background(), nil)
}

func TestSpanFromContext(t *testing.T) {
	span := SpanFromContext(context.Background())
	if span == nil {
		t.Log("SpanFromContext returns nil for empty context (expected)")
	}
}

func TestTraceIDFromContext_Empty(t *testing.T) {
	id := TraceIDFromContext(context.Background())
	if id != "" {
		t.Errorf("expected empty trace ID, got %s", id)
	}
}

func TestSpanIDFromContext_Empty(t *testing.T) {
	id := SpanIDFromContext(context.Background())
	if id != "" {
		t.Errorf("expected empty span ID, got %s", id)
	}
}

func TestShutdown_NoProvider(t *testing.T) {
	TracerProvider = nil
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	err := Shutdown(ctx)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestGRPCServerInterceptor_NilTracer(t *testing.T) {
	Tracer = nil
	interceptor := GRPCServerInterceptor()
	if interceptor == nil {
		t.Fatal("interceptor should not be nil")
	}
}

func TestGRPCStreamServerInterceptor_NilTracer(t *testing.T) {
	Tracer = nil
	interceptor := GRPCStreamServerInterceptor()
	if interceptor == nil {
		t.Fatal("interceptor should not be nil")
	}
}

func TestGRPCClientInterceptor_NilTracer(t *testing.T) {
	Tracer = nil
	interceptor := GRPCClientInterceptor()
	if interceptor == nil {
		t.Fatal("interceptor should not be nil")
	}
}

func TestGRPCClientStreamInterceptor_NilTracer(t *testing.T) {
	Tracer = nil
	interceptor := GRPCClientStreamInterceptor()
	if interceptor == nil {
		t.Fatal("interceptor should not be nil")
	}
}

func TestConfig_Fields(t *testing.T) {
	cfg := &Config{
		ServiceName:  "svc",
		Enabled:      true,
		ExporterType: "stdout",
		OTLPEndpoint: "localhost:4317",
		SampleRatio:  0.5,
		OTLPInsecure: true,
	}
	if cfg.ServiceName != "svc" {
		t.Error("ServiceName mismatch")
	}
	if !cfg.OTLPInsecure {
		t.Error("OTLPInsecure mismatch")
	}
}

func TestGRPCMetadataCarrier(t *testing.T) {
	carrier := &grpcMetadataCarrier{md: nil}
	if carrier.Get("key") != "" {
		t.Error("expected empty value for nil metadata")
	}
	if len(carrier.Keys()) != 0 {
		t.Error("expected 0 keys for nil metadata")
	}

	// Test with initialized metadata
	md := make(map[string][]string)
	carrier2 := &grpcMetadataCarrier{md: md}
	carrier2.Set("key", "value")
	if carrier2.Get("key") != "value" {
		t.Error("expected value to be set")
	}
	if len(carrier2.Keys()) != 1 {
		t.Errorf("expected 1 key, got %d", len(carrier2.Keys()))
	}
}

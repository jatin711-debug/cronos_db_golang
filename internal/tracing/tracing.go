package tracing

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
	"go.opentelemetry.io/otel/trace"
)

// Config holds tracing configuration
type Config struct {
	ServiceName  string
	Enabled      bool
	ExporterType string // "stdout", "otlp", "none"
	OTLPEndpoint string
	SampleRatio  float64
	OTLPInsecure bool
}

// Tracer is the global tracer instance
var Tracer trace.Tracer

// TracerProvider is the global tracer provider
var TracerProvider *sdktrace.TracerProvider

// InitTracing initializes OpenTelemetry tracing
func InitTracing(cfg *Config) error {
	if cfg == nil || !cfg.Enabled {
		log.Printf("[TRACING] Tracing disabled")
		Tracer = nil
		TracerProvider = nil
		return nil
	}

	exporterType := strings.ToLower(strings.TrimSpace(cfg.ExporterType))
	if exporterType == "" {
		exporterType = "none"
	}

	var exporter sdktrace.SpanExporter
	var err error

	switch exporterType {
	case "stdout":
		exp, err := stdouttrace.New(stdouttrace.WithPrettyPrint())
		if err != nil {
			return fmt.Errorf("create stdout exporter: %w", err)
		}
		exporter = exp
	case "otlp":
		exp, err := newOTLPExporter(cfg)
		if err != nil {
			log.Printf("[TRACING] OTLP exporter failed to initialize: %v; falling back to no-op", err)
			exporterType = "none"
		} else {
			exporter = exp
		}
	case "none":
		log.Printf("[TRACING] Using no-op tracing")
	default:
		return fmt.Errorf("unsupported tracing exporter: %s", cfg.ExporterType)
	}

	if exporter == nil {
		Tracer = nil
		TracerProvider = nil
		return nil
	}

	serviceName := strings.TrimSpace(cfg.ServiceName)
	if serviceName == "" {
		serviceName = "cronos-api"
	}

	res, err := resource.New(context.Background(),
		resource.WithAttributes(
			semconv.ServiceName(serviceName),
			semconv.ServiceVersion("1.0.0"),
		),
	)
	if err != nil {
		return fmt.Errorf("create resource: %w", err)
	}

	sampleRatio := cfg.SampleRatio
	if sampleRatio < 0 {
		sampleRatio = 0
	}
	if sampleRatio > 1 {
		sampleRatio = 1
	}

	sampler := sdktrace.Sampler(sdktrace.NeverSample())
	if sampleRatio > 0 {
		sampler = sdktrace.ParentBased(sdktrace.TraceIDRatioBased(sampleRatio))
	}

	TracerProvider = sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter,
			sdktrace.WithBatchTimeout(2*time.Second),
			sdktrace.WithMaxExportBatchSize(512),
			sdktrace.WithMaxQueueSize(2048),
		),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sampler),
	)

	otel.SetTracerProvider(TracerProvider)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	Tracer = TracerProvider.Tracer(serviceName,
		trace.WithInstrumentationVersion("1.0.0"),
	)

	log.Printf("[TRACING] Initialized exporter=%s sample_ratio=%.4f", exporterType, sampleRatio)
	return nil
}

func newOTLPExporter(cfg *Config) (sdktrace.SpanExporter, error) {
	opts := []otlptracegrpc.Option{
		otlptracegrpc.WithEndpoint(cfg.OTLPEndpoint),
		otlptracegrpc.WithTimeout(5 * time.Second),
	}
	if cfg.OTLPInsecure {
		opts = append(opts, otlptracegrpc.WithInsecure())
	}

	client := otlptracegrpc.NewClient(opts...)
	return otlptrace.New(context.Background(), client)
}

// Shutdown gracefully shuts down the tracer provider
func Shutdown(ctx context.Context) error {
	if TracerProvider != nil {
		return TracerProvider.Shutdown(ctx)
	}
	return nil
}

// StartSpan starts a new span with the given name and attributes
func StartSpan(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	if Tracer == nil {
		return ctx, nil
	}
	return Tracer.Start(ctx, name, trace.WithAttributes(attrs...))
}

// AddEvent adds an event to the current span
func AddEvent(ctx context.Context, name string, attrs ...attribute.KeyValue) {
	span := trace.SpanFromContext(ctx)
	if span != nil {
		span.AddEvent(name, trace.WithAttributes(attrs...))
	}
}

// SetAttributes sets attributes on the current span
func SetAttributes(ctx context.Context, attrs ...attribute.KeyValue) {
	span := trace.SpanFromContext(ctx)
	if span != nil {
		span.SetAttributes(attrs...)
	}
}

// RecordError records an error on the current span
func RecordError(ctx context.Context, err error, attrs ...attribute.KeyValue) {
	span := trace.SpanFromContext(ctx)
	if span != nil {
		span.RecordError(err, trace.WithAttributes(attrs...))
	}
}

// SpanFromContext returns the span from the context
func SpanFromContext(ctx context.Context) trace.Span {
	return trace.SpanFromContext(ctx)
}

// TraceIDFromContext returns the trace ID from the context
func TraceIDFromContext(ctx context.Context) string {
	span := trace.SpanFromContext(ctx)
	if span != nil && span.SpanContext().HasTraceID() {
		return span.SpanContext().TraceID().String()
	}
	return ""
}

// SpanIDFromContext returns the span ID from the context
func SpanIDFromContext(ctx context.Context) string {
	span := trace.SpanFromContext(ctx)
	if span != nil && span.SpanContext().HasSpanID() {
		return span.SpanContext().SpanID().String()
	}
	return ""
}

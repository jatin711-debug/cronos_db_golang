package tracing

import (
	"context"
	"fmt"
	"log"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
	"go.opentelemetry.io/otel/trace"
)

// Config holds tracing configuration
type Config struct {
	ServiceName string
	Enabled     bool
	ExporterType string // "stdout", "otlp", "none"
	OTLPEndpoint string
}

// Tracer is the global tracer instance
var Tracer trace.Tracer

// TracerProvider is the global tracer provider
var TracerProvider *sdktrace.TracerProvider

// InitTracing initializes OpenTelemetry tracing
func InitTracing(cfg *Config) error {
	if cfg == nil || !cfg.Enabled {
		log.Printf("[TRACING] Tracing disabled")
		return nil
	}

	// Create exporter based on config
	var exporter sdktrace.SpanExporter
	var err error

	switch cfg.ExporterType {
	case "stdout":
		exp, err := stdouttrace.New(stdouttrace.WithPrettyPrint())
		if err != nil {
			return fmt.Errorf("create stdout exporter: %w", err)
		}
		exporter = exp
	case "otlp":
		// Would use otlptrace/otlptracegrpc or otlptrace/otlptracehttp
		// For now, fall back to noop
		log.Printf("[TRACING] OTLP exporter not configured, using no-op")
		cfg.ExporterType = "none"
	default:
		log.Printf("[TRACING] Using no-op tracer (exporter type: %s)", cfg.ExporterType)
	}

	// Create resource with service info
	res, err := resource.New(context.Background(),
		resource.WithAttributes(
			semconv.ServiceName(cfg.ServiceName),
			semconv.ServiceVersion("1.0.0"),
		),
	)
	if err != nil {
		return fmt.Errorf("create resource: %w", err)
	}

	// Create tracer provider
	if cfg.ExporterType == "none" {
		TracerProvider = sdktrace.NewTracerProvider(
			sdktrace.WithResource(res),
			sdktrace.WithSampler(sdktrace.NeverSample()),
		)
	} else {
		TracerProvider = sdktrace.NewTracerProvider(
			sdktrace.WithBatcher(exporter),
			sdktrace.WithResource(res),
			sdktrace.WithSampler(sdktrace.AlwaysSample()),
		)
	}

	// Set global tracer provider
	otel.SetTracerProvider(TracerProvider)

	// Set global propagator (W3C TraceContext)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	// Create tracer
	Tracer = TracerProvider.Tracer(cfg.ServiceName,
		trace.WithInstrumentationVersion("1.0.0"),
	)

	log.Printf("[TRACING] Initialized with exporter type: %s", cfg.ExporterType)
	return nil
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
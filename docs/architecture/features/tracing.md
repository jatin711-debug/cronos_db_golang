# Tracing Architecture

## Purpose

Tracing module adds request-level and flow-level telemetry for diagnostics across distributed operations.

## Key Files

- [internal/tracing/tracing.go](../../../internal/tracing/tracing.go)
- [internal/tracing/grpc_interceptor.go](../../../internal/tracing/grpc_interceptor.go)
- [internal/api/grpc_server.go](../../../internal/api/grpc_server.go)

## Main Flow

1. Tracing provider is initialized at startup.
2. gRPC interceptors create and propagate spans.
3. Exporter sends traces to stdout or OTLP backend depending on config.

## Production Decisions

- Tracing can be disabled or sampled to control overhead.
- Initialization and shutdown are handled explicitly for clean process exit.
- Interceptor placement ensures core request boundaries are traced.

## Debug Pointers

- Provider setup and exporter behavior: [internal/tracing/tracing.go](../../../internal/tracing/tracing.go)
- Span boundaries in gRPC calls: [internal/tracing/grpc_interceptor.go](../../../internal/tracing/grpc_interceptor.go)

## Related Diagrams

- [observability_feedback_loop.mmd](../../mermaid/observability_feedback_loop.mmd)
- [startup_sequence.mmd](../../mermaid/startup_sequence.mmd)

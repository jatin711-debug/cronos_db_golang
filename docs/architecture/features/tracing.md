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

## Scope and defaults

- Off by default (`--tracing-enabled=false`, exporter `none`, sample ratio 0.01 when enabled, OTLP endpoint `127.0.0.1:4317`, plaintext OTLP: `--tracing-insecure=true`). The service name is `cronos-api-<node-id>`. With the exporter `none`, or an OTLP exporter that fails to initialize, no provider is installed and every span helper is a no-op.
- Spans are made for the **public** gRPC listener only: `GRPCServerInterceptor` / `GRPCStreamServerInterceptor` (named by full RPC method), plus spans inside `Publish`, `Subscribe` and `Ack` in `internal/api/handlers.go`. The internal listener (replication, Raft) has no tracing interceptor, and the client interceptors in `grpc_interceptor.go` are not installed anywhere, so a trace does not follow a publish to its followers.
- Incoming W3C trace context and baggage are extracted from request metadata; the exporter is batched (2 s, 512 spans, queue 2,048).

## Production Decisions

- Tracing can be disabled or sampled to control overhead.
- Initialization and shutdown are handled explicitly for clean process exit.
- Interceptor placement ensures core request boundaries are traced.

## Debug Pointers

- Provider setup and exporter behavior: [internal/tracing/tracing.go](../../../internal/tracing/tracing.go)
- Span boundaries in gRPC calls: [internal/tracing/grpc_interceptor.go](../../../internal/tracing/grpc_interceptor.go)

## Related Diagrams

- [Observability feedback loop](slo.md#observability-feedback-loop)
- [Startup lifecycle](../../DEVELOPER_ARCHITECTURE_GUIDE.md#42-startup-and-shutdown-sequence)

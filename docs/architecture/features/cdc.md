# CDC Architecture

## Purpose

The CDC module publishes change events to external systems without blocking the primary publish path.

## Key Files

- [internal/cdc/sink.go](../../../internal/cdc/sink.go)
- [internal/cdc/kafka_sink.go](../../../internal/cdc/kafka_sink.go)
- [internal/cdc/webhook_sink.go](../../../internal/cdc/webhook_sink.go)
- [cmd/api/main.go](../../../cmd/api/main.go)

## Main Flow

1. Startup registers sinks from environment.
2. WAL append hook emits change events to CDC manager.
3. CDC manager fan-outs asynchronously to configured sinks.
4. Sink failures are logged and isolated from core write execution.

## Production Decisions

- Async emission prevents external sink latency from impacting publish SLA.
- Sink abstraction allows independent extension for new destinations.
- Failure isolation avoids write-path cascading failures.

## Debug Pointers

- Hook wiring in startup: [cmd/api/main.go](../../../cmd/api/main.go)
- Sink behavior: [internal/cdc](../../../internal/cdc)

## Related Diagrams

- [publish_flow.mmd](../../mermaid/publish_flow.mmd)
- [cross_region_replication.mmd](../../mermaid/cross_region_replication.mmd)

# API Layer Architecture

## Purpose

The API layer is the system entrypoint for all client and internal RPC traffic. It translates contracts into partition-aware operations and enforces security, guardrails, and observability through interceptors.

## Key Files

- [internal/api/grpc_server.go](../../../internal/api/grpc_server.go) — public listener on `:9000`; registers `EventService`, `PartitionService`, `ConsumerGroupService`, `TransactionService`, and the public-side metadata services.
- [internal/api/internal_grpc_server.go](../../../internal/api/internal_grpc_server.go) — dedicated internal listener (`InternalGRPCServer`, default `:7947`); registers `ReplicationServiceServer` and `RaftServiceServer` only. Replication traffic never touches the public listener.
- [internal/api/handlers.go](../../../internal/api/handlers.go) — `EventService` handlers (`Publish`, `PublishBatch`, `Subscribe`, `Ack`, `Replay`).
- [internal/api/consumer_handler.go](../../../internal/api/consumer_handler.go)
- [internal/api/partition_handler.go](../../../internal/api/partition_handler.go)
- [internal/api/replication_server.go](../../../internal/api/replication_server.go) — `ReplicationServiceHandler.Append`, `.Sync`, `.Snapshot` (served on the internal listener).
- [internal/api/raft_server.go](../../../internal/api/raft_server.go) — `RaftService` join/leave/status (internal listener).
- [internal/api/crossregion_server.go](../../../internal/api/crossregion_server.go) — `CrossRegionService` handler.
- [internal/api/audit_interceptor.go](../../../internal/api/audit_interceptor.go) — request-level audit interceptor.
- [internal/api/health.go](../../../internal/api/health.go) — HTTP health endpoint.
- [internal/api/metrics.go](../../../internal/api/metrics.go) — Prometheus collectors.
- [internal/api/ratelimit.go](../../../internal/api/ratelimit.go), [internal/api/topic_ratelimit.go](../../../internal/api/topic_ratelimit.go) — IP and per-subject per-topic rate limiters.
- [internal/api/stream_interceptors.go](../../../internal/api/stream_interceptors.go) — stream RPC interceptor wiring.
- [internal/api/tls.go](../../../internal/api/tls.go) — public gRPC TLS / mTLS configuration.
- [internal/api/version.go](../../../internal/api/version.go) — version interceptors / build info.
- [proto/events.proto](../../../proto/events.proto) — full gRPC API specification (7 services, 30+ message types).

## Main Request Flow

1. gRPC server boots in [cmd/api/main.go](../../../cmd/api/main.go) and registers services.
2. Unary and stream interceptors apply tracing, SLO, version checks, auth, topic limits, audit, metrics, and IP rate limiting.
3. Handlers validate request semantics and route to partition manager, cluster router, and feature modules.
4. Responses return explicit success and failure details for API and operational tooling.

## Production Decisions

- `Start()` exposes `ServeError()` so server startup failures are observable.
- `GracefulStopWithTimeout(ctx)` falls back to forced `Stop` if the deadline expires.
- Health-server startup errors are propagated rather than swallowed.
- Message size and keepalive limits are configured in server options to avoid unbounded payload and idle-connection risk.
- Partition ownership checks in handlers prevent writes on non-owner nodes and fence stale leaders.
- Replay path respects follower-read policy flags for consistency control.
- Ack path supports stricter monotonic commit behavior when exactly-once commit mode is enabled.

## Debug Pointers

- Publish and replay behavior: [internal/api/handlers.go](../../../internal/api/handlers.go)
- Partition admin behavior: [internal/api/partition_handler.go](../../../internal/api/partition_handler.go)
- Service registration and middleware chain: [internal/api/grpc_server.go](../../../internal/api/grpc_server.go)

## Related Diagrams

- [publish_flow.mmd](../../mermaid/publish_flow.mmd)
- [system_overview.mmd](../../mermaid/system_overview.mmd)
- [startup_sequence.mmd](../../mermaid/startup_sequence.mmd)

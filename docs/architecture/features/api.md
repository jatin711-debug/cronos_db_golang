# API Layer Architecture

## Purpose

The API layer is the system entrypoint for all client and internal RPC traffic. It translates contracts into partition-aware operations and enforces security, guardrails, and observability through interceptors.

## Key Files

- [internal/api/grpc_server.go](../../../internal/api/grpc_server.go) — public listener on `:9000`; registers `EventService`, `PartitionService`, `ConsumerGroupService`, `TransactionService`, and **`AdminService`**.
- [internal/api/internal_grpc_server.go](../../../internal/api/internal_grpc_server.go) — dedicated internal listener (`InternalGRPCServer`, default `:7947`); registers **`ReplicationService`**, **`RaftService`**, and **`CrossRegionService`**. Peer traffic never shares the public client port.
- [internal/api/handlers.go](../../../internal/api/handlers.go) — `EventService` handlers (`Publish`, `PublishBatch`, `Subscribe`, `Ack`, `Replay`) with dedup claim/rollback.
- [internal/api/admin_handler.go](../../../internal/api/admin_handler.go) — operator AdminService (topology, health, lag, retention, compaction, schemas, tenants; **`TriggerRebalance` soft-stub** — see Known Limitations).
- [internal/api/web_handler.go](../../../internal/api/web_handler.go) — embedded SPA + `/api/admin/*` JSON proxy on HTTP `:8080`.
- [internal/api/consumer_handler.go](../../../internal/api/consumer_handler.go)
- [internal/api/partition_handler.go](../../../internal/api/partition_handler.go) — partition metadata + admin compact/retention/split (authz gated).
- [internal/api/replication_server.go](../../../internal/api/replication_server.go) — `Append`, `Sync`, `Snapshot` on the internal listener.
- [internal/api/raft_server.go](../../../internal/api/raft_server.go) — Raft join/leave/status (internal).
- [internal/api/crossregion_server.go](../../../internal/api/crossregion_server.go) — CrossRegion inject/fetch with LWW (internal only).
- [internal/api/audit_interceptor.go](../../../internal/api/audit_interceptor.go) — request-level audit interceptor.
- [internal/api/health.go](../../../internal/api/health.go) — HTTP health endpoints.
- [internal/api/metrics.go](../../../internal/api/metrics.go) — Prometheus collectors.
- [internal/api/ratelimit.go](../../../internal/api/ratelimit.go), [internal/api/topic_ratelimit.go](../../../internal/api/topic_ratelimit.go) — IP and per-topic rate limiters.
- [internal/api/stream_interceptors.go](../../../internal/api/stream_interceptors.go) — stream interceptor wiring.
- [internal/api/tls.go](../../../internal/api/tls.go) — public gRPC TLS / mTLS configuration.
- [internal/api/version.go](../../../internal/api/version.go) — wire version gate / rolling upgrade helpers.
- [proto/events.proto](../../../proto/events.proto), [proto/admin.proto](../../../proto/admin.proto) — wire contracts.

## Listener matrix

| Listener | Default | Services |
|----------|---------|----------|
| Public gRPC | `:9000` | Event, Partition, ConsumerGroup, Transaction, Admin |
| Internal gRPC | `:7947` | Replication, Raft, CrossRegion |
| HTTP | `:8080` | `/health*`, `/metrics`, `/ui/`, `/api/admin/*` |

## Main Request Flow

1. `cmd/api/main.go` starts **public** and **internal** gRPC servers plus HTTP.
2. Public unary/stream interceptors apply tracing, SLO, version, auth, topic limits, audit, metrics, and IP rate limiting (heavy ones skipped in `--dev`).
3. Handlers validate, authorize, and route via partition manager / cluster router.
4. Internal replication/cross-region RPCs use optional cluster mTLS credentials.

## Production Decisions

- `Start()` exposes `ServeError()` so server startup failures are observable.
- `GracefulStopWithTimeout(ctx)` falls back to forced `Stop` if the deadline expires.
- Health-server startup errors are propagated rather than swallowed.
- Message size and keepalive limits are configured in server options to avoid unbounded payload and idle-connection risk.
- Partition ownership checks in handlers prevent writes on non-owner nodes and fence stale leaders.
- Replay path respects follower-read policy flags for consistency control.
- Ack path supports stricter monotonic commit behavior when exactly-once commit mode is enabled.

## Known Limitations (API surface)

| RPC / path | Status | Detail |
|------------|--------|--------|
| `AdminService.TriggerRebalance` | Soft stub | Membership-driven rebalance works; on-demand trigger does not move partitions. [cluster.md](cluster.md), [ARCHITECTURE](../../../ARCHITECTURE.md#known-limitations) |
| Cross-region on public port | **Not exposed** | Only on internal `:7947` (by design, not a gap) |
| Subscribe control-stream `Recv` | Not used for credits | Flow control via credits/Ack path; control Recv is unused |

## Debug Pointers

- Publish and replay behavior: [internal/api/handlers.go](../../../internal/api/handlers.go)
- Partition admin behavior: [internal/api/partition_handler.go](../../../internal/api/partition_handler.go)
- Service registration and middleware chain: [internal/api/grpc_server.go](../../../internal/api/grpc_server.go)
- Rebalance stub response: [internal/api/admin_handler.go](../../../internal/api/admin_handler.go)

## Related Diagrams

- [publish_flow.mmd](../../mermaid/publish_flow.mmd)
- [system_overview.mmd](../../mermaid/system_overview.mmd)
- [startup_sequence.mmd](../../mermaid/startup_sequence.mmd)

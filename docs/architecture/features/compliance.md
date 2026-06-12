# Compliance and Retention Architecture

## Purpose

The compliance module enforces retention and storage lifecycle constraints while preserving protected runtime data paths.

## Key Files

- [internal/compliance/retention.go](../../../internal/compliance/retention.go)
- [internal/api/partition_handler.go](../../../internal/api/partition_handler.go)
- [cmd/api/main.go](../../../cmd/api/main.go)

## Main Flow

1. Startup launches periodic retention enforcement loop.
2. Retention policy applies age and size constraints across storage paths.
3. Partition admin RPC supports on-demand retention and compaction actions.
4. Responses expose reclaimed segments and bytes for operational visibility.

## Production Decisions

- Protected directories are excluded from destructive cleanup.
- Retention operations are separated from hot request path.
- Admin operations return explicit status payloads rather than silent actions.

## Debug Pointers

- Policy implementation: [internal/compliance/retention.go](../../../internal/compliance/retention.go)
- Runtime admin retention logic: [internal/api/partition_handler.go](../../../internal/api/partition_handler.go)

## Related Diagrams

- [wal_lifecycle.mmd](../../mermaid/wal_lifecycle.mmd)
- [observability_feedback_loop.mmd](../../mermaid/observability_feedback_loop.mmd)

# Replay Engine Architecture

## Purpose

Replay serves historical data from WAL by time or offset, enabling debugging, backfills, and catch-up flows.

## Key Files

- [internal/replay/engine.go](../../../internal/replay/engine.go)
- [internal/api/handlers.go](../../../internal/api/handlers.go)
- [proto/events.proto](../../../proto/events.proto)

## Main Flow

1. Replay request enters EventService replay handler.
2. Handler validates partition ownership and policy constraints.
3. Replay engine scans WAL ranges and applies query filters.
4. Results stream in batches back to the client.

## Production Decisions

- Streamed replay avoids loading large result sets into memory.
- Replay engine reads WAL v2 records with Raft term and trailing checksum validation.
- Context cancellation is honored for long-running requests.
- Follower-read policy gate protects consistency expectations.

## Debug Pointers

- Replay filtering or range issues: [internal/replay/engine.go](../../../internal/replay/engine.go)
- API ownership checks: [internal/api/handlers.go](../../../internal/api/handlers.go)

## Related Diagrams

- [publish_flow.mmd](../../mermaid/publish_flow.mmd)
- [system_overview.mmd](../../mermaid/system_overview.mmd)

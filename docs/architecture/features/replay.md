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
4. Events are sent to the client one to a message, as they are read.

## Production Decisions

- `speed` paces a replay at 10 ms / `speed` between events: 0 sends as fast as
  the client takes them, and other values must be at least 0.001 (10 s between
  events); anything else is refused with `INVALID_ARGUMENT`. Cancelling a
  replay ends its pause. Before 1.0 the pause was a plain sleep, and a tiny
  speed asked for months of it: a cancelled replay kept its handler and the
  chunk of log it held until the sleep ended.
- A replay reads the log 4 MiB at a time (`replayChunkBytes`) and sends each
  event before it reads on, so what it holds does not depend on the size of
  the range. The range is fixed when the replay starts: an event appended
  while it runs is not part of it.
- A replay by offset returns 1,000 events unless the request gives a count.
- A replay by time reads the segments that can hold an event in its window
  and looks at every event in those, because entries are not in time order.
  Each segment keeps the least and the greatest schedule time of its events,
  and one that the window does not meet is not read (`WAL.TimeSpan`). A window
  that events all over the log fall into still reads all of it.
- Replay engine reads WAL v2 records with Raft term and trailing checksum validation.
- Context cancellation is honored for long-running requests.
- Follower-read policy gate protects consistency expectations.
- On term-start partitions, public replay is bounded by the committed prefix.
  In lifecycle mode it emits only active event copies, strips internal mutation
  metadata, and excludes cancellations and superseded copies. Term-start records
  are always omitted. A replay is therefore not a physical WAL inventory;
  acceptance tests use authenticated physical snapshots to compare replicas.
  See [current-term commitment](../../REPLICATION_TERM_START.md#reader-behavior).

## Debug Pointers

- Replay filtering or range issues: [internal/replay/engine.go](../../../internal/replay/engine.go)
- API ownership checks: [internal/api/handlers.go](../../../internal/api/handlers.go)

## Related Diagrams

- [Publish flow](api.md#publish-flow)
- [System overview](../README.md#system-overview)

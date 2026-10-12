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

## Debug Pointers

- Replay filtering or range issues: [internal/replay/engine.go](../../../internal/replay/engine.go)
- API ownership checks: [internal/api/handlers.go](../../../internal/api/handlers.go)

## Related Diagrams

- [Publish flow](api.md#publish-flow)
- [System overview](../README.md#system-overview)

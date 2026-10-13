# Consumer Groups Architecture

## Purpose

The consumer module manages group membership and records, per group and
partition, which events consumers have finished, so consumption resumes after
a restart or a failover. Delivery is at least once.

## Key Files

- [internal/consumer/group.go](../../../internal/consumer/group.go)
- [internal/consumer/completion.go](../../../internal/consumer/completion.go) — per-event completion records and the completion floor.
- [internal/consumer/offset_store.go](../../../internal/consumer/offset_store.go)
- [internal/api/handlers.go](../../../internal/api/handlers.go)
- [internal/api/consumer_handler.go](../../../internal/api/consumer_handler.go)
- [internal/api/replication_server.go](../../../internal/api/replication_server.go) — `SyncConsumerProgress`, where a follower takes its leader's progress.

## Main Flow

1. Subscribe requests create or join group members.
2. An Ack names a delivery; the server validates it against the delivery it
   made (`Dispatcher.ValidateAck`) and records completion for the events that
   delivery carried (`GroupManager.CommitDeliveries`). A client cannot move
   progress by sending an offset.
3. Completion is recorded per event, above a **completion floor** below which
   every event is complete. The floor advances over the completed prefix, and
   records it passes are removed. Per-event records are needed because
   schedule order need not match log order.
4. Completions, floors and group metadata/assignments are kept in PebbleDB
   (`OffsetStore`) and loaded on restart. Acks that arrive together share one
   durable write.

## Production Decisions

- Ack parsing extracts the partition from the delivery ID.
- Completion commits are serialized per group manager (`commitMu`).
- On a replicated partition the leader sends each group's floor and completed
  offsets to its followers (`SyncConsumerProgress`, from the leader's
  maintenance loop). A follower takes them only as far as its own log
  reaches. What finished on the old leader in the last moments before a
  failover, and had not reached the new leader yet, can be delivered again.
- A replica keeps no completion at or beyond the end of its own log
  (`ForgetCompletionsFrom`), so an offset reused after the log was cut does
  not count as finished.
- `--exactly-once-commits` (commit IDs and strict monotonic commits) is
  refused in production ("use per-event at-least-once completion"). The
  commit-ID method in `OffsetStore` is not called by any server path.

## Debug Pointers

- Offset anomalies: [internal/consumer/group.go](../../../internal/consumer/group.go)
- Persistent commit/load behavior: [internal/consumer/offset_store.go](../../../internal/consumer/offset_store.go)

## Related Diagrams

- [Publish flow](api.md#publish-flow)
- [Delivery state machine](delivery.md#delivery-state-machine)

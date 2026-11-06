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
- Lifecycle rescheduling and relocation preserve logical identity but use a new
  physical offset. Acks complete the copy actually delivered; an old copy's
  completion never completes its successor. Lifecycle recovery imports group
  metadata, floors and sparse progress with the matching WAL generation after
  clearing stale state. See [consistent recovery](../../RETENTION_AND_EVENT_LIFECYCLE.md#consistent-recovery-snapshots-and-backups).

- **Limits on what clients create** (v0.10.0). A group name and a subscription (member) ID are at most 255 bytes (`consumer.MaxGroupIDLength`, `MaxMemberIDLength`), and a partition holds at most `--max-consumer-groups` groups, default 10,000, negative for no limit (`GroupManager.admitGroupLocked`). A group persists with its progress after its subscriptions end, and Subscribe creates one for any name it is given, hence the limit. No RPC deletes a group.

## Consumer-group RPCs

`ConsumerGroupService` (`internal/api/consumer_handler.go`) is an operator API over the group state of the first partition this node loaded (partition 0 in cluster mode, which pins it), not of every partition. With auth on, all four RPCs need global `"admin": true`; they grant no topic permission.

| RPC | Behavior |
|-----|----------|
| `CreateConsumerGroup` | Creates a group for a topic and partition list; a refusal (for example the limits above) is `success=false` with `error`, not a gRPC error. |
| `GetConsumerGroup` | Returns the group's topic, partitions, committed offsets, member offsets and last update time, or `types.ErrConsumerGroupNotFound`. |
| `ListConsumerGroups` | Every group on that state, with the same metadata. |
| `RebalanceConsumerGroup` | Reassigns the group's partitions among its members and returns the assignment (`GroupManager.TriggerRebalance`); failures are `success=false`. |

`AdminService` has its own, read-only views for the dashboard (admin as well; see [dashboard.md](dashboard.md)). `AdminService.ListConsumerGroups` lists `(group, topic)` pairs de-duplicated over every local partition, with partitions and member count. `AdminService.GetConsumerGroupLag` takes a group and an explicit partition and returns the committed offset, the partition's high watermark and `lag` (never negative); all three are `-1` when the partition is not local or the group has not committed there.

## Debug Pointers

- Offset anomalies: [internal/consumer/group.go](../../../internal/consumer/group.go)
- Persistent commit/load behavior: [internal/consumer/offset_store.go](../../../internal/consumer/offset_store.go)

## Related Diagrams

- [Publish flow](api.md#publish-flow)
- [Delivery state machine](delivery.md#delivery-state-machine)

# Consumer Groups Architecture

## Purpose

The consumer module manages group membership and committed offsets for ordered, resumable consumption.

## Key Files

- [internal/consumer/group.go](../../../internal/consumer/group.go)
- [internal/consumer/offset_store.go](../../../internal/consumer/offset_store.go)
- [internal/api/handlers.go](../../../internal/api/handlers.go)
- [internal/api/consumer_handler.go](../../../internal/api/consumer_handler.go)

## Main Flow

1. Subscribe requests create or join group members.
2. Delivery and Ack flow updates committed offsets per group and partition.
3. Persistent offset store recovers positions on restart when configured.

## Production Decisions

- Ack parsing extracts partition context from delivery identifiers.
- Offset commits are guarded by synchronization in group manager.
- Optional strict monotonic commit checks can be enabled by config.

## Debug Pointers

- Offset anomalies: [internal/consumer/group.go](../../../internal/consumer/group.go)
- Persistent commit/load behavior: [internal/consumer/offset_store.go](../../../internal/consumer/offset_store.go)

## Related Diagrams

- [publish_flow.mmd](../../mermaid/publish_flow.mmd)
- [delivery_state_machine.mmd](../../mermaid/delivery_state_machine.mmd)

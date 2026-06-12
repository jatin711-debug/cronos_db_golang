# Scheduler and Timing Wheel Architecture

## Purpose

The scheduler controls when events become deliverable, using a hot timing wheel and optional cold store hydration.

## Key Files

- [internal/scheduler/scheduler.go](../../../internal/scheduler/scheduler.go)
- [internal/scheduler/timing_wheel.go](../../../internal/scheduler/timing_wheel.go)
- [internal/scheduler/cold_store.go](../../../internal/scheduler/cold_store.go)

## Main Flow

1. Publish path schedules events by schedule timestamp.
2. Near-future events live in hot timing wheel slots.
3. Far-future events can be staged in cold store.
4. Hydrator moves due-near events into hot wheel.
5. Ready events are emitted to the worker queue.

## Production Decisions

- Hierarchical wheel gives near O(1) scheduling behavior.
- Adaptive hydrator interval manages scan pressure under changing load.
- Hot and cold separation keeps memory bounded for distant schedules.

## Debug Pointers

- Delayed or early trigger issues: [internal/scheduler/timing_wheel.go](../../../internal/scheduler/timing_wheel.go)
- Hydration lag: [internal/scheduler/scheduler.go](../../../internal/scheduler/scheduler.go)

## Related Diagrams

- [scheduler_hot_cold_flow.mmd](../../mermaid/scheduler_hot_cold_flow.mmd)
- [publish_flow.mmd](../../mermaid/publish_flow.mmd)

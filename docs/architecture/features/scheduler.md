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
- Defaults: **`-tick-ms=10`**, **`-wheel-size=600`** (6s root span); hot window default 60 minutes.
- Adaptive hydrator interval (5s–5min) manages cold-store scan pressure.
- Hot and cold separation keeps memory bounded for distant schedules.
- **Crash recovery:** restore `timer_state.json` when present, **rebase `currentTick` from wall clock**, replay WAL timers from checkpoint; events matured during downtime are **enqueued for immediate delivery** (not dropped).
- Ready-queue and wheel size admission limits reject publish under overload (`ResourceExhausted`).

## Debug Pointers

- Delayed or early trigger issues: [internal/scheduler/timing_wheel.go](../../../internal/scheduler/timing_wheel.go)
- Hydration lag / recovery: [internal/scheduler/scheduler.go](../../../internal/scheduler/scheduler.go)
- Partition WAL timer replay: [internal/partition/manager.go](../../../internal/partition/manager.go)

## Related Diagrams

- [scheduler_hot_cold_flow.mmd](../../mermaid/scheduler_hot_cold_flow.mmd)
- [publish_flow.mmd](../../mermaid/publish_flow.mmd)

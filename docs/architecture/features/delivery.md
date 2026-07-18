# Delivery Pipeline Architecture

## Purpose

The delivery module moves scheduled events to subscribers with backpressure, retry, and dead-letter safety.

## Key Files

- [internal/delivery/worker.go](../../../internal/delivery/worker.go) — scheduler-driven worker that drains ready events.
- [internal/delivery/dispatcher.go](../../../internal/delivery/dispatcher.go) — 32-shard dispatcher with credit-based flow control.
- [internal/delivery/retry_queue.go](../../../internal/delivery/retry_queue.go) — non-blocking min-heap retry queue ordered by `retryAt`.
- [internal/delivery/circuit_breaker.go](../../../internal/delivery/circuit_breaker.go) — per-subscription circuit breaker (Closed→Open→HalfOpen).
- [internal/delivery/dlq.go](../../../internal/delivery/dlq.go), [internal/delivery/dlq_segment.go](../../../internal/delivery/dlq_segment.go) — append-only DLQ segments (binary, CRC32, 64MB rotation).
- [internal/delivery/expiry.go](../../../internal/delivery/expiry.go) — message expiry handling for time-bounded deliveries.

## Main Flow

1. Worker is notified by scheduler when events become ready.
2. Dispatcher sends deliveries through subscription streams.
3. Ack success commits progress; failures move to retry queue.
4. Retries use backoff and circuit breaker protections.
5. Exhausted retries move to DLQ for inspection and replay.

## Production Decisions

- Circuit breaker isolates unstable subscribers.
- Retry queue is non-blocking and deadline ordered (no inline sleep in timeout loop).
- Dispatcher sharding reduces lock contention in high concurrency.
- **DLQ is constructed per partition** at create time (`NewDeadLetterQueue` + `NewDispatcherWithDLQ` in `partition.Manager`) so poison messages are not silently dropped.
- Credit-based flow control and in-flight limits protect memory under slow consumers.
- Publish-side admission (ready-queue / wheel / memory) lives in [partition/backpressure.go](../../../internal/partition/backpressure.go).

## Known Limitation: deep requeue under backpressure

This is an **intentional deferred** behavior (remediation plan item 2.5). It **is** part of the
architecture story—not an undocumented footgun.

### What works today

| Mechanism | Behavior |
|-----------|----------|
| Credits | No credit → skip this dispatch pass; metric `cronos_dispatcher_backpressure_skips_total{reason="no_credits"}` |
| In-flight cap | Cap hit → skip/reserve fail with metrics; protects memory |
| Circuit breaker | Open circuit skips send without burning credits |
| Durability | Event remains in **WAL**; not deleted by a skip |
| Resume | Subscribe seeds from **committed offset** so reconnect can resume progress |
| Poison path | After max retries → **DLQ** (wired at partition create) |

### What is deferred

- A **worker-level redrive queue** that, after every credit/in-flight skip, explicitly re-schedules
  the event for another dispatch attempt from the WAL without relying on a later batch or reconnect.
- Full “at-least-once even when consumers stay at zero credits indefinitely” without operator
  intervention (reconnect, credit grant, scale consumers).

### Why deferred

True requeue changes the push-based delivery model and risks double-delivery / hot-path
complexity. Observability (metrics) landed first; redrive is follow-up work.

### Operator guidance

1. Watch `cronos_dispatcher_backpressure_skips_total`.
2. Ensure consumers ack and replenish credits.
3. On prolonged stalls, reconnect consumers (resume-from-committed) or scale consumer concurrency.
4. Do not assume every ready-queue event is in-flight to a subscriber if credits are zero.

Canonical cross-link: [ARCHITECTURE.md § Known Limitations](../../../ARCHITECTURE.md#known-limitations).

## Debug Pointers

- Delivery stalling: [internal/delivery/worker.go](../../../internal/delivery/worker.go)
- Retry storms: [internal/delivery/retry_queue.go](../../../internal/delivery/retry_queue.go)
- Subscriber instability: [internal/delivery/circuit_breaker.go](../../../internal/delivery/circuit_breaker.go)
- Backpressure skips: [internal/delivery/dispatcher.go](../../../internal/delivery/dispatcher.go) + Prometheus metric above

## Related Diagrams

- [delivery_state_machine.mmd](../../mermaid/delivery_state_machine.mmd)
- [publish_flow.mmd](../../mermaid/publish_flow.mmd)

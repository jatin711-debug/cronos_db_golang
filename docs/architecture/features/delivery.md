# Delivery Pipeline Architecture

## Purpose

The delivery module moves scheduled events to subscribers with backpressure, retry, and dead-letter safety.

## Key Files

- [internal/delivery/worker.go](../../../internal/delivery/worker.go)
- [internal/delivery/dispatcher.go](../../../internal/delivery/dispatcher.go)
- [internal/delivery/retry_queue.go](../../../internal/delivery/retry_queue.go)
- [internal/delivery/circuit_breaker.go](../../../internal/delivery/circuit_breaker.go)
- [internal/delivery/dlq.go](../../../internal/delivery/dlq.go)
- [internal/delivery/dlq_segment.go](../../../internal/delivery/dlq_segment.go)

## Main Flow

1. Worker is notified by scheduler when events become ready.
2. Dispatcher sends deliveries through subscription streams.
3. Ack success commits progress; failures move to retry queue.
4. Retries use backoff and circuit breaker protections.
5. Exhausted retries move to DLQ for inspection and replay.

## Production Decisions

- Circuit breaker isolates unstable subscribers.
- Retry queue is non-blocking and deadline ordered.
- Dispatcher sharding reduces lock contention in high concurrency.
- DLQ is durable so failures are observable and recoverable.

## Debug Pointers

- Delivery stalling: [internal/delivery/worker.go](../../../internal/delivery/worker.go)
- Retry storms: [internal/delivery/retry_queue.go](../../../internal/delivery/retry_queue.go)
- Subscriber instability: [internal/delivery/circuit_breaker.go](../../../internal/delivery/circuit_breaker.go)

## Related Diagrams

- [delivery_state_machine.mmd](../../mermaid/delivery_state_machine.mmd)
- [publish_flow.mmd](../../mermaid/publish_flow.mmd)

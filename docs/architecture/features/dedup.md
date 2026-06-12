# Deduplication Architecture

## Purpose

Deduplication prevents duplicate message processing while keeping publish latency low.

## Key Files

- [internal/dedup/store.go](../../../internal/dedup/store.go)
- [internal/dedup/bloom_store.go](../../../internal/dedup/bloom_store.go)
- [internal/dedup/pebble_store.go](../../../internal/dedup/pebble_store.go)
- [internal/dedup/rust_bloom_unix.go](../../../internal/dedup/rust_bloom_unix.go)
- [internal/dedup/rust_bloom_windows.go](../../../internal/dedup/rust_bloom_windows.go)

## Main Flow

1. Handler checks message identifier through dedup manager.
2. Fast probabilistic path handles common-case duplicate checks.
3. Persistent fallback confirms and stores dedup state with TTL behavior.
4. Result controls whether publish continues or exits as duplicate.

## Production Decisions

- Two-tier design balances speed and correctness.
- Rust bloom implementation improves throughput for hot dedup checks.
- Persistent backend prevents relying only on probabilistic memory state.

## Debug Pointers

- Duplicate false positives or misses: [internal/dedup/bloom_store.go](../../../internal/dedup/bloom_store.go)
- Persistence and TTL behavior: [internal/dedup/pebble_store.go](../../../internal/dedup/pebble_store.go)

## Related Diagrams

- [dedup_decision_flow.mmd](../../mermaid/dedup_decision_flow.mmd)
- [publish_flow.mmd](../../mermaid/publish_flow.mmd)

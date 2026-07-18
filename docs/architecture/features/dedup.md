# Deduplication Architecture

## Purpose

Deduplication prevents duplicate message processing while keeping publish latency low.

## Key Files

- [internal/dedup/store.go](../../../internal/dedup/store.go)
- [internal/dedup/bloom_store.go](../../../internal/dedup/bloom_store.go)
- [internal/dedup/pebble_store.go](../../../internal/dedup/pebble_store.go)
- [internal/dedup/rust_bloom_unix.go](../../../internal/dedup/rust_bloom_unix.go) — Unix CGO bindings for the Rust bloom filter.
- [internal/dedup/rust_bloom_windows_cgo.go](../../../internal/dedup/rust_bloom_windows_cgo.go) / [internal/dedup/rust_bloom_windows_nocgo.go](../../../internal/dedup/rust_bloom_windows_nocgo.go) — Windows CGO and non-CGO bindings (the older `rust_bloom_windows.go` was split into these two files when the Windows build added a non-CGO fallback path).
- [internal/dedup/rust/src/lib.rs](../../../internal/dedup/rust/src/lib.rs) — Rust crate: lock-free `AtomicU64` arrays, XxHash64, Rayon parallel batch.

## Main Flow

1. Handler checks message identifier through dedup manager.
2. Fast probabilistic path handles common-case duplicate checks.
3. Persistent fallback confirms and stores dedup state with TTL behavior.
4. Result controls whether publish continues or exits as duplicate.

## Production Decisions

- Two-tier design balances speed and correctness (bloom fast-path, Pebble source of truth).
- Rust bloom (cgo) improves throughput; pure-Go bloom is the Windows non-cgo fallback.
- Publish path **claims** IDs before WAL append and **`RollbackBatch`** on write failure so retries are not spurious duplicates.
- **Crash recovery:** on partition start, `recoverDedupFromWAL` re-seeds recent claims from the WAL tail so NoSync/DisableWAL paths cannot forget IDs after a crash.
- TTL pruning bounds store growth (default 7 days).

## Debug Pointers

- Duplicate false positives or misses: [internal/dedup/bloom_store.go](../../../internal/dedup/bloom_store.go)
- Persistence and TTL behavior: [internal/dedup/pebble_store.go](../../../internal/dedup/pebble_store.go)
- Recovery: [internal/partition/manager.go](../../../internal/partition/manager.go) (`recoverDedupFromWAL`)

## Related Diagrams

- [dedup_decision_flow.mmd](../../mermaid/dedup_decision_flow.mmd)
- [publish_flow.mmd](../../mermaid/publish_flow.mmd)

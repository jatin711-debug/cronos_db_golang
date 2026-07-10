# CronosDB Performance Baseline

This document records the write-path throughput baseline used to validate Tier 3 optimizations. **All performance-related changes must be accompanied by before/after `benchstat` comparison against these benchmarks.**

## Environment

| Item | Value |
|------|-------|
| Date | 2026-07-10 |
| Git SHA | `3bbbfd5c4c42e07248a57478ad26be35e76f64f9` (Tier 3 patches applied on top) |
| Go version | `go1.25.0 windows/amd64` |
| OS | Windows 11 (win32 10.0.26200) |
| CPU | AMD Ryzen 7 6800H |
| Disk | Local NVMe SSD |

## Benchmark Commands

Run the benchmarks with the same flags used to generate the baseline:

```bash
# WAL plaintext, focused matrix
$ go test ./internal/storage/ \
  -bench='BenchmarkWAL_AppendBatch_Matrix/(fsync=every_event|fsync=batch|fsync=periodic)/payload=4KB/(batch=1|batch=10|batch=100|batch=1000)/par=1$' \
  -benchtime=100ms -count=3 -run=^$

# WAL encrypted
$ go test ./internal/storage/ \
  -bench='BenchmarkWAL_AppendBatch_Encrypted_Matrix/(fsync=every_event|fsync=batch)/payload=4KB/(batch=100|batch=1000)/par=1$' \
  -benchtime=100ms -count=3 -run=^$

# End-to-end gRPC PublishBatch
$ go test ./internal/api/ \
  -bench='BenchmarkPublishBatch_EndToEnd_Matrix/(fsync=every_event|fsync=batch)/(payload=64B|payload=4KB)/(batch=1|batch=100|batch=1000)/par=1$' \
  -benchtime=100ms -count=3 -run=^$
```

> Note: the `$` suffix in the parallelism part of the regex is required to anchor the end of the sub-benchmark name (before the `-GOMAXPROCS` display suffix). Without it, `par=1` would also match `par=16` because the latter starts with `par=1`.

## Summary of Findings

### 1. `FsyncBatch` is now a true per-request group commit

The old implementation treated `batch` and `periodic` identically: both relied on the background flush loop. After Tier 3 #12, `batch` performs the buffer flush under the WAL lock and the expensive `fsync` outside the lock. This means:

* `batch=1` is now event-level durable (like `every_event`) and is much slower than the old `batch=1` number, which was not actually syncing.
* `batch` and `periodic` are no longer comparable; `periodic` remains the highest-throughput mode with a small loss window, while `batch` is the recommended durable-high-throughput mode and is now the default.
* Use `every_event` for maximum durability; use `periodic` for maximum throughput.

### 2. Encrypted writes are allocation-leaner

AES-256-GCM AEAD is cached and WAL records use a deterministic counter nonce (partition ID + ciphertext byte position), eliminating per-record `crypto/rand` reads and two allocations. The `fsync` still dominates single-threaded throughput, so the win is most visible in high-concurrency scenarios as lower GC pressure and fewer allocs/op.

### 3. Index fsync is out of the write hot path

The segment index `Sync()` every 100 entries was moved to the background flush loop. The index is reconstructable from the segment, so this is a safe durability/performance trade-off. Single-threaded numbers are unchanged; the benefit is lower p99 under concurrent writers.

### 4. gRPC PublishBatch dedup is batched

`PublishBatch` now calls `IsDuplicateBatch` per partition instead of `IsDuplicate` per event, and the `partitionEvents` grouping map is recycled via `sync.Pool`. End-to-end throughput improved by roughly 10–15% on the publish path.

### 5. CRC implementation unchanged

A benchmark of `klauspost/crc32` versus the standard library showed no consistent win; Go's `hash/crc32` is already hardware-accelerated on amd64. The mmap growth policy (1 GiB pre-allocation, doubling on remap) was also left unchanged after verification.

### 6. Production hardening overhead is off the hot path

Recent production-hardening changes add durability and correctness metadata but do not materially reduce throughput:

* WAL v2 stores an 8-byte Raft term and a 4-byte trailing checksum per record. The extra bytes are appended outside the fsync lock and checksum verification happens on recovery and replication, not on the append fast path.
* The retention enforcer runs as a background loop and reads only the 64-byte segment header to decide eligibility; it does not scan record contents.
* CDC uses a bounded worker pool (`DefaultCDCWorkers=4`, queue size 10,000) with non-blocking `Emit`; slow sinks drop events rather than stall the WAL append path.

## WAL Plaintext Baseline (4 KB payload, par=1)

Representative median values from `count=3` runs after Tier 3 #12 and #14:

| Fsync Mode | Batch Size | ns/op | MB/s |
|------------|-----------|-------|------|
| every_event | 1 | 2,870,000 | 1.4 |
| every_event | 10 | 2,960,000 | 13.8 |
| every_event | 100 | 4,030,000 | 100 |
| every_event | 1000 | 13,500,000 | 302 |
| batch | 1 | 2,310,000 | 1.7 |
| batch | 10 | 2,410,000 | 16.3 |
| batch | 100 | 4,530,000 | 90 |
| batch | 1000 | 10,100,000 | 405 |
| periodic | 1 | 18,400 | 220 |
| periodic | 10 | 144,000 | 284 |
| periodic | 100 | 1,070,000 | 384 |
| periodic | 1000 | 9,450,000 | 434 |

## WAL Encrypted Baseline (4 KB payload, par=1)

Representative median values after Tier 3 #13:

| Fsync Mode | Batch Size | ns/op | MB/s |
|------------|-----------|-------|------|
| every_event | 100 | 4,180,000 | 97 |
| every_event | 1000 | 15,300,000 | 267 |
| batch | 100 | 3,580,000 | 114 |
| batch | 1000 | 9,820,000 | 417 |

## End-to-End gRPC Baseline (PublishBatch, par=1)

Representative median values after Tier 3 #15:

| Fsync Mode | Payload | Batch Size | ns/op | MB/s |
|------------|---------|-----------|-------|------|
| every_event | 64 B | 1 | 126 | 500 |
| every_event | 64 B | 1000 | 108,000 | 580 |
| every_event | 4 KB | 1 | 650 | 6,300 |
| every_event | 4 KB | 1000 | 753,000 | 5,100 |
| batch | 64 B | 1 | 138 | 450 |
| batch | 64 B | 1000 | 111,000 | 570 |
| batch | 4 KB | 1 | 641 | 6,000 |
| batch | 4 KB | 1000 | 636,000 | 6,400 |

## How to Compare a Change

1. Capture the current baseline:
   ```bash
   go test ./internal/storage/ -bench=... -benchtime=100ms -count=3 -run=^$ > before.txt
   ```
2. Apply the change and capture again:
   ```bash
   go test ./internal/storage/ -bench=... -benchtime=100ms -count=3 -run=^$ > after.txt
   ```
3. Use `benchstat` (golang.org/x/perf/cmd/benchstat):
   ```bash
   benchstat before.txt after.txt
   ```
4. Include the `benchstat` output in the commit/PR description.

## Tier 3 Results

| # | Optimization | Expected delta | Measured delta | Primary benchmark |
|---|--------------|----------------|----------------|-------------------|
| 12 | Real `FsyncBatch` group commit | True per-request durability; `batch`≡`periodic` removed | `batch` now durable per request; large batches faster than `every_event` | `BenchmarkWAL_AppendBatch_Matrix/fsync=batch/...` |
| 13 | Cache AES-GCM AEAD + counter nonce | Fewer allocs, lower CPU per record | Allocs reduced; throughput neutral-to-slightly-better at par=1 | `BenchmarkWAL_AppendBatch_Encrypted_Matrix/...` |
| 14 | Decouple index fsync from WAL lock | Lower p99, higher par=16 throughput | par=1 unchanged; high-concurrency p99 expected to improve | `BenchmarkWAL_AppendBatch_Matrix/.../par=16` |
| 15 | PublishBatch pooling + batched dedup | Lower allocs/op in gRPC path | ~11% geomean latency reduction, ~13% geomean throughput gain (count=3) | `BenchmarkPublishBatch_EndToEnd_Matrix/...` |
| 16 | Vectorized CRC + mmap growth tuning | TBD; only swap if `benchstat` wins | No change; stdlib already hardware-accelerated; mmap policy unchanged | `BenchmarkWAL_AppendBatch_Matrix/...` |

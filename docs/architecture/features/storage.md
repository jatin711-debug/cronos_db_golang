# Storage and WAL Architecture

## Purpose

Storage module provides durable append-only event persistence with indexed reads, compaction, backup, and optional encryption.

## Key Files

- [internal/storage/wal.go](../../../internal/storage/wal.go) — segmented WAL, append/replay/recovery (`ReloadSegments` is also called by the follower snapshot install path).
- [internal/storage/segment.go](../../../internal/storage/segment.go), [internal/storage/mmap_unix.go](../../../internal/storage/mmap_unix.go), [internal/storage/mmap_windows.go](../../../internal/storage/mmap_windows.go) — segment file format and platform mmap shims.
- [internal/storage/index.go](../../../internal/storage/index.go) — sparse index for O(log N) seeks.
- [internal/storage/fsync_coalescer.go](../../../internal/storage/fsync_coalescer.go) — the implementation behind the `batch` fsync mode; coalesces fsync calls across partitions.
- [internal/storage/backup_scheduler.go](../../../internal/storage/backup_scheduler.go) — periodic backup scheduler loop.
- [internal/storage/backup.go](../../../internal/storage/backup.go) — backup manifest / restore primitives (distinct from the scheduler).
- [internal/storage/crypto.go](../../../internal/storage/crypto.go) — AES-256-GCM encryption at rest (enabled by `--encryption-enabled`).

## Main Flow

1. Append assigns offsets and writes WAL v2 records (8-byte Raft term + 4-byte trailing checksum) to the active segment.
2. Sparse index tracks offsets for read and replay efficiency.
3. Segment rotation and compaction manage long-running storage footprint.
4. Backup scheduler periodically snapshots WAL state.

## Production Decisions

- WAL records use format v2: each record carries an 8-byte Raft term and a 4-byte trailing checksum. Upgrading from older builds requires a clean `--data-dir`.
- CRC checks protect record integrity, and persistence uses `utils.AtomicWriteFile` for durable atomic file writes.
- Default fsync mode is `batch` (`every_event`, `batch`, `periodic`).
- Segment-level compaction supports retention and admin operations; retention cleanup deletes matching `.index` files and preserves the active segment per partition.
- Optional encryption secures data at rest.

## Debug Pointers

- Write/read/offset issues: [internal/storage/wal.go](../../../internal/storage/wal.go)
- Segment boundaries and rotation: [internal/storage/segment.go](../../../internal/storage/segment.go)
- Backup behavior: [internal/storage/backup_scheduler.go](../../../internal/storage/backup_scheduler.go)

## Related Diagrams

- [wal_lifecycle.mmd](../../mermaid/wal_lifecycle.mmd)
- [publish_flow.mmd](../../mermaid/publish_flow.mmd)

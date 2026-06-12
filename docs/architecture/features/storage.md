# Storage and WAL Architecture

## Purpose

Storage module provides durable append-only event persistence with indexed reads, compaction, backup, and optional encryption.

## Key Files

- [internal/storage/wal.go](../../../internal/storage/wal.go)
- [internal/storage/segment.go](../../../internal/storage/segment.go)
- [internal/storage/index.go](../../../internal/storage/index.go)
- [internal/storage/backup_scheduler.go](../../../internal/storage/backup_scheduler.go)
- [internal/storage/crypto.go](../../../internal/storage/crypto.go)

## Main Flow

1. Append assigns offsets and writes records to active segment.
2. Sparse index tracks offsets for read and replay efficiency.
3. Segment rotation and compaction manage long-running storage footprint.
4. Backup scheduler periodically snapshots WAL state.

## Production Decisions

- CRC checks protect record integrity.
- Configurable fsync modes expose throughput and durability tradeoff.
- Segment-level compaction supports retention and admin operations.
- Optional encryption secures data at rest.

## Debug Pointers

- Write/read/offset issues: [internal/storage/wal.go](../../../internal/storage/wal.go)
- Segment boundaries and rotation: [internal/storage/segment.go](../../../internal/storage/segment.go)
- Backup behavior: [internal/storage/backup_scheduler.go](../../../internal/storage/backup_scheduler.go)

## Related Diagrams

- [wal_lifecycle.mmd](../../mermaid/wal_lifecycle.mmd)
- [publish_flow.mmd](../../mermaid/publish_flow.mmd)

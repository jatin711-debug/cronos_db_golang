# Storage and WAL Architecture

## Purpose

Storage module provides durable append-only event persistence with indexed reads, compaction, backup, and optional encryption.

## Key Files

- [internal/storage/wal.go](../../../internal/storage/wal.go) — segmented WAL, append/replay/recovery. A follower's snapshot install goes through `WAL.InstallCheckpoint` (journaled swap of the staged log; see [replication.md](replication.md#bulk-install-snapshot)).
- [internal/storage/retention.go](../../../internal/storage/retention.go) — `WAL.Prune`: verifies closed segments record by record against the caller's eligibility and terminal-state predicates.
- [internal/partition/retention.go](../../../internal/partition/retention.go) — `Partition.PruneWAL`, the one path by which compaction, retention and operator pruning remove segments; `FollowLogStart` on followers.
- [internal/storage/segment.go](../../../internal/storage/segment.go), [internal/storage/mmap_unix.go](../../../internal/storage/mmap_unix.go), [internal/storage/mmap_windows.go](../../../internal/storage/mmap_windows.go) — segment file format and platform mmap shims.
- [internal/storage/index.go](../../../internal/storage/index.go) — sparse index for O(log N) seeks.
- [internal/storage/fsync_coalescer.go](../../../internal/storage/fsync_coalescer.go) — shared periodic flusher for many WALs (used with `periodic` / background flush; **not** the same as `batch` group-commit).
- [internal/storage/backup_scheduler.go](../../../internal/storage/backup_scheduler.go) — periodic backup scheduler loop.
- [internal/storage/backup.go](../../../internal/storage/backup.go) — backup manifest / restore primitives (distinct from the scheduler).
- [internal/storage/crypto.go](../../../internal/storage/crypto.go) — AES-256-GCM at rest; **record cipher v2** embeds a random 12-byte nonce per record (v1 counter nonce is decrypt-only).
- [internal/lifecycle](../../../internal/lifecycle) — self-contained mutation records and the payload-free disk index of logical heads.
- [internal/recoverystate/file.go](../../../internal/recoverystate/file.go) — bounded, checksummed recovery pages bound to a WAL generation; encrypted with its key when enabled.

## Main Flow

1. Append assigns offsets and writes WAL v2 records (8-byte Raft term + 4-byte payload checksum) to the active segment.
2. Sparse index tracks offsets for read and replay efficiency.
3. Durability depends on fsync mode:
   - `every_event` — fsync each write
   - `batch` (default) — **group-commit** fsync (concurrent writers share one sync)
   - `periodic` — background / coalescer flush with a bounded loss window
4. Segment rotation keeps prior segments **readable**; compaction and retention reclaim space.
5. The backup scheduler periodically checkpoints every partition, including
   the active segment. A checkpoint pauses appends only while it notes where
   each file ends; the bytes are copied while appends continue. A backup cuts
   the logs of all partitions at one instant and starts at wall-clock
   multiples of the interval. These are independent node checkpoints, not a
   cluster-wide atomic cut; cluster restore uses the earliest required cut.

## Production Decisions

- WAL records use format v2: each record carries an 8-byte Raft term and a 4-byte payload checksum (written immediately after the payload, before metadata). Pre-v2 layouts have no automatic migration; this is separate from the capability-gated v0.7.0 to v0.8.0 upgrade.
- CRC checks protect record integrity; critical metadata uses `utils.AtomicWriteFile`.
- Default fsync mode is `batch`.
- Encryption at rest v2 uses random GCM nonces to avoid cross-segment keystream reuse; v1 remains readable for migration.
- Segment-level compaction supports retention and admin operations; retention never deletes the active segment and removes matching `.index` files.
- **Every record a log takes can be read back.** A record's length field may say at most 10 MiB (`storage.MaxRecordLength`); readers and recovery take a larger length for damage. An event is therefore taken only if its record, metadata included, is at most `storage.MaxEventRecordSize`, 64 bytes less, which leaves room for encryption, so encrypted and plaintext replicas take the same events. Publishes check this before they claim the message ID (`storage.CheckEventSize`), and the log checks it again. Up to v0.6.0 a publish limited only the payload, to 4 MiB, and an event with a lot of metadata could be acknowledged and then not be read.
- **The term of an entry is the log's.** An entry a log appends for itself (a publish, a cross-region event, a split, a transaction) is stored under the log's current term, whatever term the event carried; only a follower stores the term its leader sent. Elections and the matching of logs rely on terms saying which leader wrote an entry. Up to v0.6.0 a producer could put any term in an event and have it stored and replicated.
- **A failed write stops the log, and the node.** After a write or fsync of a segment fails, nothing says what the disk holds: a failed fsync may drop the pages it did not write, so a later fsync that succeeds proves nothing about them. The log then takes no more appends (`storage.ErrLogFailed`), a follower whose log failed acknowledges nothing, and the node shuts down and exits with status 1, so that it is restarted, reads its logs again from disk, and other replicas lead its partitions meanwhile. An append reports whether its events reached the log (`storage.EventsAppended`): a publish whose events did is held for its retry to finish, as when replication fails, and only one whose events did not gives its message IDs back. Up to v0.6.0 every append error gave them back, so a retry could append the same event a second time, and background flush errors were only counted.
- **A segment that cannot be started does not fail an append.** Rotation happens after the append that filled the segment, which is complete by then. If the new segment cannot be made, the full one takes the next append, which tries again; `cronos_wal_rotation_errors_total` counts the failures.
- **Opening a log reads each segment once**, record by record, checking every CRC. It used to read each segment a second time to verify it, the whole segment into memory at once, so starting a node took as much memory as its largest segment (512 MiB by default).
- **Publishes stop before the disk is full.** When the filesystem holding `--data-dir` is `--max-disk-usage` full (default 95%), publishes are refused with `RESOURCE_EXHAUSTED` until it is two points below; deliveries, acknowledgements, replication and retention go on. `cronos_disk_publish_refused` is 1 meanwhile. Usage is sampled every 2 seconds, so a publish costs one atomic load.
- **A segment holds a write buffer only while it is written through one.** An unencrypted segment is written through its mapping and has none. An encrypted segment is written through a 4 MiB buffer, which is made on its first append and released when the segment stops taking appends. Every segment used to get the buffer when it was created or opened and keep it, so a node held 4 MiB of heap for each segment of its logs.
- **An encrypted record that was not completely written is a torn write, like a plaintext one.** The buffer can put out part of a record and keep the rest until the next flush, so a crash can leave a final record that does not decrypt. When nothing is written after it and its last bytes are still unwritten zeros, it is treated like a plaintext record whose checksum fails (below): cut off at the end of the log, damage anywhere before it. The first record of a segment counts as torn only when its whole 16-byte authentication tag is unwritten, so that a wrong key, which fails every record, is never taken for a torn write; any other record that does not decrypt still stops the log from opening. Before 1.0 every decryption failure stopped it, and an encrypted node that crashed during a write could not start, nor be repaired with `check-log --repair`.
- **The sparse index only says where to start reading.** Its file is written through a buffer that holds a whole number of 24-byte entries, so what the buffer puts out on its own is whole entries; before 1.0 it put out 64 KiB, 16 bytes into an entry, and in production nothing else flushed it until the segment closed. Opening an index keeps its entries up to the first that cannot be right (a partial entry, or an offset or position that does not follow the one before) and cuts the file there. A read that the index points to the wrong place reads the segment from its start. Before 1.0 a partial entry made later entries misaligned after the next restart, and reads of the events they pointed to returned nothing, without an error.
- **Bytes that are not valid records** mean different things in different places, and the log treats them accordingly when it opens ([wal.go](../../../internal/storage/wal.go), `judgeInvalidTails`):
  - *At the end of the log*, absent evidence of a later record, they are treated as a write that did not finish. The log ends before them, a warning says so, and they are overwritten with zeros. In the `batch` and `every_event` fsync modes an interrupted final write should not contain an acknowledged event.
  - *Anywhere else*, inside a segment that later segments follow, they are damage: segments are closed complete, so records were there. The partition refuses to open and the error names the segment, the byte and the offsets that cannot be read. Opening used to take such bytes for the end of the segment and carry on without the events behind them.
  - Repair depends on whether the partition has other replicas. **In a cluster**, stop the node and remove the partition's directory (`partitions/<id>`): the node rejoins the partition with nothing and the leader fills it again; nothing is lost. **Without another replica**, restore a backup (`cronos-admin restore`), or run `cronos-admin check-log --data-dir <dir> --partition <id> --repair` with the node stopped, which cuts the damaged segment after its last valid record and reports the offsets it gives up. `cronos-admin check-log --data-dir <dir>` alone only reports.
  - A newly created successor records its closed predecessor's durable offset and byte boundary in its checksummed header. Recovery and `check-log` reject a mismatch even if the erased area is all zeros and the sparse index is missing. The active tail does not have this evidence. A verified later indexed record can additionally prove some damage there. Explicit repair discards the remainder of the damaged segment, including any valid records after the damaged area; when there is no successor, the last discarded offset is unknown. Version 0.9.0 and later require a fresh data directory: they reject version-1 WALs, and older binaries reject version-2 WALs. See [closed segment boundaries](../../STORAGE_SEGMENT_BOUNDARIES.md) for deployment, repair and interrupted-truncation behavior.
- **What may go.** Ordinary active events must be due and finished by every
  relevant consumer group, with at least one such group; export and durable
  dead-letter replication must also permit pruning. Accepted cancellations,
  superseded copies and term-start records can be terminal without a consumer
  completion. Unaccepted records cannot release a segment. A pass runs every
  `--compaction-interval` (default 10 minutes), checking one record at a time.
- **Prefix pruning.** Replicated partitions and standalone lifecycle partitions
  remove only a contiguous prefix of closed segments. In a cluster the leader
  decides and followers follow its log start. Without relocation, one future
  event can still pin later segments. `--wal-relocation-bytes` can copy
  never-due work to a retained successor within a byte budget; it requires
  lifecycle mode and is off by default. A replicated copy must be committed
  before the original can go. Unfinished due work and the active segment are
  never evacuated. A standalone partition without lifecycle mode may remove
  finished segments elsewhere in its log. See the [contract](../../RETENTION_AND_EVENT_LIFECYCLE.md#retention-and-relocation).
- **Retention by age and size** (`--retention-max-age-hours`, `--retention-max-size-gb`) runs hourly ([compliance/retention.go](../../../internal/compliance/retention.go)). It only picks segments; each is removed through `PartitionManager.RemoveRetainedSegment` → `Partition.PruneWAL`, with the same checks as compaction. In a cluster a replica that does not lead the partition removes nothing, and the leader removes only from the start of the log.
- **Consumer progress follows the log.** What lies below the start of a log was finished before it was removed, so it counts as complete for every group, including one created later, and each group's completion floor is raised to the start. That also discards the per-event completion records below it.
- **Logical identity survives physical moves.** A lifecycle successor retains
  the original reference but has a new physical offset. An old completion floor
  cannot finish it. Lifecycle backups and snapshots include the matching
  accepted boundary, epoch, group progress and dead letters; recovery resumes
  interrupted state import before delivery. A local index alone is not backup
  authority; it is reconstructed from retained WAL records.

## Debug Pointers

- Write/read/offset issues: [internal/storage/wal.go](../../../internal/storage/wal.go)
- Segment boundaries and rotation: [internal/storage/segment.go](../../../internal/storage/segment.go)
- Backup behavior: [internal/storage/backup_scheduler.go](../../../internal/storage/backup_scheduler.go)

## Diagrams

### WAL lifecycle

```mermaid
flowchart TB
    Append["AppendEvent / AppendBatch"] --> Active["Write active segment<br/>WAL v2: term + payload checksum<br/>optional AES-GCM record v2 random nonce"]
    Active --> Index["Update sparse index"]
    Index --> Flush{Fsync mode}
    Flush -- every_event --> SyncNow["fsync every write"]
    Flush -- batch --> GroupCommit["Group-commit fsync<br/>one leader sync per concurrent batch"]
    Flush -- periodic --> Coalesce["Background / FsyncCoalescer sweep<br/>loss window up to flush interval"]
    Active --> Rotate{Segment full?}
    Rotate -- yes --> NewSegment["Rotate; keep prior segment readable"]
    Rotate -- no --> Active
    NewSegment --> Compaction["Compaction every --compaction-interval<br/>closed segments; active events completed and exported<br/>accepted terminal controls/copies may also be pruned"]
    Compaction --> Retention["Retention enforcer (hourly): age/size picks segments<br/>removed via PruneWAL, same checks<br/>cluster: leader only, start of log only<br/>never deletes active segment"]
    Retention --> Backup["Backup scheduler"]
```

### Related diagrams

- [Publish flow](api.md#publish-flow)

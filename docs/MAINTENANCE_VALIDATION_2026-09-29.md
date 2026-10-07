# WAL maintenance and pipeline validation — 2026-09-29

This change fixes the backup path, unsafe live-file retention, and a WAL rotation
offset defect exposed while testing retention. Existing unrelated working-tree
hardening changes were preserved.

## Behavior

- Scheduled backups capture every loaded partition into
  `backups/backup-*/partitions/<id>`, laid out like the partition's data
  directory: the WAL including its active segment, consumer groups with their
  offsets and completion records, the dedup store, the dead-letter queue, and
  the fencing epoch (updated 2026-10-06; earlier generations hold the WAL
  only). `backup.json` lists the partitions, their last offsets, and the size
  and CRC-32 of every file. A generation becomes visible only when every
  partition succeeds. Failed attempts do not expire previous backups. Shutdown
  waits for active backup and retention work before closing the WAL.
- A partition is captured while it serves traffic, in an order that makes the
  result safe to restore: the log is copied last, so the other stores describe
  a slightly older moment than the log and never a newer one. A restored node
  may deliver again what was completed just before the backup. It cannot hold
  a completion or dedup record for an offset its log does not contain, which
  would silently drop the next event given that offset.
- Each generation is independent. With the node stopped,
  `cronos-admin restore --from <backup-dir> --data-dir <dir>` checks every file
  against the manifest and restores all partitions or none; it refuses a data
  directory that already holds one of them. Start the node on the restored
  directory and provide the original key for encrypted data; timers are rebuilt
  from the log. Tests restore plaintext and encrypted logs after later source
  writes, a whole partition's state into a fresh node, and damaged, incomplete
  and misplaced backups.
- Since 2026-10-07 the logs of all partitions on a node are cut at one
  instant, recorded as `cut_at` in the manifest, and scheduled backups start at
  wall-clock multiples of the interval, so the nodes of a cluster take theirs
  at the same moment as far as their clocks agree. For an encrypted log the
  manifest carries a value that identifies the key without containing it;
  `cronos-admin restore --encryption-key-file` checks the key against it before
  anything is written, and a wrong key leaves the data directory untouched.
- A backup holds no key material, credentials or Raft metadata. It does not
  need the last: when a restored cluster forms, placement is recomputed, each
  partition is first led by the replica with the most complete log, and epochs
  continue above those stored with the data. The backups of different nodes
  remain separate copies that agree only as closely as the schedule and the
  clocks do, which is why the most complete replica leads. This path is
  tested with stand-in replicas; a restore of a real multi-node cluster has
  not been run.
- Automatic compaction and both admin APIs use the live WAL's deletion lock.
  Every event must be past due and have a per-event completion record in every
  matching assigned consumer group. No matching group means keep the event.
  Cursor overrides and `Force` do not override this protection. Corrupt or
  incomplete segments fail verification; the active segment is always retained.
- Clustered/replicated pruning returns an explicit error because durable
  completion replication and safe handoff watermarks are still unresolved.
  Size limits are consequently best-effort: protected data can exceed them;
  admission/disk-pressure controls remain responsible for rejecting more writes.
- WAL rotation now derives the next segment's first offset from the last record
  actually written. Reservations by concurrent writers and speculative creation
  previously produced incorrect segment boundaries. Speculative segment creation
  was removed; rotation may incur synchronous creation latency.

## Correctness checks

Regression tests cover complete independent backups, active tails, encryption,
concurrent checkpoint/appends, failed backup publication, shutdown waiting,
retention accounting, topic/group isolation, future/unacknowledged events,
nonzero partition IDs, and cancellation/corruption protection.

`TestMaintenanceConcurrentRotationPreservesEveryAcceptedOffset` failed before
the rotation fix (`offset gap: got 4 want 7`) and passes after it, including a WAL
reopen and a ledger of all accepted IDs. The gRPC pipeline regression publishes,
delivers and ACKs 64 unique events, checks durable completion, takes a checkpoint,
prunes completed records, and verifies that a future timer remains readable.

The CDC tests were rewritten on 2026-10-07 for ordered, retryable delivery:
order across sinks, a failing sink resuming without the others receiving
anything twice, and refusal after shutdown.

## Throughput method

The publish benchmark now creates routable partitions, uses unique message IDs,
applies its concurrency parameter, rejects partial/duplicate/failed batches, and
excludes shutdown from timing. Both before and after binaries use this identical
harness with real partition deduplication. The original harness could benchmark
rejected or duplicate requests as apparent throughput.

Comparison uses Windows/amd64, Go 1.26.7, CGO disabled, four Go execution threads,
four publisher goroutines, 256-byte payloads, batches of 1,000, and five samples
of two seconds each for `batch` and `periodic` fsync. This measures single-node
SDK/gRPC publish acceptance through dedup, WAL and scheduling; delivery correctness
is checked separately. It does not establish replicated cluster capacity or
production tail latency during large checkpoint copies.

Capture matching logs with:

```powershell
$env:GOARCH = 'amd64'
$env:CGO_ENABLED = '0'
go test ./internal/api -run '^$' -bench '^BenchmarkPublishBatch_EndToEnd_Matrix/fsync=(periodic|batch)$/payload=256B$/batch=1000$/par=1$' -benchtime=2s -count=5 -cpu=4 -benchmem -timeout 180s
python scripts/check-publish-throughput.py before.txt after.txt --max-regression-percent 10
```

The comparison script checks matching configurations and successful benchmark
runs, then compares medians. Raw local logs and compiled before/after binaries
are under `build/maintenance-validation/` (ignored build artifacts).

## Follow-up: dedup completion batching

Accepted publish batches now finalize their dedup offsets with one Pebble batch
commit instead of one `Put` per event. Pending claims remain visible until the
commit succeeds; the completion test checks offsets after closing and reopening
the store. The offline retention test now expects a deleted WAL segment's matching
sparse index to be removed while unrelated index and system metadata survive.

`GOARCH=amd64 CGO_ENABLED=0 go test ./... -timeout 180s` passed. Five paired
two-second samples on this machine yielded median accepted-event rates:

| Mode | Before batching | After batching | Change |
|---|---:|---:|---:|
| `batch` | 124,577 events/s | 121,630 events/s | -2.37% |
| `periodic` | 191,507 events/s | 206,497 events/s | +7.83% |

The 10% regression gate passed. A saved pre-maintenance binary rerun in the
same session measured 119,166 and 186,364 events/s, respectively. Earlier
pre-maintenance logs were considerably faster than both paired runs; the
machine's absolute rate varied, so this is evidence for the paired local check,
not a precise capacity estimate. Logs are `current-before.txt`,
`current-after.txt`, and `current-old-binary.txt` in the build directory above.
The dedup completion and gRPC publish/deliver/ACK maintenance regressions also
passed with the race detector after placing the staged Rust DLL on Windows `PATH`.

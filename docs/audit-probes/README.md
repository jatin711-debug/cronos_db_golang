# Production audit probes

These focused regression probes accompany the [production audit](../PRODUCTION_AUDIT_2026-09-27.md) of commit `1de04d6575a810c79f716dfe48b54e1744b42a33`.

They assert the desired behavior and **fail against the audited implementation**. They are stored as `.go.txt` and added to their packages through Go's `-overlay` option. Normal test runs are unchanged. Probe data uses Go test temporary directories; no production database or running cluster is needed.

## Run

From the repository root, with Python 3.9+ and the repository's Go toolchain available:

```text
python docs/audit-probes/run.py
python docs/audit-probes/run.py --race
```

The first command runs 15 functional probes with `GOARCH=amd64 CGO_ENABLED=0`. A nonzero exit code is the expected result until the defects are fixed. The second runs one concurrency probe with cgo and the Go race detector; it requires a compatible C compiler and the Rust dedup library built using the repository's normal cgo setup. The runner preserves `CC`, linker flags and dynamic-library search paths from the calling environment. Do not use its results to claim validation on another architecture.

For the audit's Windows environment, the race run used these process-local settings before invoking Python:

```powershell
$env:CC = 'C:\msys64\ucrt64\bin\gcc.exe'
$env:PATH = "$PWD\internal\dedup\rust\target\release;$PWD\internal\dedup;$env:PATH"
python docs/audit-probes/run.py --race
```

Those paths are machine-specific prerequisites, not installation instructions. Functional probes do not need them. After a fix, move the relevant probe into the normal package tests and strengthen it with end-to-end assertions where appropriate.

## Recorded results

| Probe | Observed result | Finding |
|---|---|---|
| `TestAuditReplayFiltersAuthorizedTopic` | Request for `allowed` returned event from `secret` | F01 |
| `TestAuditNegativeUntrackedAckMustNotCommit` | Fabricated negative ACK advanced offset to 1,000,000 | F03 |
| `TestAuditWorkerOwnsDispatchSlice` | Go race detector reported concurrent read/write of dispatch slice | F04 |
| `TestAuditSnapshotRestoresPendingTimer` | One pending timer became zero after snapshot/restart | F05 |
| `TestAuditHydratorRecoversMissedWindow` | Cold event already inside hot window was not hydrated | F05 |
| `TestAuditPromotionSchedulesReplicatedEvents` | Existing follower promoted with zero timers for its replicated event | F05 |
| `TestAuditDelayedAppendMustNotTruncateAcknowledgedTail` | Repeating offset 0 erased previously accepted offset 1 | F06 |
| `TestAuditZeroTermCannotBypassFence` | Term 0 append accepted at epoch 10 | F06 |
| `TestAuditQuorumFailureRetryMustNotReportSuccess` | Failed quorum retry returned success, one duplicate, zero timers | F07 |
| `TestAuditWrongEncryptionKeyFailsClosed` | Populated encrypted WAL opened writable at next offset 0 | F10 |
| `TestAuditDLQRetainsEntryAfterRestart` | One DLQ entry became zero after reopen | F11 |
| `TestAuditUnorderedTimestampsRemainQueryable` | Time-range lookup missed an existing matching event | F12 |
| `TestAuditConsumerReturnsAfterStreamFailure` | Consumer returned only after outer context cancellation | F13 |
| `TestAuditDeploymentEnvironmentIsHonored` | Partition count, replication factor, fsync and cluster address overrides ignored | F14 |
| `TestAuditCannotCommitAbortedTransaction` | Commit accepted an aborted transaction | F16 |
| `TestAuditPrepareFailureReturnsWithoutDeadlock` | Prepare remained blocked beyond its context deadline | F16 |

The replay and ACK probes exercise the underlying engine/manager; the report separately traces the public handlers that make those paths reachable. The replication probes exercise RPC handlers directly, without simulating a network partition. The race probe exercises real worker/dispatcher code. These are targeted reproductions, not a distributed load or security penetration test.

Sanitized output from the executed checks is in [evidence](evidence/).

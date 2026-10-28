# 1.0.0 readiness review — 2026-10-28

Reviewed source: `460f8a2` on `main` (v0.9.0 source line). This is an assessment,
not a release approval or an implementation of the fixes.

**The earlier nine findings are not the complete work list.** This follow-up
reproduced them and confirmed four additional defects, for **at least thirteen
known issues**. Passing the existing tests does not establish completeness:
the three new Go probes and the new chart assertion fail while the corresponding
ordinary SDK, backup and chart tests pass.

## Resolution

All thirteen are fixed on branch `fix/1.0-readiness`. Each probe became a
regression test that fails on `460f8a2` and passes with the fix; the opt-in
`releaseaudit` probes and the `zz_audit_*` files are gone. The release gates
below are not covered by these fixes and remain open.

| # | Fix | Regression tests |
|---|---|---|
| 1 | A replica that placement drops goes on the partition's `Retiring` list: elections ask it and count it, and it keeps its copy. A leader drops former followers before it reports the new epoch, and the list is cleared once that epoch is reported and `minISR`−1 remaining replicas hold the leader's log from that point. The leader used to keep replicating to dropped nodes and counting their acknowledgements. | `internal/cluster/retiring_replicas_test.go` |
| 2 | The wheel advances to the tick the clock has reached, at most 1000 ticks per fire, never past it. | `internal/scheduler/wheel_clock_test.go` |
| 3 | A filter rebuild reloads Pebble's IDs, with every check going to Pebble until it has; claims pause only for the snapshot. The rebuild trigger is the false-positive rate over checks of absent IDs. | `internal/dedup/bloom_refresh_test.go` |
| 4 | A record that does not decrypt, with nothing written after it and unwritten trailing bytes, is a torn write, judged like a plaintext checksum failure. The first record of a segment needs its whole tag unwritten, so a wrong key still fails closed. | `internal/storage/torn_encrypted_tail_test.go` |
| 5 | `Leader.Replicate` replicates an entry below the current term's start record through that record. | `internal/replication/term_start_retry_test.go` |
| 6 | The index buffer holds whole entries; opening an index cuts it after its last valid entry; a read the index points to the wrong place rescans the segment. | `internal/storage/index_alignment_test.go` |
| 7 | Dedup records are kept per topic and message ID (`dedup.Key`). Records written before 1.0 are not carried over. | `internal/api/client_limits_test.go` |
| 8 | Group names and subscription IDs are at most 255 bytes; a partition holds at most `--max-consumer-groups` groups (default 10000). Existing groups are kept and can be joined. | `internal/api/client_limits_test.go` |
| 9 | Cancellation ends a replay's pause; `speed` must be 0 or at least 0.001. | `internal/api/client_limits_test.go` |
| 10 | The SDK saves only the completion floor the server reports, 0 included, and in async mode from the server's answers rather than at send time. | `pkg/client/ack_commit_test.go` |
| 11 | With `CommitStrategySync`, `Ack` waits for the server's answer and returns refusals, stream failures and cancellation. | `pkg/client/ack_commit_test.go` |
| 12 | A partition backup includes `changefeed.json`, taken before the log is cut. | `internal/partition/changefeed_backup_test.go` |
| 13 | The disruption budget allows one pod at a time (`podDisruptionBudget.maxUnavailable`), and the production values require one pod per node (`podAntiAffinity: hard`). `scripts/check-production-chart.py` asserts both. | `scripts/check-production-chart.py` (`check_disruption`) |

Corrections to the findings below: #6 is reachable in ordinary operation, not
only after a partial write such as a full disk, because production WALs use
the shared fsync coalescer, which never flushed the index. #13 costs
availability during voluntary disruptions; evicted pods keep their volumes,
so "lose all of its replicas" means unavailable, not lost.

The assumed 1.0 scope is the existing supported product: fixed partitions,
replicated durable publishing, scheduled at-least-once delivery, replay,
the Go SDK, authenticated topic access, backups, and optional Kafka/webhook CDC.
Transactions, online splitting and cross-region replication remain experimental.
Features deliberately excluded from 1.0 need explicit documentation and runtime
gating; they do not need to be completed merely to assign a 1.0 version.

## Four additional confirmed issues

### 10. SDK resume checkpoints can skip unfinished deliveries — high priority

`pkg/client/consumer.go:954` saves the ACK request's next offset in asynchronous
mode immediately after sending, without waiting for the server's committed
floor. In synchronous mode, lines 946–948 use the same value when the server
correctly reports a committed floor of **zero**. A delivery's highest offset
does not prove earlier offsets were processed: scheduling and concurrent handlers
allow completion out of order.

**Reproduction:** the server acknowledges delivery of offset 10 while reporting
committed floor 0. With an explicitly configured checkpoint store, both modes
save 11 and reconnect with `StartOffset=11`. The server uses that as the minimum
redrive origin (`internal/api/handlers.go:1363`); queued retries and its safety
sweep also respect that minimum (lines 1450 and 1492). Previously due, unfinished
records below it can therefore be skipped. They remain in the WAL.

Probe: `TestReleaseAudit_CheckpointMustNotSkipUnfinishedOffsets` in
`pkg/client/zz_release_readiness_test.go`. Evidence: `build/audit-1.0-sdk.log`.
This needs no malicious client. The default configuration without a checkpoint
store avoids this particular path.

**Required outcome:** persist only a server-confirmed contiguous completion
floor; preserve zero as a valid floor. Reconnect after refused/lost ACKs and
out-of-order completion must still deliver every unfinished event. Include
failover, since server completion replication is asynchronous and a client-side
checkpoint must not override a successor's older safe progress.

### 11. SDK synchronous ACK returns before server confirmation — medium priority

`CommitStrategySync` promises that ACK waits for the RPC outcome, but
`Delivery.AckSuccess` ultimately returns when `worker` enqueues an ACK
(`pkg/client/consumer.go:853`). `ackSender` receives the server response on
another goroutine; no result is returned to the original caller.

**Reproduction:** a test gRPC server withholds every ACK response.
`AckSuccess` with a 150 ms deadline returns `nil` immediately even with
`CommitStrategySync`. A caller cannot rely on the documented commit boundary
or act on a server refusal.

Probe: `TestReleaseAudit_SyncAckMustWaitForServer` in
`pkg/client/zz_release_readiness_test.go`. Evidence: `build/audit-1.0-sdk.log`.

**Required outcome:** synchronous ACK waits for its matching response and returns
refusal, stream failure or cancellation to the caller. This does not imply
exactly-once delivery or quorum replication of consumer progress.

### 12. Backup/restore silently drops pending CDC export work — high priority

`internal/partition/backup.go:173` checkpoints the dedup, consumer and DLQ stores,
and copies epoch/format markers, but omits `changefeed.json`. On restore,
`openChangeFeed` interprets the missing cursor as a newly enabled feed and starts
at the **end of the restored log** (`internal/partition/changefeed.go:114`).

**Reproduction:** accept three records while the CDC sink is down, take a backup,
and restore it through `storage.RestoreBackup`. All three records are present in
the restored WAL. The export cursor changes from -1 to 2 without exporting any
of them. Recovering the destination therefore does not retry those records.
The probe holds the restored sink unavailable while checking the cursor so
legitimate fast export cannot be mistaken for the bug. The probe uses
`fsync-mode=every_event`; this is not a periodic-durability artifact.

Probe: `TestReleaseAudit_BackupPreservesPendingChangeFeed` in
`internal/partition/zz_release_readiness_test.go`. Evidence:
`build/audit-1.0-backup.log`.

**Required outcome:** include a conservative, durable feed position in the
backup manifest and restore it with the log. Test a failing sink, concurrent
publishes and exports, and restart after restore. Missing/invalid recovery
metadata must not silently certify unexported entries as exported. Define how
older backups without a cursor are handled.

### 13. Production disruption budget permits losing quorum — high priority

`charts/cronos-db/templates/poddisruptionbudget.yaml:14` derives global
`minAvailable` from the **partition replication factor**, not the number of
cluster voters or partition placement. Production values render five pods,
RF=3 and minISR=2, but `minAvailable=2`. Thus the PDB permits three simultaneous
voluntary evictions from a healthy five-node cluster. Two remaining voters
cannot form a five-voter Raft majority, and a three-replica partition can lose
all of its replicas. Readiness changes after an eviction do not prevent requests
already admitted concurrently under that budget.

Probe: a script that rendered the actual production chart and failed the
quorum assertion; it is now `check_disruption` in
`scripts/check-production-chart.py`. Evidence:
`build/audit-1.0-chart-probe.log`. This is a rendered-configuration proof, not a
live Kubernetes drain experiment. The existing chart assertion script passes.

**Required outcome:** protect both Raft and partition availability. For the
default RF=3/minISR=2 configuration, permitting at most one concurrent voluntary
eviction is the conservative starting point; verify recovery/readiness before
the next. Also provide/test failure-domain placement: the production values
currently inherit empty affinity, so distinct pods are not guaranteed distinct
hosts. PDBs alone do not protect against host failure or controller-driven
updates.

## Earlier findings rechecked

All nine reproduced on Linux amd64 with Go 1.26.10 and `CGO_ENABLED=0`.
The existing `internal/*/zz_audit_*_test.go` files were preserved unchanged.

1. **Unsafe replica replacement on node join.** Both the one-join/lagging-replica
   and two-join/empty-replica election probes fail. Outgoing nodes still have
   data, but are excluded from the committed election set. Fixing this requires
   a quorum-safe membership transition, including writes and elections during
   catch-up; simply adding a copy step is not sufficient evidence of safety.
2. **Permanent scheduler drift after missed ticks.** Both hydration and repeated
   short-stall probes fail. Clock catch-up must also remain bounded and preserve
   the no-early-delivery guarantee.
3. **Bloom maintenance forgets accepted IDs.** The existing-ID retry is accepted
   again after maintenance. Rebuild/reset must preserve correctness while
   concurrent claims, expiry and buffered writes occur.
4. **Encrypted damaged tail prevents recovery and repair.** Both reopen and
   `check-log --repair` probes fail. The previous suggested fix was too broad:
   a decryption failure after a good record is not automatically a torn final
   append. Keep wrong-key, interior-corruption and sealed-segment checks; prove
   which tail can safely be discarded before truncating anything.
5. **Old-term publish retry acknowledged without current-term commitment.** The
   replication probe succeeds below the current-term commit barrier. The retry
   path needs the same commitment proof as a new publish.
6. **Partial index entry corrupts later reads after another restart.** The
   24-byte entries and 64 KiB write buffer can create a partial on-disk entry
   during ordinary buffering; this is not limited to a full disk. Align or
   rebuild the advisory index and verify reads against the WAL.
7. **Cross-topic/principal message-ID collision.** A publisher to beta can block
   an ID on alpha. Decide and enforce the namespace/idempotency contract before
   making a stable multi-tenant promise. Accidental cross-topic ID reuse can
   trigger this with trusted publishers too.
8. **Unbounded permanent consumer-group creation through Subscribe.** Eight
   subscriptions retain 16 MiB of names in the probe. Bound names and group
   creation/storage. Do not fix it by deleting durable consumer progress on
   every disconnect.
9. **Replay cancellation does not interrupt pacing sleep.** All ten cancelled
   handlers remain alive in the probe. This is a cancellation/resource bug for
   ordinary clients too, not exclusively a multi-tenant security issue.

Findings 1–6 and 10–13 should be addressed for the assumed supported scope.
Findings 7–9 should also be fixed before a broad stable release; a restricted
trusted-client deployment can reduce some exposure but does not remove these
bugs. CDC can instead be explicitly excluded and disabled in a narrower release,
but the current product documents it as supported.

## Validation performed

- Full pure-Go Linux suite:
  `CGO_ENABLED=0 go test -count=1 -json -timeout 300s ./internal/... ./pkg/... ./cmd/...`.
  **29 packages passed, 6 failed, 3 had no tests; 1,171 passing test/subtest
  records.** Every failed test was one of the previously supplied audit probes;
  no additional ordinary-suite test failed in this run. Evidence:
  `build/audit-1.0-linux-tests.jsonl`.
- The new SDK and CDC probes ran separately with `-tags releaseaudit`; all three
  top-level probes failed in the expected way. They are opt-in and do not add
  failures to ordinary test runs.
- One real three-process cluster backup/restore acceptance run passed:
  `go test -tags acceptance -count=1 -timeout 10m -v ./tests/acceptance -run TestClusterRestoredFromBackups`.
  It took 155.5 seconds. This does not establish that a previously reported
  intermittent failure is fixed, and does not cover the pending-CDC scenario.
  Evidence: `build/audit-1.0-restore-acceptance.log`.
- Dashboard: 21 tests in 6 files passed; production build passed. npm audit
  reported zero vulnerabilities in its response on this date.
- `scripts/check-production-chart.py` passed, as did all 8 publication-tooling
  unit tests. The additional disruption-budget assertion failed.
- `govulncheck` on the pure-Go server/client/command packages reported no
  called-symbol vulnerabilities. It reported two findings in required modules
  outside the imported/called code. This is a dated scan, not a zero-risk claim.
- Targeted Rust/cgo probes under `-race` completed across all eight selected
  packages. All nine earlier issue categories and all three new Go probes
  reproduced; **no data race or test timeout was reported**. These are failing
  correctness assertions, not a passing race suite. Evidence:
  `build/audit-1.0-race-probes.jsonl` and its stderr log.

An earlier native Windows suite run was stopped before its overall completion;
the complete suite result above is the Linux run. This review did not rebuild
or rescan the shipped container, run the entire acceptance campaign, perform a
live Kubernetes drain, or run real multi-host/power-loss tests.

## Release gates beyond fixing individual defects

These are proposed 1.0 acceptance requirements, not additional confirmed bugs.

1. **Data-conservation fault campaign.** Exercise 3-to-5 node growth, staged
   replacement, drains/removal and leader failures at every reconfiguration
   step. Track every acknowledged event through recovery and eventual delivery,
   with out-of-order ACKs, retries, duplicate detection maintenance and encrypted
   logs. Allow only the documented at-least-once duplicates, and no early delivery.
2. **Representative sustained load on separate hosts.** Establish supported
   payload sizes, event rates, backlog, p99 lateness, memory and disk envelopes.
   Include slow consumers, failing CDC, repeated restarts and disk pressure.
   A suggested starting gate is a 24–72 hour soak; duration alone is not proof.
   The current release docs explicitly lack multi-host/capacity evidence.
3. **Recovery and compatibility drills.** Restore pending CDC, scheduled work,
   consumer progress and DLQ state; refill a replica beyond the previously tested
   size; measure recovery point/time. Test v0.9-to-1.0 upgrade and supported
   rollback/format boundaries. Test machine/power-loss durability before claiming
   that guarantee; a killed process is a different failure.
4. **Bounded operation and honest stable contracts.** Choose tested limits and
   runbooks for pinned WAL segments, DLQ journal growth, group creation and
   overload. Automatic snapshot catch-up, manual rebalance and tenant quota
   configuration are documented gaps; they may remain excluded or constrained,
   but exposed APIs/flags must not imply unsupported behavior. Publish explicit
   topic/idempotency, ACK, replay, compatibility and supported-platform contracts.
5. **Release-candidate evidence.** Run required CI, race and pure-Go suites,
   complete acceptance/container/Helm tests, fresh dependency and image scans on
   the final candidate. Fix or explain intermittent failures without treating a
   lucky rerun as closure. Publish the tested digest and recovery documentation;
   close the documented signed-provenance gap or explicitly record that limitation.

No production files were changed by this review. The deliverables are this
assessment and the three opt-in probe files. The known-issue list is a lower
bound supported by the evidence above, not a proof that other defects do not
exist.

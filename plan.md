# CronosDB — Correctness & Reliability Remediation Plan

Status legend: `[ ]` todo · `[~]` in progress · `[x]` done · `[-]` won't-fix/deferred

This plan enumerates every **verified** defect found during the deep audit (cross-checked
against the code, not taken on faith). Each item lists the symptom, the root cause with
file:line anchors, the fix, and how it will be verified. Ordering follows blast radius:
**data-loss-on-normal-operation first**, then split-brain, then message-loss, then security,
then client/observability, then CI.

Architecture goals to preserve throughout: **reliability AND throughput**. Fixes must not
regress the hot path (single-partition fast path, group-commit, mmap writes). Where a fix
touches the hot path, it must be gated so RF=1 / no-encryption / no-cluster stays fast.

---

## PHASE 1 — Storage: data loss on the happy path (CRITICAL, do first)

### [ ] 1.1 — Restart corrupts the log via preallocated tail
- **Symptom:** First append after *any* restart (even clean shutdown) writes ~prealloc-EOF
  bytes past real data → zero gap → range reads stop at the gap → next restart regresses
  `lastOffset` → duplicate offsets.
- **Root cause:**
  - `scan()` mutates `s.sizeBytes = lastGoodPos` per record ([segment.go:1232](internal/storage/segment.go:1232)).
  - `truncateToPosition` inner guard `if pos >= s.sizeBytes { return nil }`
    ([segment.go:1265](internal/storage/segment.go:1265)) compares against the *already-mutated*
    `sizeBytes` → silently no-ops when the segment has ≥1 valid record (prints a false
    "Truncated N bytes" log).
  - `mmapWritePos` is only reset inside that dead truncate path ([segment.go:1301](internal/storage/segment.go:1301));
    `OpenSegment` leaves it at `stat.Size()` = prealloc EOF ([segment.go:213](internal/storage/segment.go:213)).
  - `Close()` never trims the prealloc tail ([segment.go:1401](internal/storage/segment.go:1401)).
- **Fix:**
  1. After `scan()` completes, set `s.mmapWritePos = s.sizeBytes` (real data end) in `OpenSegment`.
  2. Fix `truncateToPosition` to take the authoritative on-disk size, or have callers pass the
     true target; do not self-guard on mutated `sizeBytes`.
  3. On `Close()` (or rotate-to-inactive), truncate the file to `sizeBytes` so no prealloc tail
     persists past the last record. Must `munmap` before `Truncate` on Windows (pattern already
     in `truncateToPosition`).
- **Verify:** new test `TestSegment_AppendAfterReopenThenRead`: write N, close, reopen, append M,
  read [0, N+M) → all present, contiguous, no zero gap. Run on Windows + Linux.

### [ ] 1.2 — Historical reads break after segment rotation
- **Symptom:** After the first rotation, reading any offset in a rotated (now-closed) segment
  returns `os.ErrClosed`. Breaks Replay, follower catch-up (`Sync`), cross-region `FetchEvents`.
- **Root cause:** `rotateSegment` closes the old segment but keeps it in `w.segments`
  ([wal.go:951](internal/storage/wal.go:951)); `ReadEventsByOffsetRange` uses `s.segmentFile.ReadAt`
  on the closed handle ([segment.go:1085](internal/storage/segment.go:1085)).
- **Fix:** Do not fully `Close()` a rotated segment's read handle. Options (pick one):
  - Keep the segment file open **read-only** for reads; only flush+unmap the *write* path on
    rotation (preferred — zero read-path change).
  - Or reopen lazily on read with a refcount.
  Ensure the mmap/write resources are released but `reader`/`segmentFile` stays readable until
  the segment is deleted by retention.
- **Verify:** new test `TestWAL_ReadAfterRotation`: write enough to force ≥2 rotations, read from
  the earliest offset → success.

### [ ] 1.3 — AES-GCM nonce reuse (at-rest encryption broken)
- **Symptom:** Same (key, nonce) reused across every segment of a partition → GCM keystream reuse
  + auth-key recovery. At-rest encryption provides no confidentiality/integrity guarantee.
- **Root cause:** nonce = `partitionID(4B) || ciphertextPos-within-segment(8B)`
  ([crypto.go:92-97](internal/storage/crypto.go:92)). Positions restart per segment → collisions.
- **Fix:** Make the nonce globally unique per (key). Mix in segment identity: nonce =
  `partitionID(4B) || segmentFirstOffset ^ position` won't fit 8B safely — instead use a random
  12-byte nonce per record stored alongside the ciphertext (format already versioned via the
  1-byte header in `EncryptRecord`). Bump cipher-format version to 2; keep v1 decrypt for
  backward read. Random nonce removes the counter-collision class entirely.
- **Verify:** new test `TestCipher_NonceUniquenessAcrossSegments`: encrypt same plaintext at the
  same position in two segments → different ciphertext; decrypt round-trips both v1 and v2.

### [ ] 1.4 — Dedup store loses recent claims on crash
- **Symptom:** Pebble dedup opened `DisableWAL:true` + all writes `NoSync`; nothing replays the
  event WAL into dedup on boot → post-crash a recently-seen message ID is accepted as new.
- **Root cause:** [pebble_store.go:55](internal/dedup/pebble_store.go:55) (`DisableWAL`),
  `pebble.NoSync` at [pebble_store.go:402](internal/dedup/pebble_store.go:402) and batch commits;
  no WAL→dedup replay path invoked at startup.
- **Fix:** On partition open, after WAL recovery, re-drive dedup claims for the tail of the WAL
  (from the last durable dedup checkpoint to WAL head) so the dedup store is consistent with
  durable events. Keep NoSync for throughput but add a bounded replay-on-recovery. Alternatively
  periodically checkpoint the dedup high-water offset and replay from it.
- **Verify:** test: claim IDs, kill without sync, reopen, re-publish same IDs → detected as dup.

---

## PHASE 2 — Delivery: silent loss

### [ ] 2.1 — Scheduler clock skews into the future on every restart (compounds)
- **Symptom:** After restart, every timer with delay < previous-uptime fires immediately; the
  inflated tick is re-checkpointed → error compounds across restarts.
- **Root cause:** `recover()` restores `currentTick` ([scheduler.go:650](internal/scheduler/scheduler.go:650))
  but `startTimeMs` is re-sampled at boot; `currentAbsoluteMs = startTimeMs + currentTick*tickMs`
  ([timing_wheel.go:101](internal/scheduler/timing_wheel.go:101)).
- **Fix:** Persist `startTimeMs` in the checkpoint and restore it with the tick (so absolute time
  is continuous), OR rebase `currentTick` from wall clock on recovery:
  `currentTick = (now - startTimeMs) / tickMs`. Choose wall-clock rebase to avoid unbounded
  catch-up after long downtime; ensure matured timers are handled by 2.2.
- **Verify:** test: schedule timers, checkpoint, simulate downtime, recover → no spurious
  immediate fire; matured timers routed to 2.2 path.

### [ ] 2.2 — Events maturing during downtime are discarded
- **Symptom:** Timers whose fire-time passed while the node was down are counted `expiredCount`
  and dropped, never delivered.
- **Root cause:** `replayWALTimers` enqueues only `ScheduleTs > now`; else increments
  `expiredCount` ([manager.go:678-688](internal/partition/manager.go:678)).
- **Fix:** For `ScheduleTs <= now` during replay, **enqueue for immediate delivery** (fire-now)
  rather than discard. Preserve dedup/ordering. Add a metric `scheduler_matured_on_recovery_total`.
- **Verify:** test: schedule near-future timer, stop before fire, restart after fire-time →
  delivered once.

### [ ] 2.3 — DLQ never wired in production
- **Symptom:** Poison messages hit `d.dlq == nil → "dropping"` and vanish.
- **Root cause:** partitions build `delivery.NewDispatcher(cfg)` ([manager.go:243](internal/partition/manager.go:243));
  `NewDispatcherWithDLQ`/`SetDLQ` have no non-test callers; drop at
  [dispatcher.go:1048-1051](internal/delivery/dispatcher.go:1048).
- **Fix:** Construct a `DeadLetterQueue` per partition (segment-backed) and `SetDLQ` on the
  dispatcher during partition creation. Surface DLQ size metric (already `stats.DLQSize`).
- **Verify:** test: force max-retry failure → message lands in DLQ, not dropped.

### [ ] 2.4 — gRPC 2PC flow runs with PM == nil (durable no-op)
- **Symptom:** Transactions started via `TransactionService.BeginTransaction` build participants
  with `PM` unset → Prepare/Commit write no WAL markers.
- **Root cause:** `tx.Handler.BeginTransaction` → `PartitionParticipant{PartitionID: pid}` with PM
  unset ([transaction_handler.go:45](internal/tx/transaction_handler.go:45)); `main.go` never calls
  `coordinator.SetPartitionManager`. (The coordinator *does* wire PM when it's called —
  [coordinator.go:231](internal/tx/coordinator.go:231) — it just isn't from the server path.)
- **Fix:** Have `tx.NewHandler`/coordinator construction receive the `PartitionManager` and set it
  on the coordinator so all participants (including newly-begun ones) carry PM. Ensure
  `PartitionParticipant` is built with PM in `BeginTransaction`.
- **Verify:** test: begin→prepare→commit via handler → WAL contains tx markers.

### [~] 2.5 — Backpressure drops events with no requeue / no resume-from-committed
- **DONE:** resume-from-committed is already implemented (subscribe seeds NextOffset
  from the committed offset, handlers.go:859); backpressure skips (no-credit and
  in-flight-cap, both single and batch paths) now increment
  `cronos_dispatcher_backpressure_skips_total{partition,reason}` instead of being
  silent. The "acks commit on nack" claim was disproven (no change needed).
- **DEFERRED (follow-up):** true requeue / worker-level flow-control so a
  credit-exhausted event is re-driven from the WAL rather than relying on consumer
  reconnect. This is an architectural change to the push-based delivery model and
  is intentionally out of scope for this pass to avoid destabilizing the hot path.

<!-- original item retained below for detail -->
#### 2.5 detail
- **Symptom:** Zero credits / in-flight cap → event skipped after dequeue, no requeue, no metric.
  Disconnect abandons in-flight; `NextOffset` written but never re-read to resume.
- **Root cause:** `continue` on no-credit ([dispatcher.go:647-649](internal/delivery/dispatcher.go:647));
  in-flight cap path ([dispatcher.go:507-511](internal/delivery/dispatcher.go:507)); no
  resume-from-`NextOffset` on (re)subscribe.
- **Fix:** On credit exhaustion / in-flight cap, do not drop: leave the event un-acked so the
  worker re-drives it (or requeue), and emit `dispatcher_backpressure_skips_total`. On subscribe,
  seed the read cursor from the committed offset so a reconnect resumes rather than skips.
  Keep the fast path allocation-free.
- **Note:** "acks commit on nack" from the audit was **disproven** — `HandleAck` only advances
  `NextOffset` under `if success` ([dispatcher.go:1003](internal/delivery/dispatcher.go:1003)); no fix needed there.
- **Verify:** test: saturate credits → events redelivered after credit release, none lost.

---

## PHASE 3 — Cluster: split-brain & unclean election

### [ ] 3.1 — Leader object never learns its real epoch (fencing advertises term 1)
- **Symptom:** `PromoteToLeader` sets `partition.Epoch` but not the `Leader` object's epoch, so
  the leader's `Replicate` stamps `Term` from `l.epoch` which stays 1 → genuine stale leader and
  new leader both speak term 1; follower's stale-term gate can't distinguish them.
- **Root cause:** `replication.NewLeader` hardcodes `epoch: 1` ([leader.go:90](internal/replication/leader.go:90));
  `PromoteToLeader` ([manager.go:1079,1087](internal/partition/manager.go:1079)) sets `partition.Epoch`
  but never calls `leader.SetEpoch(epoch)`.
- **Fix:** In `PromoteToLeader`, call `leader.SetEpoch(epoch)` on both the create-new and
  already-leader branches. Confirm `Replicate` reads the updated epoch for `Term`.
- **Verify:** test: promote to epoch 5 → outgoing Append carries `Term=5`; follower at epoch 5
  rejects an Append with `Term=1`.

### [ ] 3.2 — FSM drops ReplicaOffsets → unclean failover election
- **Symptom:** After any Raft `applyUpdatePartition`, `ReplicaOffsets` is nil → `electNewLeader`
  sees `bestOffset=0` for all candidates → picks alphabetically-first node → can lose acked writes.
- **Root cause:** `applyUpdatePartition` copies LeaderID/Replicas/ISR/State but omits
  `ReplicaOffsets` ([raft.go:417-422](internal/cluster/raft.go:417)); election relies on it
  ([election.go:44-55](internal/cluster/election.go:44)).
- **Fix:** Preserve/merge `ReplicaOffsets` in `applyUpdatePartition` (and `applyAssignPartition`).
  Default election to **refuse unclean election** when offset data is absent, unless an explicit
  `--allow-unclean-election` flag is set.
- **Verify:** test: update partition via FSM → offsets retained; election picks highest-offset replica.

### [ ] 3.3 — Follower ahead of a new leader can never realign (permanent divergence)
- **Symptom:** After failover, a follower whose tail is *ahead* of the new leader rejects every
  Append with "replicated batch gap" forever.
- **Root cause:** `AppendReplicatedBatch` hard-rejects non-contiguous batches
  ([wal.go:657-660](internal/storage/wal.go:657)); no truncate-and-realign RPC. `Snapshot` covers
  *far-behind* only ([replication_server.go:182](internal/api/replication_server.go:182)).
- **Fix:** Add a follower-truncation step: when a leader's Append indicates the follower is ahead
  (leader's `NextOffset` < follower's), the follower truncates its uncommitted tail back to the
  leader's high-water (respecting epoch fencing so only a higher-term leader can force truncation)
  then resumes. Guard with epoch so a stale leader cannot truncate a follower.
- **Verify:** chaos test: diverge a follower ahead, elect new leader, assert realign to lag=0.

### [ ] 3.4 — Membership events silently dropped when channel full (ring divergence)
- **Symptom:** Under churn, `emitEvent` drops events when the 100-slot channel is full with no
  resync → divergent rings → two nodes lead the same partition (compounds 3.1).
- **Root cause:** `default: // Channel full, skip event` ([membership.go:570-572](internal/cluster/membership.go:570)).
- **Fix:** On a would-drop, set a `needsResync` flag and trigger a full membership state pull on
  the next tick (or block briefly with a bounded timeout). Never silently lose a membership delta.
- **Verify:** test: flood events → ring converges after resync; no permanent divergence.

### [ ] 3.5 — Leadership is gossip-derived, not Raft-committed
- **Symptom:** `reconcileLocalLeadership` self-promotes from the local router ring every 5s,
  ungated by Raft-committed leadership → two nodes can both promote.
- **Root cause:** [cluster/manager.go:279-319](internal/cluster/manager.go:279) promotes whenever
  `info.LeaderID == nodeID` from the (gossip-fed) router, not from Raft FSM state.
- **Fix:** Gate promotion on Raft-committed partition assignment (read leadership from the FSM /
  raft-applied state, not the gossip ring). Keep the 5s reconcile but source truth from Raft.
  With 3.1 fencing correct, a brief double-promote becomes safe (higher term wins), but the
  source-of-truth fix removes the race.
- **Verify:** chaos test: partition the gossip layer → no two nodes hold the same partition as
  Raft-leader simultaneously.

### [ ] 3.6 — Cross-region replication lossy + echo loop
- **Symptom:** Batch buffer cleared before send; send failure = silent permanent loss. Received
  events re-fire the append hook → re-replicate back out (echo).
- **Root cause:** buffer cleared pre-send + only `slog.Warn` on failure
  ([region.go:158,168-172](internal/replication/region.go:158)); `CrossRegionServer.ReplicateEvents`
  → `AppendBatch` → append hook → `ReplicateAsync` again ([main.go:281-283](cmd/api/main.go:281)),
  no origin tag.
- **Fix:** (a) Do not clear the batch until the remote acks; on failure retry with backoff and a
  bounded persistent outbox/checkpoint. (b) Tag events with origin region (or set a context flag
  on replicated appends) so the append hook skips re-replicating foreign-origin events.
- **Verify:** test: two-region loop → an event replicated in does not bounce back out; dropped
  send is retried, not lost.

---

## PHASE 4 — Security / API surface

### [ ] 4.1 — Per-method authorization missing on ~22 of 25 RPCs
- **Symptom:** Only Publish/PublishBatch/Subscribe check topic authz. `Replay`, `Ack`,
  `Compact`, `RunRetention`, `SplitPartition`, consumer-group mutation, all of TransactionService
  reachable by any authenticated token.
- **Root cause:** `checkTopicAuth` called only at [handlers.go:258,533,828](internal/api/handlers.go:258);
  [partition_handler.go](internal/api/partition_handler.go) has zero auth calls.
- **Fix:** Add authz to every mutating/destructive RPC: topic-permission for Replay/Ack (read/own
  offset), admin-permission for Compact/RunRetention/SplitPartition and consumer-group admin ops,
  topic-permission for TransactionService participants. Centralize via a small helper.
- **Verify:** table test per RPC: non-authorized subject → PermissionDenied.

### [ ] 4.2 — CrossRegionService exposed on the public client listener
- **Symptom:** Any client reaching the client port can inject/read arbitrary partition data,
  bypassing schema/tenant/dedup/fencing.
- **Root cause:** registered on the public server ([main.go:411-412](cmd/api/main.go:411)).
  (ReplicationService/RaftService already moved to the internal listener — this didn't follow.)
- **Fix:** Move `CrossRegionService` registration to the internal gRPC listener
  ([main.go:414-438](cmd/api/main.go:414)) with replication mTLS; require admin/peer auth.
- **Verify:** cross-region RPC on the public port → Unimplemented/unauthenticated; works on internal.

### [ ] 4.3 — Default allow-all policy when no policy file supplied
- **Symptom:** `--auth-enabled` without a policy file = every authenticated subject is effectively
  admin on every topic.
- **Root cause:** `authConfig.Policy = auth.AllowAllPolicy()` default ([main.go:306](cmd/api/main.go:306));
  config validation does not require a policy file.
- **Fix:** In production (auth enabled, non-dev), require `--auth-policy-file`; fail fast if
  missing. Keep AllowAll only under `--dev`.
- **Verify:** start with auth enabled + no policy + non-dev → refuses to boot.

### [ ] 4.4 — Admin/HTTP plane: plaintext + no timeouts
- **Symptom:** `/metrics` + admin dashboard over plain HTTP, Bearer tokens in cleartext,
  Slowloris-able (no read/write timeouts).
- **Root cause:** bare `&http.Server{Addr, Handler}` ([main.go:510-513](cmd/api/main.go:510)).
- **Fix:** Set `ReadHeaderTimeout`, `ReadTimeout`, `WriteTimeout`, `IdleTimeout`. Support optional
  TLS for the admin/health server (reuse cert config). (Admin handler already checks
  `CheckAdminPermission` — [web_handler.go:151](internal/api/web_handler.go:151).)
- **Verify:** server has non-zero timeouts; slow-header client is cut off.

---

## PHASE 5 — Client SDK correctness

### [ ] 5.1 — Circuit breaker "disabled" default opens on first failure
- **Symptom:** Default `FailureThreshold: 0` → `failures.Add(1) >= 0` trips immediately; comment
  claims "disabled by default".
- **Root cause:** [producer.go:107](pkg/client/producer.go:107) + trip logic
  [breaker.go:109](pkg/client/internal/circuitbreaker/breaker.go:109).
- **Fix:** Treat `FailureThreshold <= 0` as **disabled** (never trips) in `RecordFailure`; or set a
  sane non-zero default. Keep semantics explicit.
- **Verify:** test: threshold 0 → breaker never opens after N failures.

### [ ] 5.2 — Hedging returns first-completed, not first-successful
- **Symptom:** A fast failure beats a slow success; caller sees the error.
- **Root cause:** `res := <-results` takes the first result regardless of error
  ([hedging.go:83](pkg/client/internal/hedging/hedging.go:83)).
- **Fix:** Loop receiving results; return the first success; if all fail, return the last error.
  Cancel losers on first success.
- **Verify:** test: hedge where request A fails fast, B succeeds slow → returns B's success.

### [ ] 5.3 — Capability probes always report true
- **Symptom:** `Subscribe`/`Ack` capabilities set true merely because opening a bidi stream didn't
  immediately error (opening never round-trips).
- **Root cause:** [compatibility.go:86-99](pkg/client/compatibility.go:86).
- **Fix:** Probe with a real round-trip (send a benign frame / check `Unimplemented` via a header
  exchange) before declaring support, or drop the false-positive probes.
- **Verify:** test against a server without Subscribe → capability reports false.

### [ ] 5.4 — SendAsync/Close TOCTOU panic race (harden)
- **Symptom:** Narrow window: `closed.Load()` passes, `Close` closes `p.queue`, send panics.
- **Root cause:** check at [producer.go:504](pkg/client/producer.go:504) vs `close(p.queue)` at
  [producer.go:560](pkg/client/producer.go:560).
- **Fix:** Guard the send with `recover`, or use an RWMutex (RLock around enqueue, Lock around
  close), or a sentinel that makes send-after-close return an error instead of panicking.
- **Verify:** stress test: concurrent SendAsync + Close → no panic, clean error returned.

---

## PHASE 6 — Observability & CI

### [ ] 6.1 — Dead/duplicate replication-lag metrics + broken alert
- **Symptom:** `cronos_replication_lag_seconds` + `SetReplicationMetrics` have zero callers (dead);
  `cronos_wal_high_watermark` isn't a registered Prometheus gauge → Helm `ReplicationLagHigh`
  alert's denominator is empty → never fires.
- **Root cause:** [metrics.go:268](internal/metrics/metrics.go:268) (no callers);
  `cronos_wal_high_watermark` only in `/health` JSON ([health.go:282](internal/api/health.go:282)),
  not a promauto gauge; alert at
  [prometheusrule.yaml:20-31](charts/cronos-db/templates/prometheusrule.yaml:20).
- **Fix:** Either wire `SetReplicationMetrics` from the leader loop or delete the dead gauge and
  standardize on `cronos_replication_lag` (events). Register `cronos_wal_high_watermark` as a real
  gauge (we already compute it) or rewrite the alert to use existing series. Fix the 3 alert rules
  to reference real metrics.
- **Verify:** `/metrics` exposes the referenced series; alert expression evaluates non-empty.

### [ ] 6.2 — Restore CI with a chaos gate
- **Symptom:** No CI (`.github/workflows` deleted in `ecf8668`); regressions like the above go
  unnoticed. `--dev` skips metrics/SLO interceptors and all compose nodes run `--dev`.
- **Fix:** Add a CI workflow: build (cgo/MinGW note), `make test-unit`, `go vet`, gofmt check, and
  a `tests/chaos` gate covering Phase 1–3 regression cases (append-after-reopen, read-after-
  rotation, kill-leader failover to caught-up replica, fail-closed below minISR). Provide a
  non-`--dev` profile so metrics are exercised.
- **Verify:** CI is green on a clean checkout and red if any Phase 1–3 fix is reverted.

---

## NEW issues found during verification (not in the original audit)

### [ ] N1 — `truncateToPosition` inner guard is a latent no-op for all future callers
- Folded into **1.1** (fix the guard at the source).

### [ ] N2 — Snapshot install has no key negotiation for encrypted segments
- **Symptom:** `buildSnapshotFileList` streams raw segment bytes ([replication_server.go:289](internal/api/replication_server.go:289));
  a follower with a different per-partition key can't decrypt installed segments.
- **Fix:** Either replicate the partition key material over the mТLS internal channel during
  snapshot install, or transcode on the leader to the follower's key, or forbid raw-file snapshot
  when encryption is on and fall back to event-level `Sync`. Decide during Phase 3.3.
- **Verify:** encrypted snapshot install → follower reads decrypt correctly.

### [ ] N3 — Lazy partition creation races epoch stamping
- **Symptom:** In cluster mode only partition 0 is eager ([main.go:244-245](cmd/api/main.go:244));
  the 5s reconcile and a concurrent first write both materialize a partition; a write can briefly
  see `Epoch=0` before reconcile stamps the real epoch.
- **Fix:** When materializing a partition on the write path in cluster mode, fetch and stamp the
  Raft-known epoch atomically (don't leave it 0 until reconcile). Folds into 3.1/3.5.
- **Verify:** race test: concurrent first-write + reconcile → write never observes epoch 0.

---

## Verification harness (applies to all phases)
- Unit/targeted: `make test-unit` (Windows needs the `GO_RUNTIME_PREFIX` PATH fix already present).
- Each Phase-1/2 bug gets a **reproduction test that fails before the fix** and passes after.
- Distributed items: live 3-node RF=3/minISR=2 (run binaries directly on Windows) + loadtest +
  `curl :8080/metrics | grep replication_lag` (expect 0) + kill-node phases.
- No hot-path throughput regression: re-run `make loadtest-max` (RF=1) and the durable RF=3
  benchmark; compare against the ~790K / ~200K baselines.

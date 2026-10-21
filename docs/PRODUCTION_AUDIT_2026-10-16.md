**CronosDB production hardening audit — 2026-10-16**

Audited `developement` at `f8cc2063534be4aa97ffac602d9297b15d636e56`. The working tree was clean when the audit began. `git diff v0.6.0 HEAD -- internal pkg cmd` is empty: the audited runtime source matches the local v0.6.0 tag. This does not verify the provenance of a published container or binary.

The next work should close correctness and bounded-resource gaps before adding features. Much of the prior hardening is present: durable batch fsync, quorum completion, epoch fencing, log matching, restart/redrive, consumer-progress replication, authenticated internal traffic, and CI fault/restore/overload campaigns. They should be preserved. The findings below distinguish reproduced defects, code-traced limits, and missing validation; the September audit's historical defects are not being treated as still open without checking the current code.

**Status of the findings**

The runtime fixes below shipped in v0.7.0 and remain in v0.8.0. Closure notes
were reviewed on 2026-10-21; the original findings and measurements later in
this document describe the audited v0.6.0 source.

| Finding | Status |
|---|---|
| 1. Unreadable records | Fixed. One record limit for writers, readers and recovery; publishes check it before claiming an ID. |
| 2. Producer-chosen terms | Fixed. A log stores its own term for every entry it appends for itself; followers store the leader's. |
| 3. Append errors and dedup | Fixed. Appends report whether their events reached the log, and publishes hold those for the retry instead of releasing the ID. A failed write or fsync stops the log and the node; a failed rotation no longer fails the append. |
| 4. Dead-letter copy not replicated | Implemented. Bounded journal pages include payloads and tombstones; followers fsync before acknowledging. Completion transfer and retention wait for the copy. |
| 5. Whole-segment startup verification | Fixed. The second, whole-segment read was removed; opening a segment already checks every record one at a time. |
| 6. Scheduler memory | Implemented. A shared node payload budget spills overflow to WAL references; hydration is paged and can evict later timers for earlier work. |
| 7. Dedup recovery cutoff | Implemented. Durable checkpoints replace the count cutoff; startup and promotion fail closed on recovery errors. |
| 8. Pinned log and disk admission | Implemented. Disk admission covers new follower appends; retries of already stored entries remain possible. Retention exports the blocking offset, age and reason. An optional separate backup PVC is available. Retention remains conservative; unfinished events are not discarded to reclaim space. |
| 9. Sparse completion transfer | Implemented. Per-follower cursors send 4,096 completions per page, across all groups, with versioned retry rounds. |
| 10. Client discovery of new nodes | Implemented. Metadata advertises public endpoints; clients refresh routing and preserve explicit overrides and mixed-version discoveries. |
| 11. Quota refusal strands an ID | Implemented. Both publish paths release refused claims, charge after deduplication, and refund reservations on definite pre-append failure. |
| 12. Release evidence | Workflow implemented. The release image is built once, scanned, tested, pushed and attested by digest. Publishing requires repository credentials and a successful CI run. The repository had no Docker Hub credentials at release, so the `v0.7.0` image was built, scanned and pushed by hand, without provenance. |

The probes of findings 1, 2, 3 and 11 are now ordinary tests ([internal/api/publish_integrity_test.go](../internal/api/publish_integrity_test.go), [internal/storage/append_outcome_test.go](../internal/storage/append_outcome_test.go)). The probe of finding 3 changed with the fix: the publish that fills a segment now succeeds, and the test checks that its retry does not append the event again.

The second batch implementation and upgrade behavior are described in
[v0.7.0 hardening](HARDENING_V0.7.0.md). v0.8.0 additionally fixes old-term
commitment after capability agreement and adds opt-in lifecycle mutations,
bounded relocation and consistent recovery snapshots. These do not close all
of finding 12: the image was published locally from the CI-tested archive
without signed workflow provenance; multi-host validation, real power loss
and release-specific capacity remain unproven. Lifecycle v1 excludes CDC and
experimental features; relocation cannot discard unfinished due work. See
[v0.8.0 notes](RELEASE_V0.8.0.md) and
[remaining limits](PRODUCTION_RELEASE.md#what-v080-was-released-without).

The checks below describe the original audit, not a new run against v0.8.0.

**What was checked**

- Followed the public publish handlers through dedup, WAL append, replication, scheduling, and retry; reviewed completion/DLQ transfer, recovery, retention, client discovery, and release workflows.
- `GOARCH=amd64 CGO_ENABLED=0 go test -count=1 ./internal/... ./pkg/... ./cmd/... -timeout 300s`: passed on Windows with Go 1.26.9.
- `GOARCH=amd64 CGO_ENABLED=0 go vet ./internal/... ./pkg/... ./cmd/...`: passed.
- Six isolated audit test functions, with nine leaf cases, failed on the expected invariant assertions and reproduce five distinct defects. [Runner and scope](audit-probes/2026-10-16/README.md), [probe source](audit-probes/2026-10-16/api.go.txt), [captured output](audit-probes/2026-10-16/evidence.txt).
- No runtime source was changed. Probes are installed through a Go overlay and do not make ordinary CI fail.

This session did not run Linux/cgo/race, the tagged acceptance campaigns, a new dependency scan, a production load benchmark, or a real power-loss test. Existing CI definitions and archived measurements are evidence of coverage and prior work, not new executions. Expected performance costs below are engineering estimates, not measured results.

**1. Critical — a successful publish can create an unreadable WAL record. Reproduced.**

Both public publish methods restrict payloads to 4 MiB but do not restrict total encoded event size. The public gRPC limit is 16 MiB. `PrepareRecord` checks field widths and the uint32 record length, while WAL readers and recovery reject records larger than 10 MiB. A small payload plus 180 metadata values of 60 KiB passes admission and encoding, is acknowledged, and cannot immediately be read back at its assigned offset. The reproduced event is 11,062,302 protobuf bytes, below the transport limit. Both single and batch requests exhibit this.

On recovery, the oversized record is classified as an invalid tail. Depending on whether populated segments follow, that can discard the tail or refuse to reopen the partition. This recovery consequence follows from the code; the recorded probe demonstrates immediate unreadability, not a cluster crash.

Evidence: [public validation](../internal/api/handlers.go#L402), [batch validation](../internal/api/handlers.go#L762), [transport limit](../internal/api/grpc_server.go#L78), [record size calculation](../internal/storage/wal.go#L420), [read limit](../internal/storage/segment.go#L936), [recovery limit](../internal/storage/segment.go#L1336), [tail handling](../internal/storage/wal.go#L282).

Fix: establish one maximum physical record size shared by encoder, reader, recovery, and replication. Validate before reserving offsets or claiming IDs, including metadata and encryption overhead. Bound metadata count/key/value sizes as well. Test just below/at/above the boundary, encrypted and plaintext, single and batch, and reopen all accepted records.

Expected cost: a size check in the metadata traversal already performed by encoding; no additional disk sync or network round trip. Rejecting formerly accepted unreadable inputs is an intentional compatibility change.

**2. High — public producers can choose the term stored in the replicated log. Reproduced locally and on a follower.**

`Event.Term` is part of the public event object. The public handlers pass it through; `termForEvent` and `PrepareRecord` prefer a nonzero supplied term over the local WAL term. The probes store term 999999 while the local current term is 7, and separately propagate term 999999 through real follower gRPC replication while the leader epoch is 1.

Terms are protocol authority, not application data. Election ordering prefers a higher last-entry term before comparing offsets, and log matching/truncation assumes terms reflect actual leadership. Thus a producer can violate a prerequisite for recovery correctness. The probes prove the authority violation; they do not demonstrate acknowledged-data loss in a full election campaign.

Evidence: [term selection](../internal/storage/wal.go#L403), [termForEvent](../internal/storage/wal.go#L522), [election comparison](../internal/cluster/leadership.go#L336), [follower log matching](../internal/api/replication_server.go#L139).

Fix: reject or overwrite server-owned fields at public ingress, and make local append derive its term from the current fenced leader. Preserve historical terms only in the explicitly internal replication path. Test forged positive/negative terms, leadership changes, and a retry using an event object previously returned to a caller.

Expected cost: constant-time field normalization; no new I/O. Correct epoch assignment must stay within the existing leadership/append synchronization boundary.

**3. High — append errors are treated as if nothing reached the log. Reproduced with rotation failure.**

The WAL can append an event, advance its watermark, and then return an error from flush, fsync, or rotation. Both public handlers unconditionally roll back the dedup claim on any append error. The probe makes the next segment filename unavailable, gets the expected rotation failure, removes that obstacle, and retries the same message ID. The retry succeeds with two entries for that ID. Both public methods exhibit this.

This is distinct from the already repaired replication-timeout retry path. At-least-once delivery allows repeated delivery attempts; it does not make the advertised publish deduplication correct when a failed operation is appended twice.

Evidence: [append mutation and late errors](../internal/storage/wal.go#L810), [batch path](../internal/storage/wal.go#L550), [single rollback](../internal/api/handlers.go#L614), [batch rollback](../internal/api/handlers.go#L1037), [existing held-publish mechanism](../internal/partition/unaccepted.go#L43).

Fix: return a structured append outcome distinguishing no mutation, appended range, durability established, and indeterminate/partial failure. Release an ID only for a definite no-append result. Reconcile an appended operation through its original offsets; fence an indeterminate writer until its prefix is established. Review follower retries too: an existing matching record alone must not stand in for a durable sync after a failed append. Test rotation, short write, ENOSPC, and fsync failure, followed by retry and restart.

Expected cost: outcome bookkeeping on success and additional work on failure. Keep batched/group durability; this does not require a second fsync on every healthy publish.

**4. High — completed dead-letter work is replicated without its DLQ copy. Reproduced at the disposition/progress boundary.**

`sendToDLQ` durably writes the local DLQ and invokes `OnDeadLettered`, which records ordinary consumer completion. Consumer progress is replicated, but that protocol contains no DLQ entry or dead-letter disposition. The probe follows this actual persistence order and invokes the replication handler: the follower reports the event completed while its DLQ is empty.

If the leader's disk is then lost, the replacement has neither an accessible DLQ copy nor an automatic redelivery obligation for that group. The original event may initially remain in its WAL, but completion makes it eligible for retention; keeping the event temporarily is not a recoverable DLQ disposition. A temporary leader failure with its disk intact is a weaker scenario: the copy still exists on that old disk.

Evidence: [DLQ transition](../internal/delivery/dispatcher.go#L1276), [completion hook](../internal/partition/manager.go#L482), [progress export](../internal/consumer/completion.go#L603), [progress installation](../internal/api/replication_server.go#L311), [retention predicate](../internal/consumer/completion.go#L363).

Fix: replicate a durable dead-letter disposition and its recoverable payload/reference before completion allows the source event to be pruned. Include DLQ retry/removal state in that ownership model. Gate this with leader-volume loss after dead-lettering, follower promotion, retention, and subsequent DLQ inspection/redrive.

Expected cost: quorum work on the dead-letter transition, which can be batched; ordinary successful publishing need not gain an additional round trip.

**5. High — startup verification still materializes a whole segment. Code trace.**

`loadSegments` invokes `verifySegment`; that calls the unlimited `ReadEventsByOffsetRange` over the whole segment and retains the returned event slice. Parsing allocates each payload. Consequently startup needs memory proportional to a populated segment, before the later byte-bounded timer/dedup replay loops help. Default segments are 512 MiB. Both backlog restart tests explicitly use 8 MiB segments, so their successful 384 MiB container scenario does not cover restarting a large default segment under that limit.

Evidence: [load and verification](../internal/storage/wal.go#L235), [whole-segment read](../internal/storage/wal.go#L328), [unlimited range wrapper](../internal/storage/segment.go#L1098), [payload allocation](../internal/storage/segment.go#L700), [default segment size](../internal/config/defaults.go#L21), [test segment sizes](../tests/acceptance/restart_backlog_test.go#L190).

Fix: validate incrementally with a streaming scanner or bounded chunks, checking record CRC and continuity without retaining all payloads. Avoid duplicating the startup scan where possible. Add a restart test whose single populated segment exceeds the container's heap allowance, with production encryption settings. No OOM was induced in this session.

Expected cost: no publish-path cost; lower recovery memory, and potentially less repeated decode work.

**6. High — scheduling can exceed the memory budget without any new publish. Code trace; extends a documented limit.**

Hot timers retain complete events. The cold hydrator obtains every matching offset for its time window, reads each payload, and inserts all of them into the wheel/ready queue. It has no byte budget or bounded page. Publish admission checks do not govern hydration or timer recovery. Many far-future events can therefore be accepted while cheap, then enter the hot window together and exhaust memory even after publishers have stopped. Restart also schedules every future event in the hot window. Count limits and sampled RSS rejection are not reservations against these allocations; concurrent requests can pass the same count check.

Evidence: [hot scheduling](../internal/scheduler/scheduler.go#L203), [unbounded cold scan](../internal/scheduler/cold_store.go#L92), [hydration](../internal/scheduler/scheduler.go#L582), [recovery](../internal/partition/manager.go#L1020), [admission](../internal/partition/manager.go#L705). The worker already has a [64 MiB cap per partition](../internal/delivery/worker.go#L38); that is useful but is neither a node-wide cap nor a cap on the timing wheel.

Fix: add a node-wide retained-byte budget with explicit reservations and per-partition fairness. Keep offset/schedule references for overflow, page cold-store reads, and hydrate only within the reserved budget. Consider replacing payload-bearing hot timers with references plus a bounded payload cache; benchmark the extra WAL reads and due-event latency before selecting that representation. Test synchronized due times, no consumers, large metadata, concurrent ingress, and promotion/restart under a cgroup limit.

Expected cost: cheap batched accounting can have low overhead; reference-based scheduling trades retained memory for reads. This is a real design tradeoff that requires measurement, not a promised zero-cost fix.

**7. High — dedup recovery has an event-count cutoff, not a durable recovery boundary. Code trace; known open limit.**

The dedup Pebble database disables its WAL, uses 64 MiB memtables, and recovery scans only the last 500,000 log entries. The cutoff is justified by an assumption that older IDs were flushed or expired, not a persisted frontier proving it. Follower append does not maintain the producer's dedup store; a promoted follower relies on this bounded reconstruction too. Retained, still-in-window IDs before the cutoff can be absent from the promoted node's dedup state. A larger configured TTL does not repair that.

Evidence: [dedup storage options](../internal/dedup/pebble_store.go#L52), [cutoff and recovery](../internal/partition/manager.go#L924), [promotion](../internal/partition/manager.go#L1559), [follower append](../internal/api/replication_server.go#L167). The production values configure a [168-hour TTL](../charts/cronos-db/values-production.yaml#L29).

Fix: persist a crash-consistent dedup checkpoint frontier and replay the required suffix, or maintain dedup state on replicas in bounded background batches. Define how the promised ID window survives WAL pruning and node replacement; rebuilding a retained tail cannot reconstruct already pruned IDs. Fail readiness or disable writes if required recovery fails, instead of logging and continuing with incomplete dedup state. Test over 500,000 IDs, a fresh follower promotion, crash before a dedup flush, pruning, and retries within the promised window.

Expected cost: amortized checkpoint/replica maintenance, not necessarily per-event dedup fsync. Benchmark recovery time and write amplification separately.

**8. High for sustained operation — one unfinished event can pin the replicated log; disk pressure does not reject ingress. Code trace; known retention limit.**

Replicated pruning deliberately removes only a contiguous prefix. One far-future event, a group that never completes, or a stalled change feed can retain every later segment. `DiskMonitor` requests compaction at its threshold, but `CanAcceptBatch` has no disk-headroom gate. Compaction cannot free a pinned prefix, so continued traffic can reach disk-write errors precisely where finding 3 is unsafe.

Evidence: [prefix-only rationale and enforcement](../internal/partition/retention.go#L25), [disk monitor](../internal/partition/disk_pressure.go#L48), [admission checks](../internal/partition/manager.go#L705), [completion eligibility](../internal/consumer/completion.go#L363).

Fix first: cached disk-headroom admission with hysteresis and emergency reserved space for metadata/recovery, plus oldest blocking offset/reason/age metrics. Keep draining existing work when ingress is refused. Establish schedule-horizon and inactive-group policies. Separating long-horizon scheduling from the delivery log is a later storage/protocol change, not permission to delete arbitrary middle segments: election and replication currently depend on a contiguous log.

Expected cost: periodic filesystem measurement and a cheap cached admission check. No filesystem stat per event is needed.

**9. Medium — completion replication truncates sparse progress. Code trace; known limit.**

Only the first 100,000 completed offsets above a group's floor are exported. In a partition shared by topics, or with one old delayed offset, the floor can remain pinned while completions accumulate beyond that limit. Exporting the same prefix again is not pagination, so the omitted completions need not reach a follower even after the system has been idle. Failover can cause a large replay of completed work, beyond the normal short maintenance-tick duplicate window.

Evidence: [cap and exporter](../internal/consumer/completion.go#L596), [full-state shipping](../internal/replication/leader.go#L1113).

Fix: versioned, bounded progress chunks with per-follower continuation, or compact intervals/bitmaps plus topic-aware safe cursor advancement. Preserve the rule that an unrelated topic's offset is not an acknowledged event. Test more than 100,000 sparse completions, mixed topics, an old future event, and failover after all progress has supposedly converged.

Expected cost: background replication; compact deltas can reduce repeated scanning, bytes, and lock occupancy compared with repeatedly exporting full lists.

**10. Medium — connected clients cannot discover newly added node addresses. Code trace; documented operational restriction.**

Metadata refresh gets leader IDs but resolves addresses from a static map or IDs that already look like addresses. Candidate fallbacks are the existing connection pool. Leadership can move to a new node that none of those addresses names, leaving existing producers and consumers unable to route there.

Evidence: [metadata refresh](../pkg/client/internal/metadata/manager.go#L100), [candidate addresses](../pkg/client/internal/metadata/manager.go#L225), [release restriction](PRODUCTION_RELEASE.md#configuration-and-installation).

Fix: return authenticated, advertised client endpoints with versioned topology metadata; update the pool and retire obsolete endpoints safely. Test adding a node and moving leadership while original clients remain connected, including certificate SAN validation. Preserve the existing static mapping as a compatibility fallback.

Expected cost: metadata-refresh/control-path work; negligible expected steady-state publish cost.

**11. Medium, currently latent in the shipped entry point — quota refusal can strand a message ID. Reproduced.**

Single publish claims the ID before `ReservePublishBatch`. A quota refusal returns without releasing the claim. Raising the quota then leaves the retry rejected as an earlier publish still in progress, with WAL next-offset still zero. Batch publish performs quota reservation before dedup instead, so the ordering is inconsistent; reservation accounting on later failures/duplicates also deserves coverage.

Evidence: [single publish ordering](../internal/api/handlers.go#L517), [quota refusal](../internal/api/handlers.go#L572), [batch ordering](../internal/api/handlers.go#L839).

Qualification: the production entry point constructs an accountant, but a repository search finds no non-test caller of `SetLimits`. This is a defect when quota configuration is exercised, not a claim that the default server is currently rejecting users this way.

Fix: one reservation/claim lifecycle for both APIs; undo a claim on every pre-append rejection and reconcile quota reservations with actual accepted work. Test quota rejection/retry, duplicate-only batches, validation failure, and append failures. Expected throughput cost is bookkeeping and can be negligible when quotas are disabled.

**12. Release and capacity evidence remains incomplete. Confirmed workflow/documentation gaps, not reproduced runtime failures.**

CI already includes meaningful fault and container tests; adding another generic unit-test target is not the missing work. Prior cluster throughput measurements use periodic fsync and are not a production durable-capacity baseline. Other archived end-to-end benchmark rows are explicitly marked invalid. The release workflow builds binaries, but states that the image is built and pushed manually; the CI image scan does not automatically establish the identity/security of the exact published digest. Backups default to the data volume and the chart provides no separate backup mount. Multi-host load/fault tests and large bootstrap validation remain unshown.

Evidence: [release workflow](../.github/workflows/release.yml), [CI](../.github/workflows/ci.yml), [nightly coverage](../.github/workflows/nightly.yml), [performance qualifications](CLUSTER_PERFORMANCE_VALIDATION_2026-09-29.md), [invalid historical benchmark rows](PERFORMANCE.md#end-to-end-grpc-baseline-publishbatch-par1), [release limitations](https://github.com/jatin711-debug/cronos_db_golang/blob/v0.6.0/docs/PRODUCTION_RELEASE.md#what-v060-was-released-without).

Close this with a digest-bound release pipeline that builds, scans, tests, and publishes the same artifact, with provenance and checksums; add Rust dependency auditing. Add off-volume backup configuration and restore drills that destroy the original volumes. These have no serving-path throughput cost. No current dependency vulnerability count is asserted by this audit.

**Recommended implementation order and throughput guardrails**

1. Land small invariant fixes for findings 1–3; move the corresponding probes into normal CI tests and extend them to encrypted records and durable replicated faults. Fix the latent quota lifecycle while consolidating admission only if that scope remains small.
2. Fix DLQ disposition durability and streaming startup verification. Establish regression coverage for leader-volume loss and large individual segments.
3. Add scheduler byte reservations/bounded hydration, a real dedup recovery frontier, and disk-headroom rejection. Keep format/protocol changes explicit and test rolling compatibility.
4. Address sparse completion transfer and live client topology discovery; then tackle the larger retention architecture if real workloads need long schedule horizons mixed with high-rate near-term work.
5. Gate the resulting production claim on measured durable throughput and fault recovery, and automate publication/backup evidence.

For the performance comparison, use the same machines, payload distributions, partition count, TLS/auth/encryption settings, RF=3/minISR=2, dedup enabled, and `fsync=batch` before and after each change. Exercise batches of 1, 100, and 1,000; small and large payloads; one and several groups; immediate and future schedules; a skewed hot partition; and a lagging follower. Keep offered IDs, unique accepted IDs, and terminal/pending outcomes separate: rejected requests and successful duplicate responses are not newly persisted throughput.

Record sustainable accepted-and-accounted-for events/s, publish p50/p95/p99, delivery lateness, duplicate rate, fsync latency, queue/cache bytes, process/cgroup memory, disk growth, recovery time, and follower catch-up lag. Fault tests must also reconcile DLQ and explicit expiry outcomes. Run enough to include rotation, checkpointing, hydration, pruning, and steady-state resource behavior; follow a shorter controlled comparison with a multi-hour soak on multiple hosts.

Suggested initial change budget, subject to the workload's requirements: investigate a repeatable drop greater than 5% in durable sustainable throughput or a rise greater than 10% in p99 publish latency across matched runs. These are proposed regression budgets, not measured guarantees or release capacity promises. Correctness remains mandatory; a faster path that acknowledges unreadable or unsafe data is not a valid baseline to preserve.

Retain group fsync, parallel follower sends, quorum completion without the slowest unnecessary replica, batched dedup/ACK writes, and work outside the WAL append lock. If profiles show replicated partitions bottlenecking on `ReplicateMu` spanning local sync and replication, consider a bounded per-partition batch combiner with an explicit latency budget. Pipelining the replication protocol is a separate, higher-risk optimization; do not mix it into the initial correctness fixes.

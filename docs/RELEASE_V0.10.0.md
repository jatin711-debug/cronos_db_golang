# CronosDB v0.10.0

This release fixes the thirteen defects confirmed by the
[1.0 readiness review](RELEASE_READINESS_1.0.0_2026-10-28.md). Two could lose
acknowledged writes: a change of replica placement, and a retried publish
after a change of leader. Others delayed scheduled events, accepted duplicates,
stopped an encrypted node from starting, or made reads come back empty. It
keeps the version-2 WAL format of v0.9.0: v0.9.0 data directories are used as
they are. It is not 1.0: the release gates the review lists are still open.

## Changes

- **A replica that placement drops stays in elections until it is no longer
  needed.** Placement used to drop a replica at once, before its replacement
  had copied anything. The leader went on replicating to the dropped node and
  counting its acknowledgements, while elections no longer asked it. A leader
  failure in that window could elect a replica without writes acknowledged
  with its help; growing a cluster by two nodes could elect an empty replica.
  A dropped replica is now *retiring*: elections ask it and count it, and it
  keeps its copy. A leader stops replicating to it before taking up the new
  assignment. The retiring list is cleared once the leader has done so and
  enough remaining replicas hold its log. If the leader and a retiring replica
  both fail before then, the partition waits for one of them rather than
  elect a replica that may lack acknowledged writes.
- **A retried publish is acknowledged only once it is committed.** A retry
  can concern an entry an earlier leadership term wrote. Replication now goes
  on through the current term's start record, which is what commits it.
- **Scheduled events no longer drift later over time.** The scheduler's wheel
  advanced one tick per timer fire, so every pause of its worker (a pass
  loading events from disk, garbage collection, CPU throttling) delayed every
  later event for good. It now advances to the time the clock shows, and
  never past it.
- **Duplicate detection keeps accepted IDs.** Hourly maintenance emptied the
  bloom filter without reloading the stored IDs, and judged it full under
  ordinary traffic; retries of accepted IDs were then accepted again. The
  filter is rebuilt from the store, with checks going to the store meanwhile,
  and only when it really is too full.
- **Message IDs are unique per topic.** The same ID on two topics used to be
  refused the second time, and a client allowed to publish to one topic could
  block, or detect, IDs used on another.
- **An encrypted node starts after a crash during a write.** An encrypted
  record that was not completely written stopped the log from opening, and
  `cronos-admin check-log --repair` could not fix it. It is now cut off like
  a torn plaintext record. A wrong key, or a complete record that does not
  decrypt, still stops the log.
- **Reads no longer come back empty after a crash.** The sparse index could
  hold a partial entry after a crash, and after the next two restarts reads of
  the events behind it returned nothing, without an error. The index is now
  written in whole entries, cut back to its last valid entry when opened, and
  a read it points to the wrong place reads the segment from its start.
- **Backups keep the change feed's position.** A restored partition started
  its Kafka/webhook export at the end of its log, and events not yet exported
  when the backup was taken were never exported.
- **Go SDK.** With a `CheckpointStore`, the SDK saves only the completion
  floor the server reports: every offset below it is complete. It used to
  save the offset after the last acknowledged delivery, and since events are
  delivered in schedule order, a consumer that resumed from it skipped
  unfinished events. With `CommitStrategySync`, `Ack` now returns the server's
  answer, including refusals; it used to return once the acknowledgement was
  queued.
- **Limits on what clients create.** Consumer group names and subscription
  IDs are at most 255 bytes, and a partition holds at most
  `--max-consumer-groups` groups (default 10000). A replay's `speed` must be 0
  or at least 0.001, and cancelling a replay ends its pause.
- **Helm chart.** The disruption budget allows one pod down at a time
  (`podDisruptionBudget.maxUnavailable`). It used to keep floor(RF/2)+1
  pods, which let a drain take 3 of 5 production pods at once. Pods are
  spread across Kubernetes nodes (`podAntiAffinity`, `soft` by default); the
  production values require it (`hard`), so they need one node per pod.

## Upgrade and compatibility

**v0.9.0 data directories are kept.** The WAL format is unchanged. Stop every
node, upgrade, and start them again: mixed-version clusters remain
unsupported, and cluster metadata gains a field that v0.9.0 nodes drop.

- Duplicate-detection records written by v0.9.0 are not carried over. Until
  dedup TTL (`--dedup-ttl`, 168 hours by default) has passed since the
  upgrade, a retry of a message ID first accepted by v0.9.0 is accepted again.
- A local SDK checkpoint saved by an earlier SDK may be past unfinished events.
  Clear checkpoint stores when upgrading the SDK; the server's own progress is
  kept.
- Subscriptions to a consumer group whose name is longer than 255 bytes are
  refused.
- The production Helm values need at least as many Kubernetes nodes as
  `replicaCount`. Set `podAntiAffinity: soft` to run them on fewer.

The chart's version and default image tag are `0.10.0`. A downgrade to
v0.9.0 has not been tested. The v0.8.0 capability and v0.9.0 format
constraints still apply. Delivery remains at least once.

## Validation and artifacts

Every defect has a regression test that failed on v0.9.0's source (`460f8a2`)
and passes here. The full Go suite passes, the changed packages pass under
the race detector, and the process acceptance suite passes on three-node
clusters. Node logs from its node-replacement and node-removal runs show the
retiring path: a leader removes the former follower, and the former replica
is released once the remaining replicas hold the log. The required CI checks
passed on [PR #17](https://github.com/jatin711-debug/cronos_db_golang/pull/17).

The release gates in the [readiness review](RELEASE_READINESS_1.0.0_2026-10-28.md#release-gates-beyond-fixing-individual-defects)
remain open: a sustained multi-host soak, recovery and upgrade drills, and
machine or power-loss testing. No new throughput or capacity claim is made.
The [remaining release limits](PRODUCTION_RELEASE.md#what-v080-was-released-without)
continue to apply.

Publication status, binary checksums, the image scan and exact image identity
are recorded on the [v0.10.0 release](https://github.com/jatin711-debug/cronos_db_golang/releases/tag/v0.10.0).
The binaries cover Linux and macOS amd64/arm64 and Windows amd64 and use the
Go bloom filter. The container is Linux amd64 and uses the Rust implementation.
`release.yml` stages the scanned and fault-tested image archive on a draft
release; local publication loads that exact archive, verifies its config and
layers, pushes `johnny711dock/cronos-db:0.10.0` and moves `latest`. Pin the
`IMAGE_DIGEST` recorded on the release for deployments. Local publication has
no signed GitHub workflow provenance.

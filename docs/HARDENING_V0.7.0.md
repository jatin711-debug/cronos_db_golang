# v0.7.0 hardening

These changes implement the remaining findings of the [October audit](PRODUCTION_AUDIT_2026-10-16.md). They were released as `v0.7.0` on 2026-10-17.

This is the dated v0.7.0 record; its measurements are not v0.8.0 results. The
[v0.8.0 notes](RELEASE_V0.8.0.md) describe current-term commitment, opt-in event
mutations/relocation, and lifecycle snapshots that bind progress and dead letters
to a WAL generation. Those supplement the legacy snapshot path described here.
The original release automation command below refers to the v0.7.0 workflow;
current manual runs stage an archive unless `publish_image=true` is explicit.

## Resource and recovery behavior

The scheduler shares a 64 MiB payload budget across partitions, configurable with `--scheduler-memory` / `CRONOS_SCHEDULER_MEMORY`. Overflow is stored as timestamp/offset references and reloaded in bounded pages. This is a scheduler allocation estimate, not a process RSS limit. Worker queues, Pebble, in-flight requests and a transient record read use additional memory.

Dedup recovery uses `dedup/recovery.json` as a durable replay boundary, validated against the WAL entry's term and message ID. It is written after pending IDs and Pebble tables are flushed. An absent or mismatched checkpoint causes a scan of the retained log, without the old 500,000-event cutoff. Unreadable checkpoints or recovery failures stop admission/startup/promotion rather than serving incomplete dedup state.

Batch quota reservations exclude duplicate retries. Both publish methods return reservations on a definite failure before append. Appended operations retain their reservation and dedup claim while their original publish completes.

## Completion and dead-letter replication

Sparse consumer completions transfer in pages of at most 4,096 offsets. Each follower has its own cursor and retries failed pages. Version changes and periodic refreshes catch additions behind a cursor. A follower that catches up with the event log receives progress again.

Dead letters transfer from the durable journal, including payloads evicted from the in-memory queue and removal tombstones. Pages contain at most 256 records, normally around 512 KiB; a single larger record can exceed that byte target. Replicas sync records before acknowledging, apply retries idempotently, and encrypt the local copy with their configured key. Completion pages are withheld until the follower has the corresponding dead-letter journal. Retention waits for every assigned follower to acknowledge it. Replicas missing dead-letter state are excluded from the leader's in-sync report. A new replica cannot skip the retained log boundary until its journal is durable. Bulk snapshots transfer journal pages before installing the WAL; a failed journal write leaves the existing WAL in place.

The journal is retained, including removal history. Replay and idempotence metadata grow with its history; journal compaction is not added by this change. Corruption blocks transfer and consequently retention until repaired. Backups remain necessary.

During a rolling upgrade, an older follower cannot provide the new durable acknowledgement. Once dead letters exist, the new leader holds completion transfer and retention for that follower until it is upgraded or removed from the replica assignment. Event replication continues within the follower's existing log coverage. A follower missing the discarded prefix cannot advance past it, and bulk snapshots with dead-letter history require an upgraded receiver. An older leader cannot supply the new guarantee; finish upgrading the cluster before relying on replicated dead-letter recovery.

## Discovery and disk operations

A node started with `--advertise-grpc-addr` (`CRONOS_ADVERTISE_GRPC_ADDR`) gives clients that address in partition metadata; the Helm chart sets it to each pod's DNS name with `service.advertisePodAddress=true`. Clients learn added nodes, retire obsolete discovered connections and retain explicit `NodeIDToAddress` overrides. Responses from older servers preserve endpoints already learned. A node started without the flag advertises nothing, and clients reach it through the addresses they were configured with, as before. Advertise addresses reachable from clients, and covered by the client port's certificate when TLS is on, or configure overrides when using proxies.

Before the release, metadata named each node's cluster gRPC address (`--cluster-grpc-addr`), where replication and Raft are served. A client sent there was refused with "unknown service cronos_db.EventService", or at the TLS handshake when the nodes use mutual TLS. The acceptance tests did not notice: in containers they configure `NodeIDToAddress`, and as processes a refused handshake made the client try another node. The image check of the release found it, and `TestClientsFindTheOtherNodesThroughPartitionMetadata` now publishes to every partition through a client given one node.

Full followers reject new log growth while still handling retries of matching entries already on disk. `cronos_retention_blocking_offset` and `cronos_retention_blocking_age_seconds` expose the oldest observed retention blocker by partition and reason: `unaccepted`, `change_feed`, `future_schedule`, `consumer_completion`, or `dead_letter_replication`. An offset of -1 means no blocker was observed in the last pass.

Set Helm `backupPersistence.enabled=true` for a separate per-pod backup PVC mounted at `/backups`; size, mount path, access mode and storage class are configurable. Choose storage in a separate failure domain and preserve the claims during recovery. A second PVC alone does not protect against loss of its storage backend.

## Release automation

The release workflow needs GitHub repository secrets `DOCKERHUB_USERNAME` and `DOCKERHUB_TOKEN`, with push access to `johnny711dock/cronos-db`. It runs when a GitHub release is published, or with:

```sh
gh workflow run release.yml --ref v0.7.0 -f tag=v0.7.0
```

The image job requires the tag's commit on `main` and the workflow's source commit to match it. It builds one linux/amd64 image, scans for fixable HIGH/CRITICAL vulnerabilities, and runs both container acceptance tests. It then pushes a run-specific candidate tag, records the registry digest, creates signed provenance with [GitHub's attestation action](https://github.com/actions/attest), and publishes the release tag from the same image. `IMAGE_DIGEST`, scan results and test logs preserve the evidence. It does not move `latest`.

For `v0.7.0` the repository did not have the two secrets, so the image job could build, scan and test the image but not push it. The `v0.7.0` image was built from the tagged commit, scanned and started as three nodes by hand, and pushed by hand as `0.7.0` and `latest`, as for `v0.6.0`. It has no signed provenance.

CI also audits the locked Rust dependencies. The publication job performs container acceptance and image scanning in Linux CI before registry publication and signing. Local workflow lint alone does not establish those results.

## Validation and release gates

Local validation on 2026-10-16 includes all 33 Go test packages (Windows amd64, cgo disabled), focused race checks for the changed scheduler, completion, dedup, discovery, quota and dead-letter paths, workflow lint, strict documentation build, and chart assertions for persistent/ephemeral data with and without a separate backup PVC. The dead-letter regression covers loss of the original leader after WAL retention, replacement catch-up through a snapshot, failed journal writes and mixed-version acknowledgement.

These local results come from separate runs. The required GitHub production checks passed on the pull request that took the release to `main`.

The process acceptance runs passed node faults, certificate/CA rotation, overload refusal, lost-node removal, first-node replacement, backlog recovery and topology convergence after a node was killed and restarted. The overload run delivered all 6,315 acknowledged events. Process backlog recovery delivered all 11,365 acknowledged events with peak observed memory of 111 MiB against a 142 MiB configured limit.

The Linux image passed the container network-partition test with all 2,742 acknowledged events delivered. Its memory-limited restart test accepted 16,371 events of 64 KiB (about 1 GiB), restarted every node, and delivered every acknowledged event. Peak observed memory from restart through delivery was 208 MiB per node against the kernel-enforced 384 MiB limit, with no OOM kill. Thirteen initial publishes failed during cluster formation and are excluded from the acknowledged-event guarantee.

Backup restore passed after every node and data directory was lost. All events acknowledged before the selected backup cut were present in the restored logs, and the cluster accepted new work and rejected an earlier ID as a duplicate. Forty-five of the 60 previously delivered events were redelivered, which is permitted by the at-least-once backup contract.

The local Linux image also passed Trivy 0.75.0 with no fixable HIGH/CRITICAL findings at scan time; this is a thresholded scan, not a claim that the image has no vulnerabilities. The scan report identifies local image `sha256:fc47a6380e8f5355825f98f2fc5f9e22a18cb89d8a33cb7bff5ca21e74b4d76b`.

The process overload test intentionally raises the scheduler budget so it reaches the process memory guard. With the default budget, this workload spills to disk and need not reject publishes merely because the accepted backlog exceeds RAM.

The initial single-node Windows publish benchmark was slower after the change: three-sample medians were about 21.7k versus 40.8k events/s on the first-batch baseline. These short runs overlapped other validation, varied substantially, and accumulate a backlog without a consumer; the bounded scheduler starts spilling while the baseline keeps payloads in memory. This is a performance concern to investigate, not evidence that the audit's RF=3/minISR=2 durable-capacity comparison passed. Three-node runs with production TLS, authentication and encryption were later matched against `v0.6.0` on the same Windows host, with every accepted event checked as delivered: the rates were level, within the variance of that host ([record](CLUSTER_PERFORMANCE_VALIDATION_2026-10-16.md)). Matched Linux runs, multi-host runs and real power loss are not established; `v0.7.0` was released without them.

The GitHub repository secrets `DOCKERHUB_USERNAME` and `DOCKERHUB_TOKEN` were still absent at release. Configure them before relying on the image publication job; no signed `v0.7.0` image exists.

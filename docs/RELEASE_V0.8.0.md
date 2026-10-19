# CronosDB v0.8.0

This release line adds opt-in future-event cancellation, rescheduling and
bounded WAL relocation. It also fixes commitment of old-term entries after
leader changes. Implementation and regression coverage were merged in
[PR #8](https://github.com/jatin711-debug/cronos_db_golang/pull/8).

## Changes

- `--event-lifecycle` enables cancellation and rescheduling through
  `GetEventState`, `CancelEvent` and `RescheduleEvent`. Save the publish
  receipt's partition, offset, term, message ID and topic. Mutations require
  the expected revision and a request ID; retry an ambiguous result unchanged.
- `--wal-relocation-bytes` bounds how much future work a retention pass may
  move from the oldest closed segment. Zero disables copying. A retained
  replacement preserves the original logical identity after its root is pruned.
- New leaders commit older entries only after a durable current-term
  term-start record reaches the required replicas. Idle partitions receive
  the same proof. Internal records consume physical offsets and stay out of
  user delivery, public replay and change-feed output.
- Recovery handles lost mutation replies, promotion, restart, suffix
  replacement and offset reuse. Staged mutations freeze their predecessor
  without releasing its retention obligations. Relocation reaches quorum
  before the predecessor can be pruned.
- Lifecycle snapshots and full backups carry a recovery manifest tying the
  WAL cut to its epoch, accepted boundary, consumer progress and dead letters.
  Recovery pages are bounded, checksummed and encrypted with the WAL key
  when encryption is enabled. A crash after WAL publication resumes import
  before the partition serves work.
- Acceptance nodes use their own memory policy instead of inheriting a
  runner's `GOMEMLIMIT`. With that correction, the process backlog campaign
  delivered all 9,389 acknowledged events at 81 MiB peak against a 117 MiB limit.

## Upgrade and compatibility

Lifecycle operations remain off by default. Standalone use requires RF=1
and `--min-insync-replicas=1`. Replicated lifecycle operations require majority
acknowledgements and every assigned replica, handoff target and Raft voter
to advertise the required formats before the coordinator activates them.

Term-start activation also waits for all assigned replicas and voters.
A mixed-version cluster retains legacy commit accounting until agreement,
including its old-term limitation. Complete the upgrade before relying on
the new guarantee. Once activated, format requirements persist through
placement and elections; downgrade to a build that ignores them is unsupported.

Version 1 rejects lifecycle operations combined with CDC, cross-region
replication, experimental transactions or online partition splitting.
Cancellation applies only to future work, not to events already delivered.
Delivery remains at least once. Existing performance measurements are for
v0.7.0; no new throughput claim is made for v0.8.0.

The Helm chart's version and default application image tag are `0.8.0`.
Only deploy that tag after image publication, preferably pinned by digest.

## Validation and artifacts

All six checks passed on the implementation revision `39a5216`, including
Linux cgo/race and non-cgo suites, the process fault campaign, Docker network
partitions and memory-limited restart, security scans, and production chart
startup with TLS, authentication and encryption. The release preparation
revision must pass those checks as well before promotion to `main`.

The GitHub release carries server and `cronos-admin` binaries for Linux and
macOS (amd64 and arm64) and Windows (amd64), with `SHA256SUMS`. These binaries
use the Go bloom filter; the Linux amd64 container uses the Rust implementation.

A manual preparation run of `release.yml` stages the tested image as
`cronos-db_v0.8.0_linux_amd64_image.tar.gz`, plus `IMAGE_ID`,
`IMAGE_ARCHIVE_SHA256` and `release-image-scan.json`, on a draft release.
The archive can be loaded with `docker load` after decompression. Its image
ID is local content identity, not a registry manifest digest or provenance.

Public image publication requires repository secrets `DOCKERHUB_USERNAME`
and `DOCKERHUB_TOKEN`. Publishing the GitHub release, or explicitly selecting
`publish_image=true` in a manual run, builds, scans and fault-tests the image,
pushes it, attests its registry digest, then publishes
`johnny711dock/cronos-db:0.8.0` and attaches `IMAGE_DIGEST`. The workflow does
not move Docker Hub's `latest` tag. A preparation run does not publish either
public tag or make the GitHub release public.

The existing limits in the [release requirements](PRODUCTION_RELEASE.md)
still apply: validation uses one host, has not simulated a machine power cut,
and does not establish deployment-specific throughput or very large refill
performance. Dead-letter journal history is not compacted. Relocation is
opt-in and does not make unfinished due events removable.

See the [lifecycle contract](RETENTION_AND_EVENT_LIFECYCLE.md) and
[current-term commitment protocol](REPLICATION_TERM_START.md) for details.

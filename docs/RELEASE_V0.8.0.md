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
The release and Linux amd64 image were published on 2026-10-19; pin the image
manifest digest below for deployments.

## Validation and artifacts

All six checks passed on the implementation revision `8bb7e2b`, including
Linux cgo/race and non-cgo suites, the process fault campaign, Docker network
partitions and memory-limited restart, security scans, and production chart
startup with TLS, authentication and encryption. The required source checks
also passed on the release preparation revision `ed7910f` before promotion
to `main` ([successful check run](https://github.com/jatin711-debug/cronos_db_golang/actions/runs/39275875123)).

The GitHub release carries server and `cronos-admin` binaries for Linux and
macOS (amd64 and arm64) and Windows (amd64), with `SHA256SUMS`. These binaries
use the Go bloom filter; the Linux amd64 container uses the Rust implementation.

A manual preparation run of `release.yml` stages the tested image as
`cronos-db_v0.8.0_linux_amd64_image.tar.gz`, plus `IMAGE_ID`,
`IMAGE_ARCHIVE_SHA256` and `release-image-scan.json`, on a draft release.
The gzip archive can be loaded directly with `docker load --input <archive>`.
`IMAGE_ID` identifies its config digest; a local engine may display a manifest
digest instead. Neither is a signed provenance statement.

`johnny711dock/cronos-db:0.8.0` (Linux amd64) was published from local
Docker on 2026-10-19 using the exact staged image archive above. Its config
digest and all nine filesystem layers match the scanned image. Local admin
and server readiness checks passed, and the published image was verified
through anonymous Docker Hub registry requests.

Pin the published image for deployments:

```sh
docker pull johnny711dock/cronos-db@sha256:428690215148b97eb9b6aaed5a3afb7c7a3227613c8fee731ab922e2276d4928
```

`IMAGE_DIGEST` and `LOCAL_PUBLICATION_EVIDENCE.json` record the registry
digest, source revision and verification. This local publication has no
signed GitHub workflow provenance. Docker Hub's `latest` was moved to this
image on 2026-10-19 as an OCI index (`sha256:a17853491e12898cfa22fa8b698c7c41d7f70ac3773da74c499226ad3efec176`)
whose only entry, linux/amd64, is the manifest above, so both tags run the same
image. Pin the digest above for deployments.

Automated publication through `release.yml` requires repository secrets
`DOCKERHUB_USERNAME` and `DOCKERHUB_TOKEN`. That workflow builds, scans and
fault-tests an image, pushes it and attests its registry digest. Local Docker
publication uses the workstation's saved Docker Hub login instead.

The existing limits in the [release requirements](PRODUCTION_RELEASE.md)
still apply: validation uses one host, has not simulated a machine power cut,
and does not establish deployment-specific throughput or very large refill
performance. Dead-letter journal history is not compacted. Relocation is
opt-in and does not make unfinished due events removable.

See the [lifecycle contract](RETENTION_AND_EVENT_LIFECYCLE.md) and
[current-term commitment protocol](REPLICATION_TERM_START.md) for details.

# CronosDB v0.8.1

This patch contains security dependency updates and two concurrency fixes found
while validating the v0.8.0 documentation audit. It retains the v0.8.0 lifecycle
and current-term commitment protocols.

## Changes

- Go 1.26.10 replaces 1.26.9 in the module and container builder, addressing
  `GO-2026-6809` in `crypto/x509`.
- Vite 7.3.7 replaces 7.3.6, including fixes for the bundled `launch-editor`
  advisories. `github.com/klauspost/compress` v1.18.7 replaces v1.18.2 to fix
  `CVE-2026-63209` in dictionary processing.
- Consumer-group initialization finishes before a live subscription is exposed
  to delivery, so its first acknowledgement can record completion. Duplicate
  subscription IDs are rejected before changing membership, and server-assigned
  subscriptions join their resolved partition.
- Deduplication IDs remain visible to existence, offset and timestamp lookups
  while their pending buffer is being committed to Pebble. Failed flushes keep
  the buffered entries available for retry.
- The documentation audit corrects release status, lifecycle and retention
  behavior, recovery guarantees, configuration flags and unsupported features.
  Its per-document report is [available here](DOCUMENTATION_AUDIT_2026-10-21.md).

## Upgrade and compatibility

The Helm chart's version and default image tag are `0.8.1`. There is no new WAL
or replication format in this patch. The v0.8.0 capability activation and
downgrade constraints still apply; see the [v0.8.0 notes](RELEASE_V0.8.0.md).
Lifecycle operations remain opt-in, and delivery remains at least once.

Existing v0.8.0 binaries and images retain their original contents. Use the
v0.8.1 artifacts to obtain these fixes. No new throughput or multi-host capacity
claim is made. The [remaining release limits](PRODUCTION_RELEASE.md#what-v080-was-released-without)
continue to apply.

## Validation and artifacts

The implementation passed all required source checks before PR #10 merged:
Linux cgo/race and non-cgo suites, the process fault campaign, security scans,
dashboard/chart checks, container network faults, memory-limited restart and
production chart startup. The paused-flush regression and strengthened dedup
concurrency test also passed 100 local non-cgo repetitions; the dedup package
passed 20 local cgo/race repetitions. Release preparation runs the required
checks again on the final release revision.

Publication status, binary checksums, the image scan and exact image identity
are recorded on the [v0.8.1 release](https://github.com/jatin711-debug/cronos_db_golang/releases/tag/v0.8.1).
The binaries cover Linux and macOS amd64/arm64 and Windows amd64. They use the
Go bloom filter. The container is Linux amd64 and uses the Rust implementation.

`release.yml` stages the scanned and fault-tested image archive on a draft
release. Local Docker publication loads that exact archive and verifies its
config and filesystem layers before pushing `johnny711dock/cronos-db:0.8.1` and
promoting `latest`. `IMAGE_DIGEST` and `LOCAL_PUBLICATION_EVIDENCE.json` on the
release record the registry identity and verification. Pin that digest for
deployments; `latest` is a mutable convenience tag. Local publication has no
signed GitHub workflow provenance.

After local publication and evidence upload, dispatch
`publish-staged-release.yml` with `--ref main -f tag=v0.8.1`. It verifies the
successful staging run, uploaded checksums and registry tags before publishing
the draft as the latest GitHub release. It does not rebuild the image or need
Docker Hub write credentials in GitHub.

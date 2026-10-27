# CronosDB v0.9.0

This release changes the on-disk WAL segment format and **requires new data
directories**. It detects erased areas of closed log segments that earlier
versions could not tell from unused space, and it fixes a node crash that a
scheduled backup could cause while a partition was being moved away. The
lifecycle and current-term commitment protocols of v0.8.0 are unchanged.

## Changes

- **WAL segment format version 2.** A new segment's header records the closed
  predecessor's first offset, last offset and byte end, covered by the header
  checksum. A segment is preallocated, so zero bytes after its last readable
  record look the same whether they are unused space or erased data; record
  checksums cannot tell the two apart once every record beyond that point is
  gone, and the sparse index can be missing or stale. Startup and
  `cronos-admin check-log` now compare each recorded boundary with the prefix
  they recover and refuse to open a log that does not match, including a zeroed
  tail with no index. A verified indexed record beyond the point where reading
  stops also marks damage in the active tail. See
  [closed WAL segment boundaries](STORAGE_SEGMENT_BOUNDARIES.md).
- **Repair stays explicit.** `check-log --repair` cuts a damaged segment after
  its last readable record and updates the successor's boundary. It discards
  every later record in that segment, and reports that the last discarded
  offset is unknown when no segment follows. It refuses to change a segment
  whose recovered records extend past the recorded boundary; restore a backup or
  rebuild the replica from its leader instead. For a replicated partition,
  rebuilding from a healthy leader is preferred.
- **Backups and restore check the format first.** `cronos-admin restore` and
  the WAL restore path reject a backup holding version-1 segments before
  anything is published, so an incompatible backup cannot appear to restore and
  then fail at the next start. The legacy incremental backup keeps the last
  closed segment's boundary by copying the active segment's header as an empty
  successor.
- **A scheduled backup can no longer crash a node.** A backup that overlapped
  the release of a partition, or a shutdown, could use that partition's stores
  after they were closed. The storage engine panics on that instead of
  returning an error, which ended the whole process. A partition is now kept
  open until a running backup has finished with it, and a backup that begins
  after a partition was stopped leaves it out. A partition is also stopped only
  once, however many callers reach it.
- The Go 1.26.10 toolchain of v0.8.1 is kept. The dashboard's test tooling
  moves to `undici` 7.30.1 and `vitest` 4.1.12 to clear new advisories; these
  are development dependencies and do not ship in the dashboard bundle.

## Upgrade and compatibility

**Start with new, empty data directories on every node.** This version reads and
writes only version-2 segments. It rejects version-1 logs and backups with an
error that names the version it found, and it does not migrate them. Older
binaries reject version-2 segments. Rolling upgrades and mixed-version clusters
are unsupported, and a version-1 backup must not be restored into a version-2
cluster. Keep existing data directories separately if their contents might be
needed.

The Helm chart's version and default image tag are `0.9.0`. There is no new
replication format, and the v0.8.0 capability activation and downgrade
constraints still apply; see the [v0.8.0 notes](RELEASE_V0.8.0.md). Lifecycle
operations remain opt-in, and delivery remains at least once.

Existing v0.8.1 binaries and images retain their original contents. Boundary
evidence protects closed segments created by this version. The currently active
segment has no closed boundary, deleting whole files together with their
evidence is outside what it can detect, and records are checked when a log is
opened, not continuously. No new throughput or multi-host capacity claim is
made. The [remaining release limits](PRODUCTION_RELEASE.md#what-v080-was-released-without)
continue to apply.

## Validation and artifacts

The storage, partition, replication and compliance suites pass, and so does the
process acceptance suite on three-node clusters, including backup and restore,
node replacement, certificate rotation and a restart with a backlog larger than
memory. The container tests (network faults and memory-limited restart) run
against the exact image when it is staged. The backup crash has two regression tests: one reproduced the failure's stack
trace before the change, and the other shows a release waiting for a backup that
is using the partition. Release preparation runs the required checks again on
the final release revision.

Publication status, binary checksums, the image scan and exact image identity
are recorded on the [v0.9.0 release](https://github.com/jatin711-debug/cronos_db_golang/releases/tag/v0.9.0).
The binaries cover Linux and macOS amd64/arm64 and Windows amd64. They use the
Go bloom filter. The container is Linux amd64 and uses the Rust implementation.

`release.yml` stages the scanned and fault-tested image archive on a draft
release. Local Docker publication loads that exact archive and verifies its
config and filesystem layers before pushing `johnny711dock/cronos-db:0.9.0` and
promoting `latest`. `IMAGE_DIGEST` and `LOCAL_PUBLICATION_EVIDENCE.json` on the
release record the registry identity and verification. Pin that digest for
deployments; `latest` is a mutable convenience tag. Local publication has no
signed GitHub workflow provenance.

After local publication and evidence upload, dispatch
`publish-staged-release.yml` with `--ref main -f tag=v0.9.0`. It verifies the
successful staging run, uploaded checksums and registry tags before publishing
the draft as the latest GitHub release.

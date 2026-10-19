# WAL retention, cancellation, and rescheduling

Status: implemented on `retention-and-cancel`, off by default. Standalone nodes
use RF=1. Replicated clusters activate only after every assigned replica and
Raft voter advertises the required protocols. Release validation is required
before merging or publishing the branch.

## Contract

`--event-lifecycle` enables future-event cancellation and rescheduling. A
replicated cluster requires majority acknowledgements. Standalone RF=1 uses
`--min-insync-replicas=1`.
`--wal-relocation-bytes` optionally sets a byte budget for moving future work
out of the oldest closed WAL segment; zero disables copying. The matching
environment variables are `CRONOS_EVENT_LIFECYCLE` and
`CRONOS_WAL_RELOCATION_BYTES`.

The WAL remains authoritative. A change appends a self-contained successor;
it never edits the original record. A payload-free index is rebuilt from the
retained WAL before readers run. Consumer acknowledgements always refer to the
physical offsets actually delivered, so moving work never inherits an old
copy's completion.

Save the publish receipt's partition, offset and term together with message ID
and topic as an `EventReference`. The Go producer exposes the term in
`SendResult`. `GetEventState` returns the current revision. `CancelEvent` and
`RescheduleEvent` require that revision and a unique request ID. Retry an
ambiguous request unchanged; a completed retry does not append another record.
`Aborted` means the revision changed, `NotFound` means the reference expired or
never matched, and `Unavailable` means its outcome is unresolved or this node
cannot serve it. Only future events can be changed; cancellation is terminal.
Rescheduling also requires a future timestamp. This is not a recall protocol
for events already sent to consumers.

## Identity and staged state

A logical identity is `(partition, original offset, original term, message ID,
topic)`. Its term prevents an offset reused after recovery from selecting a
different event. Rescheduling and relocation preserve that identity. User
mutations create a new revision; relocation preserves the last user revision
and request history. The reference remains valid after its original physical
record is pruned while a retained successor represents it. Deduplication TTL
is never the recovery frontier.

A staged successor freezes delivery of its predecessor immediately. It does
not make that predecessor removable. Only an accepted successor proves that
an old copy is terminal for retention; an accepted cancellation is itself
terminal. An unresolved mutation cannot release a prefix, report success,
reach public replay, or cause repeated evacuation of a pinned segment.

The delivery gate excludes a mutation through durable local append and index
update. In a cluster, the append lock preserves WAL order while replication
runs. Success requires durable acknowledgement by the configured replicas.
Timeout after append has an unknown outcome: the record is held until catch-up
proves its acceptance or another leader's suffix replaces it. A later retry
resolves that outcome using the original request ID and revision.

The [current-term commitment protocol](REPLICATION_TERM_START.md) establishes
the accepted boundary after promotion. Counting old-term replicas alone cannot
release work. A new leader's durable term-start record must reach the required
replicas before its preceding log is committed, including on idle partitions.

## Format and recovery

Version 1 uses CRC-protected WAL records with reserved `cronos.internal.*`
metadata. Public publish and batch publish reject that namespace. Cancel records
have no payload; reschedule and relocate records contain the complete event.
Public delivery and replay remove the lifecycle envelope and skip term-start
records entirely.

The disk-backed index stores the local head, mutation offsets and terms, and
cleanup keys without payloads. Accepted-state queries seek directly to the
newest retained mutation at or below the supplied boundary. Rebuild uses at
most 1,024 offsets with a 4 MiB target per chunk; a chunk may exceed the byte
target by one valid record. Readers see a complete generation. A failed rebuild
or conflicting observation fails delivery and retention closed.

A suffix cut forgets invalid consumer completions and rebuilds the index under
the delivery gate before offsets can be reused. Cached records removed by that
cut stay inactive. Promotion reconstructs timers from the surviving log and
uses a fresh current-term proof before allowing delivery. A successful
cancellation does not resurrect after failover. A retained reschedule or
relocation represents one pending event after recovery.

Standalone lifecycle nodes persist a new generation term on open, protecting
receipts from periodic-fsync offset reuse. Cluster nodes preserve the epoch
assigned by the coordinator on reopen; they do not invent an unassigned epoch.
They accept election fences and replicated progress, while standalone lifecycle
partitions refuse those requests.

## Replication and rolling upgrades

Nodes advertise `lifecycle-records/1`, `lifecycle-replication/1` and
`term-start/1` when their configuration supports lifecycle data. Enabling
`--event-lifecycle` on cluster nodes asks the coordinator to commit that
one-way requirement. Every assigned replica, handoff target and Raft voter
must be alive and capable first. Before agreement, mutations are refused.
Placement, elections, handoffs and replacement voter admission continue to
honor the durable requirement afterwards. Downgrading a required partition or
voter to a build that ignores capabilities is unsupported.

Every request of a term-start log, including empty probes and catch-up, uses
`AppendTermStart`, independently naming both record formats. Older servers
answer `Unimplemented`; there is no fallback to ordinary append. Receivers
validate versions, requirements and envelopes before storing records. The
lifecycle-only `AppendLifecycle` transport remains for format compatibility;
it cannot carry term-start records.

## Retention and relocation

Pruning removes a contiguous prefix of closed segments. A pass may move active,
never-due work out of the oldest selected segment within its encoded byte
budget. It does not bypass disk admission, unresolved mutations, unfinished due
work, change-feed progress or durable dead-letter replication. The active
segment is not evacuated.

A replicated evacuation uses the append lock and replicates its durable
replacement in WAL order. It cannot prune the predecessor until quorum
acceptance. An interrupted pass stages at most the budget it was given;
catch-up resolves that state, and a later pass does not duplicate the copy.
The leader advertises the new log start only after proving retention safe.
A follower behind that start resets there and receives the retained successor.
Raising its consumer floor must not complete that successor's newer offset.

An accepted cancellation or superseded record has no remaining consumer
obligation. The active replacement still does. An unaccepted control cannot
release a segment. Index cleanup is bounded and advances the retained floor,
so old cached references cannot become active after their heads are collected.

## Consistent recovery snapshots and backups

A lifecycle snapshot requires recovery format 1 in addition to the record
formats. Its manifest names partition, epoch, physical first/last offsets and
accepted boundary. Consumer progress is captured before dead letters while
appends and suffix replacement are excluded. Progress beyond the captured
accepted boundary is omitted. Recovery pages are bounded, checksummed and
written with an explicit footer. Payload-bearing pages use the WAL's encryption
key when encryption at rest is enabled.

The receiver validates the complete transfer and manifest before switching
WAL generations. Recovery pages move with the segments through the existing
journaled install. An interrupted transfer leaves the old live state intact.
After publication, stale local completion records are cleared, dead letters
are made durable, and then matching consumer progress is imported and synced.
The recovery file remains until import completes. A crash after WAL publication
therefore resumes import on startup before the partition serves work. It cannot
combine old mutation heads or completions with newly reused offsets.

Full backups retain lifecycle and term-start markers and include the same
recovery manifest/pages alongside their WAL cut. Restore validates files,
selects a complete WAL generation and reconstructs the index. An encrypted
backup requires the matching master key. Credentials and key material are not
included in backups.

## Change feeds and migration

Version 1 supports lifecycle operations without CDC, cross-region replication,
transactions or online partition splitting. Those combinations are rejected
explicitly before enabling the format; nodes configured for them do not
advertise lifecycle replication support. A raw publish sink has no cancellation
or reschedule contract, so controls must never be presented to it as new
publishes. Adding logical-operation sinks or migration identity mappings is a
separate feature, not a fallback in this protocol.

Ordinary term-start clusters can continue using change feeds. Feeds skip
term-start records while advancing physical cursors; cross-region fetch also
returns `next_offset` for a control-only page. It exports only the committed
prefix. Partition splitting skips term-start records because a leadership
proof belongs to its original log.

## Validation

Focused tests cover revisions, exact retries, forged envelopes, due-event
rejection, delivery/mutation exclusion, staged versus accepted history,
periodic fsync, encryption, bounded relocation, pruning and backup restore.
Replication tests cover current-term proof, lost success replies, leader death,
restart, election fences, consumer progress, suffix replacement and offset
reuse, retained replacements whose roots were pruned, incompatible receivers,
and interrupted snapshot installation. Snapshot tests include a crash after
WAL publication but before recovery-state import.

Three-node acceptance tests compare authenticated physical WAL snapshots for
contiguous offsets and identical events, including internal records. The
lifecycle campaign activates through committed capabilities, cancels and
reschedules future work, loses its leader, restarts the entire cluster, and
checks delivery after the original schedule has passed. The general campaign
also exercises node replacement, retention and whole-cluster loss.

Use low Go concurrency on the development laptop; do not substitute a
throughput benchmark for invariant and fault tests. Acceptance node processes
must use their own memory policy rather than inherit a test runner's
`GOMEMLIMIT`. Run both native race tests and the non-cgo release suite, plus the
Docker network-partition and container-memory checks in CI before release.

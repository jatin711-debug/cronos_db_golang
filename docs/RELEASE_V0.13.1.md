# CronosDB v0.13.1

This release fixes what failed v0.13.0's first 24-hour soak, and what the
soaks of those fixes found next. Some acknowledged events were never delivered
to a slow consumer group. Throughput fell to a tenth after the first node was
lost. Nodes held more memory than their limit once they had run for a few hours.
On XFS, each log segment took three times its size on disk. It also takes the
costs the soaks' profiles showed off the publish and delivery paths. It keeps the version-2 WAL format and changes no
API, flag or metadata format: v0.13.0 data directories and cluster state are
used as they are. It is not 1.0: the release gates of the
[1.0 readiness review](RELEASE_READINESS_1.0.0_2026-10-28.md) and the
[deployment validation record](PRODUCTION_VALIDATION.md) (a multi-host soak,
production-sized recovery and upgrade drills, physical power-loss testing) are
still open.

## Who should upgrade

Every deployment that schedules events for later or runs consumer groups with
small credit windows; those are where events went undelivered. Also every
clustered deployment, whose replicated publishes are several times faster, and
every node whose memory limit is under about 2 GiB per partition, which could
exceed it. Linux deployments whose data is on XFS get back the disk space their
segments held hidden at the first start of v0.13.1. The supported upgrade from v0.13.0 is unchanged: stop every node,
replace the binary or image, and start them again; data directories are kept.
Applications using the Go client should take v0.13.1 of the module too: the
acknowledgement fix is in the client.

## What the soak found

The v0.13.0 24-hour soak (three nodes as processes on one 4-vCPU Linux host,
384 MiB each, encrypted WAL, `every_event` fsync, RF=3/minISR=2, 100 publishes
per second with bursts of 500, 100,000 events scheduled hours ahead) failed at
7.9 hours. At its third disruption, a leader lost while the slow consumer group
caught up, 16,107 acknowledged events had never been delivered to that group,
and publishes had fallen from about 90 per second to 9.

## Fixes

- **Held-back events are redelivered.** Events a consumer group could not take
  yet were queued for redelivery as one span from the lowest to the highest
  offset. Scheduled events fall due in schedule order, so one pass held back
  events written hours apart; each older one sent the walk back to the bottom
  of the span, and the newer ones were never reached. Held-back offsets are now
  kept as separate ranges, each with its own progress.

- **The Go client acknowledges at once when it has nothing else to do.** The
  consumer held acknowledgements until 64 had gathered or 50 ms had passed, so
  a group with a window of 4 credits could take at most 4 events per 50 ms per
  partition. An acknowledgement is now held only while more deliveries are
  buffered.

- **Segment indexes reach the disk.** The fsync coalescer, which every
  production WAL uses, synced records but never the segments' sparse indexes.
  A node that was killed lost its active segments' indexes, and from then on
  every point read of those segments decrypted them from the start, for as long
  as retention kept them. This is why throughput fell after the first
  disruption. The coalescer now flushes the indexes, and a segment that is
  opened with entries missing from its index adds them.

- **Node memory stays inside its limit.** Every partition opens four Pebble
  databases. Pebble allocates a memtable at its full size and keeps a spent one
  to reuse, and in the release binaries, which are built without cgo,
  memtables and the block cache are on the Go heap. With fixed sizes (a 256 MiB
  cache, 64 MiB dedup memtables) a four-partition node held 225 MiB of dedup
  memtables and 43 MiB of cache a few hours in, against a 384 MiB limit. It
  then refused most publishes and fired some scheduled events up to five
  minutes late. The cache now takes an eighth of the memory limit, at most
  256 MiB, and another eighth is divided among the partitions' memtables, with
  the old sizes as the ceiling. Each node logs the sizes it chose at startup
  ("Pebble memory"). The metrics loop and health checks no longer read every
  stored message ID to count them, and a sealed segment's index drops its write
  buffer.

- **Log segments take their own size on disk.** A segment is preallocated to
  its full size, and its last batch runs a few kilobytes past that. On XFS, a
  write that extends a file reserves about as much again past its end, and
  XFS keeps that reservation for as long as the file is open: its background
  trimming skips preallocated files, and closing one does not free it either.
  Sealed segments stay open for reads, so every segment took about three times
  its size on disk, which file sizes do not show. The second 24-hour soak
  filled its 40 GB disk this way with 13.5 GiB of files, and the disk guard
  refused every publish. A segment is now truncated to its own length when it
  is sealed and when it is opened, which frees the reservation and changes no
  data; on other filesystems and on Windows it does nothing.

## Performance

- **Point reads** skip to the record by its length prefix instead of
  decrypting up to 1,000 records before it: 11.7 ms to 0.32 ms. The scheduler
  reads cold events this way, one at a time.
- **Replicated publishes** hold the replication lock for the append and for
  queueing the sends only. They no longer hold it through the quorum's
  round trip and fsyncs. Sends waiting for a follower go together, up to
  8 MiB. Seeding 100,000 events took 35 seconds instead of 6 minutes 51.
  `every_event` still syncs each event.
- **Replicated appends** take the previous entry's term from the WAL's memory
  instead of reading the entry from disk.
- **Consumer progress** goes to followers as what changed since the last send,
  not every completion above each group's floor (about a million per group in
  the soak). The whole copy is still sent when a follower connects, when the
  change journal cannot reach back far enough, and every 30 minutes.
- **The scheduler** keeps the events it holds in memory in order of due time,
  and makes room for an earlier one directly. Before, with its memory budget
  full, it moved timers through the cold store on every publish: 48% of a
  leader's CPU in the soak. It scans the cold store from a floor instead of
  over every deleted entry: 4.56 ms to 9.5 µs per pass. It reads events
  before taking its tick lock and wakes at most every 100 ms.
- **Allocations**: scans reuse one plaintext buffer and pooled read-ahead
  buffers (2.06 MB to 0.65 MB for an encrypted 512-event read), and completion
  keys are built without `fmt` (258 ns to 40 ns, byte-identical).

## Compatibility

No on-disk format changes: completion keys are byte-identical, an index rebuild
only appends entries a file is missing, and cold store keys are unchanged.
Progress changes use the existing replication message, which followers apply
by adding, so v0.13.0 and v0.13.1 nodes work together during an upgrade. A
follower may now have 64 batches queued (4 before). Nodes with at least about
2 GiB of memory limit per partition keep the old Pebble sizes; smaller nodes
get smaller caches and memtables, which do not change what is stored. Trimming
a segment to its own length changes no data, so v0.13.0 reads segments v0.13.1
has trimmed.

## Verification

The unit suites passed, with the race detector clean on the changed packages.
The acceptance suite passed on processes on one Windows machine, and again in
CI on the merged code: the fault campaign on a three-node cluster
(996 seconds), and the network partition and memory-limit tests on
containers of the image.

The soaks ran on RunPod: three nodes as processes on one 4-vCPU Linux host,
384 MiB each, in the v0.13.0 soak's configuration.

| Build | Length | Result |
|---|---|---|
| v0.13.0 | 7.9 h of 24 | Failed: 16,107 acknowledged events undelivered; publishes 90/s to 9/s |
| Undelivered events and collapse fixed | 2 h | Passed all 7 disruptions; 772,635 accepted; publishes 30–50/s during the slow group's catch-up |
| Plus scheduler, replication and allocation changes | 2 h | Passed; 900,496 accepted; publishes about 100/s, median 3–5 ms, during the catch-up |
| Same | 5.5 h of 24 | Stopped: memory refusals from 4 h (612,046 refused); nothing lost |
| Plus memory sizing | 7.6 h of 24 | Failed: the disk guard refused every publish from 6.6 h, with 13.5 GiB of files filling a 40 GB disk; memory held at 100–250 MiB per node |
| Release build, compressed | 2.7 h of 4, at 450/s | Failed at the fifth disruption: a consumer group 391,000 events behind did not catch up within 15 minutes. Nothing refused, lost or early; heap in use at most 258 MiB per node; disk use equal to file sizes |

The compressed run was to put about the data of 18 hours at 100 publishes per
second through the cluster in 4 hours, with 8 MiB segments. It reconciled the
first four disruptions in 303, 27, 479 and 940 seconds at a steady 450
publishes per second, with none refused. By its end it had accepted 4.28
million events, about 12 hours' worth at 100 per second and past the volume at
which the memory (4 hours) and disk (6.6 hours) failures appeared. Each node's
data took 7.2 GiB, and its disk use stayed within 22 MiB of its file sizes.

The fifth disruption stops the slow consumer group for 14 minutes; at 450
publishes per second it came back 391,000 events behind. Catching up made the
nodes CPU-bound, mostly on Pebble compactions (58% of a node's CPU, 36% in
steady state). Publishes fell to about 180 per second with a 200 ms median,
and the group took about 200 events per second, too few to catch up within the
15 minutes the soak allows. Nothing was lost or delivered early. In the 2-hour
run at 100 publishes per second the same disruption reconciled in 552 seconds
with publishes held, so this is a limit of these 2-CPU nodes at four and a half
times that rate; see [Known limits](#known-limits).

The compressed run does not test what only time brings: consumer progress
fully re-sent every 30 minutes, the hourly bursts, events scheduled a day
ahead. A 24-hour run at 100 publishes per second needs more
than the 40 GB disks of the soak hosts used so far: the log cannot be pruned
until the seeded events fall due, by when the three nodes hold about 39 GB.

The release itself is checked as before. That means CI, the release workflow's
image scan, and the container fault tests on the exact image. It also means the
rolling upgrade drill from v0.13.0, whose rollback phase runs v0.13.0 nodes on
data the new build has written. The release page records the results and the
image digest.

## Known limits

Unchanged from v0.13.0: replication lag is reported by a partition's leader
only; retention and compaction take partition 0 as "all local partitions";
schemas cannot be registered at run time; manual rebalance returns
`Unimplemented`; tenant limits cover publish rate only; automatic lag-triggered
snapshot selection and dead-letter journal compaction are not implemented. The
soaks ran on one host, so the separate-host soak gate stays open. This release
makes no new capacity claim.

New in the soaks of this release: a consumer group far behind competes with
publishes for CPU while it catches up, mostly through the Pebble compactions
of its completion records. In the compressed soak, nodes limited to 2 CPUs
could not take 450 publishes per second and also catch up a group 391,000
events behind; publishes fell to about 180 per second until it had. Give nodes
CPU headroom over their steady load for catch-up. Making catch-up cheaper is
the next release's work.

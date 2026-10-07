# Three-node publish validation — 2026-09-29

These measurements use three local Windows/amd64 processes on one AMD Ryzen 7
6800H host, Go 1.26.7, 256-byte payloads, periodic fsync, and the gRPC batch
load test. All reported runs had a nonzero share of partition leaders on each
node, 100% `PublishedCount`, and zero reported publish errors. The load-test
counter measures accepted publish responses, not consumer delivery or survival
of a power loss. The three nodes share one CPU, memory pool, and SSD.

`scripts/compare-cluster-throughput.ps1` starts each node after the previous
one is healthy, checks the leader distribution in the load-test output, and
saves per-node logs and metrics under ignored `build/perf-comparison/`. Build
the server and `cluster_loadtest.go` with `-tags clustertest` before using it.
The harness keeps WAL data unless `-RemoveData` is passed; allocate enough disk
or clear only its temporary node directories after saving the evidence you
need. It does not start with less than `-MinFreeMB` of available memory and
stops the load if that falls below `-AbortFreeMB`. `-Pprof` saves CPU, block,
and mutex profiles for each node.

## RF=1 raw publish ceiling

The matched comparison uses 32 partitions, 128 MiB WAL segments, 32 publishers
per node, 50,000 events per publisher (4.8 million total), batches of 4,000,
and `AllowDuplicate=true`. This bypasses deduplication and uses one replica.
Both versions used the same load-test binary and fresh data in each sample.

| Build | Run 1 | Run 2 | Run 3 | Median |
|---|---:|---:|---:|---:|
| v0.5.0, CGO disabled | 717,695 | 732,563 | 743,281 | 732,563 events/s |
| Current branch after quorum guard, CGO disabled | 621,230 | 597,596 | 612,095 | 612,095 events/s |

The current median is 16.4% lower. A separate matched native Rust/CGO pair
measured 625,338 events/s on v0.5.0 and 453,924 on the current branch (27.4%
lower); it is one pair, so it is less precise. All eight runs above reported
zero publish errors. The code changes after `v0.6.0-rc.1` affect the epoch and
replication guard; RF=1 continues to use the unlocked WAL append path.

An earlier short 1.92-million-event pair with 512 MiB segments measured
601,735 events/s on v0.5.0 and 727,042 on `v0.6.0-rc.1`. Its opposite result
shows that short local tests vary. It is not evidence for a sustained 1M+
ceiling. The 19.2-million-event Makefile preset could not complete on this
machine: three nodes reserved 48 GiB of WAL segments during the attempt.

## RF=3 quorum path

The current branch was tested with `replication-factor=3`,
`min-insync-replicas=2`, `AllowDuplicate=false`, 16 partitions, 128 MiB
segments, and 256-byte payloads. Followers were registered, the three leaders
shared partitions, and captured `cronos_replication_lag` gauges were zero at
the end of successful runs.

| Publishers per node | Batch | Accepted events | Go fallback | Native Rust/CGO |
|---:|---:|---:|---:|---:|
| 4 | 1,000 | 240,000 | 62,348 events/s | — |
| 16 | 4,000 | 960,000 | 130,128 events/s | 110,773 events/s |
| 32 | 4,000 | 1,920,000 | 125,390 events/s | — |

Every listed run reported zero publish errors. These are publish acceptance
rates, not a fault campaign or independent per-ID follower reconciliation.
Periodic fsync does not imply each acknowledged write is safe against a
simultaneous power loss. Quorum loss, failover, and restart correctness remain
production release blockers in the audit.

## Correctness finding

The `v0.6.0-rc.1` source could acknowledge RF=3/minISR=2 batches while no
replication leader was installed. New router assignments used epoch zero,
promotion rejected nonpositive epochs, and the publish handler then fell
through to the single-replica WAL path. The current branch starts new epochs
at one and rejects replicated publishes if the local replication leader is
absent. Unit tests cover the missing-leader rejection and RF=1 fast path.
The safety fix is **not** in the `v0.6.0-rc.1` tag.

The historical 1,010,933 events/s entry in `ARCHITECTURE.md` had no preserved
run artifacts or independent durability check. The README's older ~790K RF=1
and ~200K RF=3 figures used different conditions and should not be treated as
current capacity. The matched results above show an RF=1 regression requiring
investigation before a production throughput target is set.

## Follow-up — 2026-10-06

**Cause of the RF=1 regression.** The post-v0.5.0 epoch fencing persists the
partition epoch with an fsynced file write. `PromoteToLeader` did that on every
5-second leadership reconcile, for every locally led partition, while holding
the partition manager's write lock. Every publish takes that lock for reading,
so each tick stalled all publishes on the node for the duration of about ten
fsyncs. The write is now skipped when the epoch is unchanged.

Two older stalls on the same path, present in v0.5.0 as well, were removed at
the same time: the periodic WAL flush did its disk I/O while holding the WAL
append lock, and each flush re-walked the whole mapped segment instead of the
bytes written since the previous flush.

**Measurement.** The host had 2–6 GiB of free memory and other workloads during
this session, so runs were kept to 1.2–1.8 million events (4 publishers per
node with 100,000 events each unless noted, 32 partitions, 64 MiB segments,
batch 4,000, dedup bypassed) and run with the harness's memory guard and
`-RemoveData`. Block profiles (`--pprof-addr`) give the direct evidence: goroutine
time publish handlers spent blocked on the partition-manager lock, summed over
the three nodes.

| Build | Batch A (1.2M events) | Batch B (1.2M) | Batch C (1.8M) |
|---|---:|---:|---:|
| Branch head before the fixes | 599,018 / 585,141 / 602,217 | 476,561 / 466,365 | 658,743 / 666,085 / 717,273 |
| With the fixes | 736,857 / 723,716 | 631,854 / 659,751 | 823,516 / 821,356 / 646,450 |

Values are accepted events per second. Each batch interleaved the two builds;
the batches ran under different background load, so compare within a batch.
"With the fixes" is the working tree at the time of the batch; the three
publish-path fixes above were in place before batch A.
Batch C used 12 publishers per node with 50,000 events each. In batch A the
profiled time blocked on the manager lock was 2.7 s, 3.0 s, and 3.1 s per run
before the fixes and 0.03 s and 0.04 s with them.

These are short runs and are not comparable with the 4.8-million-event medians
above; the matched comparison has not been repeated at that size. Run-to-run
variance on this host is large: one further 1.2M run of each build was several
times slower because file syncs on the shared SSD took far longer than usual
(151,058 events/s before the fixes, 271,279 with them), and one batch C run
with the fixes was slower than two of the runs without them.

RF=3 with `min-insync-replicas=2` and dedup enabled could only be run at
240,000 events (4 partitions, 32 MiB segments); four attempts at 480,000
events were stopped by the memory guard on both builds. Before the fixes:
138,090, 131,594, and 145,413 events/s. With them: 164,630 and 187,973. Every
run reported zero publish errors, and all `cronos_replication_lag` gauges were
zero when scraped, at most three seconds after the load ended. The sample is
too small to size a deployment from.

## Second follow-up — 2026-10-06

**`batch` fsync.** In `batch` mode every append flushed and synced the segment
while holding the WAL lock, so concurrent writers queued behind each other's
disk writes. The sync now happens after the lock is released, and writers that
append while one is in flight share the next. `BenchmarkWAL_AppendBatch_Matrix`
with 16 writers and 256-byte payloads, time per batch, four runs each:

| Events per batch | Before | After |
|---:|---:|---:|
| 10 | 3.2-6.1 ms | 0.44-0.51 ms |
| 100 | 3.2-3.9 ms | 0.68-1.10 ms |
| 1,000 | 6.0-6.7 ms | 2.1-2.3 ms (one run 13.2 ms) |

A writer still returns only after a sync that began after its append.

**Memory per partition.** An empty partition held 146 MB of heap at default
settings: a bloom filter sized for 100 million message IDs and a
one-million-entry consumer queue, both allocated at creation. They are now
allocated on first use, leaving 12 MB. The filter is still built, at full size,
by the first deduplicated publish to the partition.

**Cluster formation.** Leadership now comes only from Raft-committed
assignments, and partitions that never had a leader are assigned five seconds
after membership last changed (since 2026-10-07: as soon as
`--cluster-expected-nodes` nodes are up, with the wait as the fallback).
Without that wait the first node of a new
cluster led every partition and handed most of them over as the others
joined, refusing publishes for those partitions meanwhile. A node also unloads
a partition once it is neither its leader nor a replica. `/health/ready`
answers 503 until every partition has a leader that has loaded it, and
`scripts/compare-cluster-throughput.ps1` waits for it before starting the
load; a fixed pause was not enough on a slow host.

**Measurement.** Free memory on the host moved between 0.7 and 4.5 GiB during
this session for reasons outside the test, so the comparison was limited to
480,000 events (16 publishers per node with 10,000 events each, 32 partitions,
64 MiB segments, batch 4,000, dedup bypassed, RF=1), interleaving the working
tree with the branch head from before this session's changes:

| Build | Events/s | Server CPU per event | Peak working set, three nodes |
|---|---:|---:|---:|
| Working tree | 629,769 / 711,887 | 10.1 / 10.3 us | 958 / 972 MB |
| Branch head before the fixes | 704,335 / 60,367 | 10.1 / 16.0 us | 1,498 / 1,662 MB |

The slow baseline run coincided with a drop in free memory. Runs this short
(under a second of load) do not include a leadership reconcile tick, so they
neither show nor contradict the gain measured in the first follow-up; they do
show that the later changes cost no CPU on the publish path and that the
servers need about a third less memory. The first run of each newly built
binary was repeatedly several times slower than its second and is not listed.
Larger matched runs were not repeated: two attempts were stopped by the memory
guard.

RF=3 with `min-insync-replicas=2` and dedup enabled was run as a failover check
rather than a throughput test: 60,000 events across four partitions, one node
killed, 40,000 more events to the two survivors, zero publish errors, and
identical logs on the survivors.

## Container release implication

The rc.1 CI built a container and started the production Helm chart in kind,
but it did not publish or scan a release image. There is no Docker CLI on this
Windows host. Do not publish an image from the rc.1 tag: it contains the quorum
acknowledgement bug. A future image candidate needs the corrected source, a
scan of that exact image, and release notes stating the measured profile and
remaining production blockers.

# Three-node throughput validation — 2026-10-16

These measurements compare `v0.6.0` (`f8cc206`) with the `hardening-v0.7.0`
branch at `e08c1bc`, whose server code is that of `d4702bf` and of `v0.7.0`. All runs use three
server processes on one AMD Ryzen 7 6800H laptop (16 logical processors,
15 GB of memory, one Micron 2450 NVMe SSD), Windows/amd64, Go 1.26.9 with cgo
disabled (the Go bloom filter), and 256-byte payloads. The two server builds
ran alternately with the same load generator. The three nodes share one CPU,
memory pool and SSD, and Windows syncs files differently from Linux, so these
are results for this machine, not a capacity to size a deployment on.

## Production settings, every event checked as delivered

`scripts/measure-throughput-native.ps1`, which runs the nodes as Windows
processes:

- RF=3 with `min-insync-replicas=2` and 3 partitions;
- `batch` fsync with a 2 ms flush interval, deduplication on;
- public TLS, replication mTLS, JWT authentication and encryption at rest.

Each node is capped and watched:

- 2 CPUs (`GOMAXPROCS=2`), a 150 MiB Go memory target and a 384 MiB budget
  (`--memory-limit`);
- below-normal priority;
- the supervisor stops the run at 75% of the budget or below 1.5 GiB of free
  host memory.

One consumer group reads every partition with a buffer of 64 deliveries and
one worker per partition. Publishers pause while 20,000 accepted events are
undelivered. A trial is valid only when every accepted message ID was
delivered.

Accepted events per second over 15-second trials, round 1 / round 2. Delivered
per second is within a few percent below the accepted rate except where noted.

| Batch × publishers | v0.6.0 | Current | Publish p99, v0.6.0 | Publish p99, current |
|---|---:|---:|---:|---:|
| 1 × 1 | 40 / 49 | 50 / 46 | 159 / 32 ms | 31 / 35 ms |
| 1 × 4 | 72 / 81 | 82 / 73 | 268 / 115 ms | 106 / 126 ms |
| 100 × 1 | 3,964 / 3,172 | 4,044 / 3,582 | 50 / 107 ms | 47 / 52 ms |
| 100 × 4 | 9,068 / 8,712 | 7,873 / 6,970 | 115 / 118 ms | 225 / 159 ms |
| 100 × 8 | 10,347 / 9,418 | 9,920 / 9,251 | 230 / 214 ms | 204 / 248 ms |
| 1,000 × 1 | 18,061 / 16,582 | 18,599 / 17,525 | 64 / 100 ms | 86 / 61 ms |
| 1,000 × 4 | 12,526 / 12,460 | 13,794 / 10,918 | 250 / 324 ms | 188 / 595 ms |
| 1,000 × 8 | 12,666 / 11,792 | 14,257 / 11,592 | 426 / 540 ms | 502 / 280 ms |
| 1,000 × 16 | 13,158 / 11,126 | 6,598 / 11,060 | 480 / 951 ms | 1,741 / 917 ms |

Every trial delivered every accepted event; no ID was missing.

- **Peak.** Batches of 1,000 from one publisher gave the highest rate for both
  builds, about 17,000–18,600 accepted events per second. A separate run of
  the current build reached 18,256 events/s accepted and 16,199 delivered, and
  19,691 in its repeat before the server refused a batch with "partition 1 is
  at capacity".
- **Ceiling.** In the larger configurations the consumer, not the publish
  path, set the ceiling. Delivery lateness p99 was 2 to 6 seconds, and
  publishers paused for the backlog.
- **More publishers.** Beyond one publisher of large batches, adding publishers
  only grew the backlog.
- **Single-event publishes.** These are bound by the fsync each waits for,
  about 19 ms here.
- **One configuration to watch.** 100 × 4 was slower on the current build in
  both rounds, by about 15%. Runs on this host vary by ±20%, so two rounds do
  not settle it.
- **One outlier.** The current build's first 1,000 × 16 run coincided with host
  CPU at 91%.
- **At-least-once.** One trial redelivered 20 events, which at-least-once
  delivery allows.

Peak memory per node was 213–225 MiB of the 384 MiB budget. Host CPU peaked at
55–91% and free host memory stayed above 6.5 GiB.

## Publish acceptance, as in the 2026-09-29 record

`scripts/compare-cluster-throughput.ps1` with `--dev`, no TLS, authentication
or encryption, `periodic` fsync (which production mode refuses), no CPU caps
and no consumer. It counts accepted publish responses only. A run is stopped
below 1.5 GiB of free host memory.

**RF=1**, dedup bypassed: 32 partitions, 128 MiB segments, 32 publishers per
node, batch 4,000, 4.8 million events:

| Build | Run 1 | Run 2 |
|---|---:|---:|
| v0.6.0 | 705,374 | 357,331 events/s |
| Current | 856,003 | 646,826 events/s |

**RF=3, minISR=2**, dedup enabled: 16 partitions, 32 MiB segments, 16
publishers per node, batch 4,000, 240,000 events:

| Build | Run 1 | Run 2 | Run 3 | Median |
|---|---:|---:|---:|---:|
| v0.6.0 | 165,189 | 190,709 | 199,019 | 190,709 events/s |
| Current | 216,916 | 177,276 | 203,791 | 203,791 events/s |

Every completed run reported zero publish errors.

- **No regression.** Neither profile shows the current build slower.
- **Variance.** The 357,331 run of v0.6.0, with unchanged settings, shows how
  much a single run varies here.
- **1M+.** The figure of earlier versions of the README was not reached by
  either build; the best run was 856,003 events/s.

The same RF=3 profile at the earlier record's 960,000 events (128 MiB
segments) did not complete on either build: all four runs were stopped by the
memory guard. In one v0.6.0 run free memory had reached 795 MiB before the
load stopped. Publishing into three replicas with no consumer and no pause
outgrows this laptop's memory at that size; where the memory goes has not been
diagnosed. The 2026-09-29 record completed 960,000 events with both the Go
(130,128 events/s) and the native Rust/CGO build (110,773), and a later
session of that record was stopped by the guard at 480,000. That is consistent
with this host's free memory varying between sessions. It is not evidence of a
change in either direction.

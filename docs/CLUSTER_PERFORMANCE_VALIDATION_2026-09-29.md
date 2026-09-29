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
The harness does not remove WAL data; allocate enough disk and clear only its
temporary node directories after saving the evidence you need.

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

## Container release implication

The rc.1 CI built a container and started the production Helm chart in kind,
but it did not publish or scan a release image. There is no Docker CLI on this
Windows host. Do not publish an image from the rc.1 tag: it contains the quorum
acknowledgement bug. A future image candidate needs the corrected source, a
scan of that exact image, and release notes stating the measured profile and
remaining production blockers.

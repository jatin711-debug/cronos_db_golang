# Power-loss and network-fault testing

The acceptance suite kills nodes, wipes their disks and cuts the network
between containers, all on one machine. Two release gates need more than
that: proof that a power cut loses no acknowledged event, and a soak on
separate hosts. This page covers the tools for both and what each has shown
so far.

## Power cuts with LazyFS

Killing a process loses nothing it wrote: the kernel still writes its page
cache to disk. A power cut loses whatever was written and not yet synced.
[LazyFS](https://github.com/dsrhaslab/lazyfs) is a FUSE filesystem that keeps
written data in its own cache until it is synced, and drops what was not
synced when it is told to. Jepsen uses it to simulate power loss, and with it
found [lost committed writes in NATS 2.12.1](https://jepsen.io/analyses/nats-2.12.1)
when several nodes lost power at once ([the LazyFS paper](https://www.vldb.org/pvldb/vol17/p3017-ramos.pdf)).

`TestPowerLoss` (`tests/acceptance/powerloss_test.go`) puts each node's data
on a LazyFS mount and runs a workload with `--fsync-mode=every_event`. Each
round kills a node, leaders first, drops its unsynced writes and starts it
again; the last round does that to all three nodes at once. Afterwards every
acknowledged event must have been delivered, none early, and the replicas'
logs must match, as in the other fault tests.

It needs Linux, FUSE that your user can mount (`/dev/fuse`), and a LazyFS
build:

```bash
git clone https://github.com/dsrhaslab/lazyfs && cd lazyfs
(cd libs/libpcache && ./build.sh) && (cd lazyfs && ./build.sh)
cd ../cronos_db_golang
CRONOS_LAZYFS_BIN=$PWD/../lazyfs/lazyfs/build/lazyfs \
  go test -tags acceptance -run '^TestPowerLoss$' -count=1 -v -timeout 40m ./tests/acceptance/
```

`CRONOS_POWERLOSS_ROUNDS` sets the single-node rounds (6). The configuration
the test writes follows LazyFS 0.2's `config/default.toml`. For another
version, pass a configuration with `CRONOS_LAZYFS_CONFIG` and put `{{FIFO}}`
where the fault FIFO path goes.

`CRONOS_POWERLOSS_FSYNC` sets the nodes' `--fsync-mode` (`every_event`).

**Results (2026-11-19, WSL2 Ubuntu 24.04, LazyFS at commit `3edad01`):**

| `--fsync-mode` | Runs | Outcome |
|---|---|---|
| `every_event` | 3 | Passed: every acknowledged event delivered, none twice, replica logs equal |
| `batch` (the production default) | 2 | Passed, as above |
| `periodic` | 1 | Failed, as it should: one acknowledged event was never delivered, 72 were delivered twice, and one node lost a partition. `periodic` acknowledges before it syncs, and production mode refuses it; the run shows that the test detects lost writes |

Each run cut single nodes six times, leaders first, then all three nodes at
once, under a workload of about 40 publishes per second. These are short runs
on one machine: they show that acknowledgement waits for a synced majority,
not how a long run under load behaves.

LazyFS builds without root when `cmake` and the libfuse3 headers are missing:
`cmake` from a Python virtual environment, the headers from
`apt-get download libfuse3-dev` unpacked with `dpkg -x`, named with `CPATH`
and `LIBRARY_PATH`, and `CMAKE_POLICY_VERSION_MINIMUM=3.5` for CMake 4. The
executable needs `LD_LIBRARY_PATH` pointing at `libs/libpcache/build`.
LazyFS itself has had bugs (Jepsen found one that zeroed a file extended
after a dropped write), so a failure needs a look at the node logs before it
is blamed on CronosDB.

A real power cut is still a separate gate. LazyFS models a volatile page
cache, not a disk that reorders or loses its own writes, so a run on the
production disks with their power pulled remains on the
[release requirements](PRODUCTION_RELEASE.md).

## Network faults between hosts

`scripts/soak/netem.sh` adds faults on a Linux host, as root. Latency and
loss use `tc netem` on an interface; partitions drop traffic to and from
chosen peers with `iptables`, in a chain of its own (`CRONOS-NETEM`).
`clear` removes all of it.

```bash
sudo scripts/soak/netem.sh delay eth0 80 20     # 80 ms +/- 20 ms on everything sent
sudo scripts/soak/netem.sh loss eth0 2          # drop 2% of packets sent
sudo scripts/soak/netem.sh cut 10.0.0.1         # this host and 10.0.0.1 stop hearing each other
sudo scripts/soak/netem.sh heal 10.0.0.1
sudo scripts/soak/netem.sh clear eth0
```

Latency and loss apply to all traffic on the interface, clients included. A
cut applies on the host where it is run: run it on both hosts for a symmetric
partition, or on one only for a link that fails in one direction.

## The separate-host soak

The soak (`TestSoak`) runs its three nodes as processes of one machine. It
finds lost or early events, leaks and unbounded growth, but the nodes share
one kernel, disk, clock and power supply. The 1.0 gate asks for 24 to 72
hours on three hosts.

What is ready for it:

- the production chart, or the release binaries, to run one node per host;
- `scripts/soak/netem.sh` for latency, loss and partitions between them;
- `cluster_loadtest.go` (build tag `clustertest`) to drive publishes at the
  nodes' addresses from a fourth machine;
- the metrics and alerts of the chart to watch memory, disk, replication and
  delivery lateness.

What is missing: a driver that keeps the soak's ledger against remote nodes,
so that every acknowledged event is checked for delivery, on time and once
per group, the way `TestSoak` checks it locally. The acceptance harness
starts and stops local processes; driving remote nodes needs it to reach
them over SSH or through the chart, and its ledger to run from the
load-generating machine. Until that exists, a separate-host run shows
throughput, resource use and recovery, but not the absence of lost events.

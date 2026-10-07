# Production release requirements

CronosDB is still undergoing production hardening. The
[audit closure table](PRODUCTION_AUDIT_2026-09-27.md#finding-closure-review)
tracks unresolved guarantees. A green CI run is necessary but does not establish
the complete release acceptance contract.

## Supported scope under validation

The first release targets fixed partitions and telemetry with explicit loss,
retention, and duplicate-delivery limits. Transactions and online partition
splitting are disabled by default. Both require `--dev --experimental-features`;
production configuration rejects that flag. Production also rejects
`--exactly-once-commits`: commit-ID storage is not an end-to-end delivery guarantee.

Change data capture to Kafka and webhooks is supported: it exports accepted
events in log order from each partition's leader, and a partition keeps its
log entries until they have been exported. Replication to other regions is
experimental and part of the same opt-in: a node with `CRONOS_REGIONS` set
refuses to start without `--dev --experimental-features`.

Production mode accepts `--fsync-mode=batch` and `every_event`. In both, a
replica has an entry on disk before it acknowledges it, so a publish
acknowledged with `min-insync-replicas=2` is on two disks. `periodic` is
refused: it acknowledges first and syncs later, and loses acknowledged
publishes when the machines of a quorum lose power together.

## Configuration and installation

Settings are applied in this order: defaults, matching `CRONOS_*` environment
variables, explicit command-line flags. Invalid environment values fail loading.
An explicit empty `--cluster-seeds=` clears environment-provided seeds. Startup
logs effective partition, replication, durability, and address settings without
logging secret values. Invalid authorization files fail before data directories
or listeners are initialized.

The production chart requires these existing secrets:

| Default secret | Required keys |
|---|---|
| `cronos-db-auth` | `jwt-secret`, `policy.json` |
| `cronos-db-tls` | `tls.crt`, `tls.key`, `ca.crt` |
| `cronos-db-replication-tls` | `tls.crt`, `tls.key`, `ca.crt` |
| `cronos-db-encryption` | `master.key` |

The policy file is a JSON object keyed directly by JWT subject, for example
`{"operator":{"admin":true}}`. Supply topic-specific permissions for application
subjects. The encryption key must meet `LoadMasterKey`'s 32-byte requirement.
The chart's non-root init container copies the projected key into a memory volume
with mode `0600`; the application reads that private copy. Keep the default
user/group/fsGroup relationship or adapt key staging to your security policy.

Use certificates whose SANs cover the advertised pod DNS addresses, including
`<pod>.<headless-service>.<namespace>.svc`. Replication certificates must support
client and server authentication. Public client certificates depend on the
configured public TLS policy.

The `cronos-db-replication-tls` secret secures all traffic between nodes:
replication, membership and Raft. Each of the three ports reads nothing from a
caller that has not presented a certificate signed by the CA in `ca.crt`, and a
node checks that the certificate of the node it calls is valid for the address
it called. Certificates are read when a node starts; `ca.crt` may hold more
than one CA certificate.

A cluster that runs 0.6.0-rc.2 or earlier with replication TLS cannot be
upgraded one pod at a time: those versions speak plain TCP on the membership
and Raft ports, and a node of this version does not talk to them. Stop all its
nodes and start them on the new version together. The data stays.

The chart starts pods in parallel, and every pod names every pod as a seed.
Ordinal zero is the one that creates the cluster (`--cluster-bootstrap`), and
it does so only when it has no Raft state and no other pod belongs to a
cluster. Raft state is on the data volume under `raft/`.

A pod whose volume is replaced comes back empty, finds the cluster through the
other pods and is filled from it; that holds for ordinal zero too. Replace one
volume at a time and wait for the pod to be ready: with two of three volumes
gone, events that were acknowledged by two replicas can be on neither of the
two that are left. Production mode refuses to start a node without
`--cluster-seeds`, because such a node creates a cluster whenever it has no
state.

The HTTP port (health, metrics, dashboard and the admin API) is plain HTTP. The
admin API checks the bearer token, but the token and the answers cross the
network unencrypted. Keep the port on a trusted private network and terminate
TLS in front of it before exposing the dashboard or the admin API.

The image builds dashboard assets from the npm lockfile and the Rust library from
the Cargo lockfile. Its Go builder matches the module's minimum version. Pin the
resulting application image digest for a release deployment.

## Checks

`.github/workflows/ci.yml` defines Linux cgo/race tests, Go vet/build/formatting,
Rust tests, dashboard tests/build, Go/npm vulnerability checks, Helm assertions,
a scan of the built image for known vulnerabilities, a fault campaign against
three server processes, and a disposable kind cluster running the production
chart with security enabled. It runs for pull requests and for pushes to
`main`. Work on the development branch is not checked as it is pushed; a
commit there is checked on request with
`gh workflow run ci.yml --ref developement`, which is done for every commit a
release is cut from.
Make these jobs required in repository branch protection before treating them as
enforced merge gates. A vulnerability finding fails its job; it is not silently
waived.

Run chart assertions locally with `python scripts/check-production-chart.py`
(Helm and PyYAML required). `scripts/production-chart-smoke.sh` requires the
disposable `kind-cronos-ci` context and an already-loaded `cronos-db:ci` image. It
creates ephemeral test credentials, installs three production-mode pods, and
checks readiness, embedded dashboard assets, key permissions, and rejection of
unauthenticated HTTP admin requests. It then restarts the pods one at a time,
and starts the cluster again with the volume of ordinal zero emptied, checking
that the pod joins the cluster instead of creating one. It sends no events: it
is not a proof of quorum acceptance, failover, or restore. Tooling follows the
[kind quick-start workflow](https://kind.sigs.k8s.io/docs/user/quick-start/).

The `fault-campaign` job runs `tests/acceptance`: three server processes on the
runner, producers and a two-member consumer group working through the client
library, and a sequence of faults (a leader killed, a follower killed, a leader
frozen and released, each node restarted, a replica replaced with an empty one
and killed while it is refilled, the whole cluster killed). It fails if an
acknowledged event was not delivered, if one arrived before its time, or if
the replicas of a partition do not end with the same log. Its nodes speak
mutual TLS with each other, as production nodes do. Run it locally with
`go test -tags acceptance -count=1 -timeout 20m ./tests/acceptance/`; it needs
about 500 MB of memory. It stops and kills processes, and it publishes about
60 events a second, so it says nothing about behavior under load.

Processes on one machine cannot be cut off from one another, so the network
is failed in a test of its own. `TestNetworkPartitions` runs three containers
of the image on a network of their own and drops what chosen nodes send each
other, without a word, as a failed link does, while all three keep running
and applications still reach every one of them: the leader of a partition on
its own, one link down between two nodes that both still reach the third,
each node on its own in turn, and every link down. The same checks apply as
for the other faults. It runs in the `production-chart-startup` job, which
has built the image, and locally with
`CRONOS_IMAGE=<image> go test -tags acceptance -run TestNetworkPartitions ./tests/acceptance/`.

The same job restores a whole cluster from backups
(`TestClusterRestoredFromBackups`): three nodes back up while publishes
arrive, all three are destroyed, each is restored from its own backup, and
what was acknowledged before the backups is there and is delivered at its
time. Backups go to `backups` under the data directory unless `--backup-dir`
names another place. A backup on the volume it copies does not survive the
loss of that volume, and the chart mounts no other: copy backups off the
node, or mount a second volume and point `--backup-dir` at it.

`cronos-admin` is in the image at `/app/cronos-admin`. `restore` and
`check-log` work on the files of a stopped node: scale the StatefulSet down
and run them from a pod of the same image that mounts the node's volume.

## Memory and overload

A node keeps in memory, payload included, every event that is due within the
hot window (`--hot-window-minutes`, 60 by default) and every event that is due
and not yet delivered. Events due later are kept on disk as references. So the
memory a partition's leader needs grows with what producers schedule for the
next hour and with how far consumers are behind.

Publishes are refused, with `ResourceExhausted`, while the process holds
`--max-memory-percent` (80) of its memory limit. The limit is the container's
when there is one, otherwise the machine's memory, or `--memory-limit`. The
Go client reports the refusal as an error of kind `overloaded` and does not
retry it: the application should send less. A node at that point keeps
delivering, and takes publishes again when consumers have worked the backlog
off. `cronos_memory_held_bytes` and `cronos_memory_limit_bytes` show where a
node stands, and the chart alerts when a node has been refusing for two
minutes. The `fault-campaign` job tests this (`TestOverloadIsRefusedAndSurvived`):
producers flood a cluster that has a small limit and no consumer; the nodes
refuse, none dies, and everything that was acknowledged is delivered once a
consumer starts.

What this does not bound: a node that restarts reads its undelivered backlog
back into memory whatever its size, because the refusal applies to publishes
and not to recovery. A node whose backlog alone exceeds its memory limit is
killed on every start. Give nodes memory for the backlog you allow, alert on
the two gauges, and keep consumers running. The queues themselves are still
bounded by event count (`--max-ready-queue`, `--max-timing-wheel-size`,
`--max-in-flight`), not by bytes. Sustained load at production sizes has not
been measured.

## What a release promises, and what that rests on

Each promise below is checked by the test named with it, in CI: for every pull
request, for every push to `main`, and for the commit of a release. The
tests run three server processes on one machine at about 60 small events a
second; the last column says what they do not show.

| Promise | Checked by | Not shown |
|---|---|---|
| A publish that was acknowledged survives the loss of any one node (replication factor 3, two in-sync replicas, fsync `batch` or `every_event`) | `TestFaultCampaign`: a leader killed, a follower killed, a leader frozen and released, every node restarted, a node replaced with an empty one and killed while it is refilled, the whole cluster killed | Two nodes' disks lost together; loss of power on a real machine (the tests kill processes) |
| The same holds when the network between the nodes fails while all of them keep running, and a node that is cut off acknowledges nothing | `TestNetworkPartitions`: links cut between three containers of the image: a leader alone, one link down, each node alone in turn, every link down | Slow or lossy links, as opposed to dead ones; outages longer than a minute; more than one machine |
| Every acknowledged event is delivered at least once, and never before its time | The ledger of the same test, and of the restore and overload tests | Exactly once: after a failover some events are delivered again |
| The replicas of a partition hold the same log | `TestFaultCampaign` compares them entry for entry | |
| A partition takes publishes again within 90 seconds of its leader's death | The bound the campaign enforces | Typical times; behavior under load |
| Any node, the first included, can be replaced with an empty one | `TestFaultCampaign`, `TestFirstNodeReplaced`, the kind job on the chart | Two at once |
| A cluster that is lost entirely comes back from its nodes' backups with everything acknowledged before the earliest of them | `TestClusterRestoredFromBackups` | Restore to a point between backups; getting backups off the node, which is the operator's to arrange |
| Producers that outrun the cluster are refused, and no node dies of it | `TestOverloadIsRefusedAndSurvived` | A backlog larger than a node's memory at restart; load at production sizes |
| Traffic between nodes is encrypted and only nodes that hold the cluster's certificate take part | `TestTransportTLS_*`, the kind job | The HTTP port; replacing certificates without a restart |
| One tenant cannot read, replay or acknowledge another's events | `TestIsolation_*` | |

No throughput, payload-size or latency figure is promised. None has been
measured on production hardware.

## Before a final release

Release candidates are cut from the development branch when CI is green on
the commit. The current one is `v0.6.0-rc.3`. It is published as the latest
release and as the `latest` image, in place of v0.5.0 and v0.4.0, which have
defects it fixes; it is a release candidate all the same. A final `v0.6.0`
still needs:

- The five CI jobs made required in branch protection. Today a red run does
  not stop a merge:
  `gh api -X PUT repos/<owner>/<repo>/branches/main/protection` with
  `required_status_checks.contexts` set to `go`, `fault-campaign`,
  `dashboard-and-chart`, `go-security` and `production-chart-startup`.
- A run on more than one machine. Everything above was shown on one host,
  with processes or with containers; links were cut between containers.
- A replica refilled from a partition larger than 1 GiB. The transfer has no
  size limit and resumes file by file, and has a ten minute deadline; it has
  only been run on megabytes.
- Throughput and latency measured on the hardware a deployment will use, with
  the memory that the backlog it allows needs.
- A scan of the image that is published. CI scans the image it builds from
  the same Dockerfile, not the published one.

Known limits of what is released are in the
[audit](PRODUCTION_AUDIT_2026-09-27.md): log entries are removed from the
start of a log only, so one event that cannot go yet keeps every later
segment; a restart reads the undelivered backlog into memory; a consumer
group's finished work in a partition shared by several topics is carried over
a failover only in part, and may be delivered again.

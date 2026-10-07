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

The chart starts pods in parallel. Ordinal zero bootstraps a new Raft cluster;
other pods use the configured seed list. Existing Raft state remains on the data
volume under `raft/`. Do not replace or wipe these volumes as a bootstrap recovery
procedure. Membership and ownership recovery after node replacement remain audit
release blockers.

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
chart with security enabled.
Make these jobs required in repository branch protection before treating them as
enforced merge gates. A vulnerability finding fails its job; it is not silently
waived.

Run chart assertions locally with `python scripts/check-production-chart.py`
(Helm and PyYAML required). `scripts/production-chart-smoke.sh` requires the
disposable `kind-cronos-ci` context and an already-loaded `cronos-db:ci` image. It
creates ephemeral test credentials, installs three production-mode pods, and
checks readiness, embedded dashboard assets, key permissions, and rejection of
unauthenticated HTTP admin requests. It is a startup smoke test, not a proof of
quorum acceptance, failover, or restore. Tooling follows the
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
about 500 MB of memory. It stops and kills processes. It does not cut network
links, and it publishes about 60 events a second, so it says nothing about
network partitions or behavior under load.

Before release, also require independent backup restoration, bounded overload
behavior, and a scan of the image that is actually published: CI scans the
image it builds from the same Dockerfile, not the published one. Set and measure explicit throughput, payload,
retention, delay, and crash-loss targets. See the audit acceptance contract.

# Admin Dashboard Architecture

## Purpose

The admin dashboard is the operator-facing web UI for CronosDB AdminService. It
surfaces cluster topology, partition health, replication lag, consumer-group
lag, retention, compaction, schemas, tenants, and rebalance actions through a
React 19 SPA served by the same Go binary that runs the public gRPC API.

The dashboard talks to the process over HTTP JSON (`/api/admin/*`), which is a
thin proxy over the same `AdminServiceHandler` used by gRPC — no separate
business logic path.

## Key Files

- [web/dashboard/src/main.tsx](../../../web/dashboard/src/main.tsx),
  [App.tsx](../../../web/dashboard/src/App.tsx) — React 19 entry and nav shell
  (basename `/ui`).
- [web/dashboard/src/views/](../../../web/dashboard/src/views/) — views for
  home, cluster, partitions, replication, consumers, schemas, tenants,
  operations, login.
- [web/dashboard/src/lib/api.ts](../../../web/dashboard/src/lib/api.ts) —
  live `fetch()` client + React hooks; JWT stored in `localStorage`.
- [web/dashboard/src/components/](../../../web/dashboard/src/components/) —
  theme, toasts, mobile nav, error boundary, shadcn-style UI primitives.
- [web/dashboard/vite.config.ts](../../../web/dashboard/vite.config.ts) —
  Vite 7 build; emits `web/dashboard/dist/` with content-hashed assets.
- [internal/api/web_handler.go](../../../internal/api/web_handler.go) —
  embeds SPA via `//go:embed web/dist/*`; serves `/ui/` with SPA fallback to
  `index.html`; exposes JWT-gated `/api/admin/*` JSON proxy.
- [internal/api/web/dist/](../../../internal/api/web/dist/) — the embedded build:
  checked-in assets that `make dashboard` / `make build` replace (the
  `README.txt` there is a placeholder that is kept).
- [cmd/api/main.go](../../../cmd/api/main.go) — mounts `WebHandler` on the
  HTTP mux next to health and metrics.
- [pkg/types/admin.pb.go](../../../pkg/types/admin.pb.go) — protobuf JSON tags
  define the wire field names.
- [internal/auth/auth.go](../../../internal/auth/auth.go) — `ParseToken` +
  `CheckAdminPermission` shared with gRPC admin paths.

## Main Flow

```
Browser (SPA)
   │  GET /ui/…                 (assets + SPA routes → index.html fallback)
   │  GET /api/admin/topology   (Authorization: Bearer <jwt> when auth on)
   ▼
Go binary (cronos-api)  HTTP :8080
   │  /ui/     → embedded dist (spaFileServer)
   │  /api/admin/* → authMiddleware → AdminServiceHandler.<RPC>
   │                 → json.Marshal(proto response)
   ▼
Same handler code as gRPC AdminService
```

## Endpoints

Every RPC below is an `AdminService` RPC on the public gRPC port as well, and needs global `"admin": true` when auth is on. The three actions marked `POST` refuse other methods; the views do not check the method.

| HTTP path | Admin RPC / action |
|-----------|--------------------|
| `/api/admin/topology` | `GetClusterTopology`: nodes, partition assignments and epochs, with in-sync replicas and offsets asked of each partition's leader. |
| `/api/admin/partition-health?partition_id=N` | `GetPartitionHealth`: for a partition loaded on the node that answers, high watermark, segment count, active timers, ready events, cold-store entries and the last replay error. `partition_id` is required, and 0 is partition 0. `disk_usage_bytes` and the first-offset and segment timestamps are not filled in. |
| `/api/admin/replication-lag[?partition_id=N]` | `GetReplicationLag`: leader high watermark and each follower's offset, lag in events and in-sync flag, for partitions **this node leads**; a partition it does not lead, or an RF=1 partition, has no followers listed. Without `partition_id` it aggregates every partition the node leads (its `leader_high_watermark` is then a sum). `lag_seconds` is always 0. |
| `/api/admin/consumer-groups` | `ListConsumerGroups`: groups known to this node (see [consumer.md](consumer.md)). |
| `/api/admin/consumer-group-lag?group_id=G&partition_id=N` | `GetConsumerGroupLag`: committed offset, high watermark and lag; `-1` values when the partition is not local or nothing was committed. |
| `/api/admin/schemas` | `ListSchemas`: topic, latest version and compatibility mode. See [schema.md](schema.md). |
| `/api/admin/schema?topic=T[&version=V]` | `GetSchema`: one version (0 or absent is the latest); `404` when missing. |
| `/api/admin/tenant-usage[?tenant_id=T]` | `GetTenantUsage`: in-flight events, storage bytes, configured publish rate. An empty tenant aggregates usage over all known tenants and reports no limits. See [tenant.md](tenant.md). |
| `POST /api/admin/retention/run` | `RunRetention` (`partition_id`, `max_age_hours`, `max_size_bytes`): `partition_id` 0 or absent means all local partitions. |
| `POST /api/admin/compaction/run` | `RunCompaction`: `partition_id` is required; 0 means all local partitions. |
| `POST /api/admin/cluster/rebalance` | `TriggerRebalance` returns gRPC `Unimplemented`, so HTTP `501` (since v0.11.0; it used to return success and move nothing). The Operations view shows the action disabled. See [cluster.md](cluster.md) and [ARCHITECTURE known limitations](../../../ARCHITECTURE.md#known-limitations). |

`partition_id=0` names partition 0 on the HTTP API for `partition-health`,
`replication-lag` and `consumer-group-lag` (`parsePartitionID` and
`replicationLag` in `web_handler.go` / `admin_handler.go`). The gRPC
`GetReplicationLag` keeps 0 as "all partitions", because the field cannot tell
0 from unset, and retention and compaction take 0 as "all local partitions" on
both surfaces. The Replication view therefore no longer titles partition 0
"All Partitions", and says that only a
partition's leader reports follower offsets. The per-node leader count is
labelled "Led by this node".

## Production Decisions

- **Single binary, single HTTP listener.** SPA and JSON proxy share `:8080`
  with `/health` and `/metrics`.
- **`//go:embed` keeps the binary self-contained.** `make build` stages
  `web/dashboard/dist` → `internal/api/web/dist` before compile.
- **SPA client-route fallback.** Unmatched `/ui/*` paths return `index.html`
  so React Router deep links work.
- **Auth mirrors gRPC.** Bearer JWT, same secret/keys/policy; admin requires
  `Subject.Admin=true`. In `--dev` (auth off) middleware is a pass-through.
- **gRPC status → HTTP.** NotFound→404, PermissionDenied→403, etc.
- **Schemas are read-only here; tenant limits come from a file.** The schema
  views list what the node loaded from `<data-dir>/schemas` at start, normally
  nothing ([schema.md](schema.md)). Without `--tenant-limits-file` nothing calls
  `SetLimits`, so the tenant view reports zero with `limits_configured: false`;
  with it, listed subjects show their `max_events_per_second` as
  `publish_rate_per_sec`. In-flight and storage limits cannot be configured.
  See [tenant.md](tenant.md).
- **The topology asks the leaders.** Which replicas of a partition are in
  sync, and their offsets, are known to the node that leads it alone.
  `GetClusterTopology` asks every node that is alive for the partitions it
  leads (`ReplicationService.LeaderReport` on the port the nodes use with
  each other, with the replication certificates) and waits at most two
  seconds. A partition whose leader did not answer comes with
  `leader_reported: false` and no in-sync replicas, and the view says so.
- **Zero values are not in the JSON.** `json.Marshal` of a proto response
  leaves out empty and zero fields: partition 0 has no `partition_id`, and an
  unreported partition has no `isr`. The SPA's types mark such fields
  optional.
- **`make build` depends on dashboard stage** so release binaries include UI.
- Lifecycle state/cancel/reschedule operations are gRPC EventService methods;
  this dashboard has no controls for them. See [api.md](api.md).

## Debug Pointers

- **404 on `/ui/`:** embed empty. The repository checks in a build under
  `internal/api/web/dist`, which a plain `go build` embeds as it is; it can
  trail the sources. `make dashboard && make build` stages a fresh one, as does
  the container build.
- **401 on JSON:** missing/invalid JWT or subject lacks `admin: true`.
- **CORS in vite dev:** SPA and API must be same origin for production embed;
  proxy or point fetch base URL when developing SPA against a remote node.
- **Stale UI after rebuild:** re-stage dist and restart the binary (embed is
  compile-time).

## Related

- [api.md](api.md) — public vs internal listeners and AdminService
- [auth.md](auth.md) — JWT and RBAC
- [ARCHITECTURE.md](../../../ARCHITECTURE.md)

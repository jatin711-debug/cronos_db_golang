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
- [internal/api/web/dist/README.txt](../../../internal/api/web/dist/README.txt) —
  embed placeholder until `make dashboard` / `make build` stages real assets.
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

| HTTP path | Admin RPC / action |
|-----------|--------------------|
| `/api/admin/topology` | GetClusterTopology |
| `/api/admin/partition-health` | GetPartitionHealth |
| `/api/admin/replication-lag` | GetReplicationLag |
| `/api/admin/consumer-groups` | ListConsumerGroups |
| `/api/admin/consumer-group-lag` | GetConsumerGroupLag |
| `/api/admin/schemas` | ListSchemas |
| `/api/admin/schema` | GetSchema |
| `/api/admin/tenant-usage` | GetTenantUsage |
| `/api/admin/retention/run` | RunRetention |
| `/api/admin/compaction/run` | RunCompaction |
| `/api/admin/cluster/rebalance` | **TriggerRebalance soft-stub** — RPC exists, RBAC applies; returns success + message that rebalance is membership-driven (`PartitionsMoved=0`). See [cluster.md](cluster.md) and [ARCHITECTURE known limitations](../../../ARCHITECTURE.md#known-limitations). |

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
- **Schemas and tenant usage are live** when schema registry / tenant
  accountant are initialized at process start (they are in `cmd/api/main.go`).
- **`make build` depends on dashboard stage** so release binaries include UI.

## Debug Pointers

- **404 on `/ui/`:** embed empty — run `make dashboard && make build` and restart.
- **401 on JSON:** missing/invalid JWT or subject lacks `admin: true`.
- **CORS in vite dev:** SPA and API must be same origin for production embed;
  proxy or point fetch base URL when developing SPA against a remote node.
- **Stale UI after rebuild:** re-stage dist and restart the binary (embed is
  compile-time).

## Related

- [api.md](api.md) — public vs internal listeners and AdminService
- [auth.md](auth.md) — JWT and RBAC
- [ARCHITECTURE.md](../../../ARCHITECTURE.md)

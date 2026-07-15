# Admin Dashboard Architecture

## Purpose

The admin dashboard is the operator-facing web UI for the CronosDB
AdminService. It surfaces cluster topology, partition health,
replication lag, consumer-group lag, retention, compaction,
schema, tenant, and rebalance information through a React 19 single-page
application served by the same Go binary that runs the public gRPC
API. The dashboard reads exclusively from the AdminService — it is a
thin client, and the JSON wire format mirrors the gRPC protobuf so
backend and frontend evolve together.

## Key Files

- [web/dashboard/src/main.tsx](../../../web/dashboard/src/main.tsx),
  [App.tsx](../../../web/dashboard/src/App.tsx) — React 19 entry point
  and top-level nav.
- [web/dashboard/src/views/](../../../web/dashboard/src/views/) — one
  component per AdminService RPC group.
- [web/dashboard/src/lib/api-mock.ts](../../../web/dashboard/src/lib/api-mock.ts) —
  typed `useAdmin()` hook returning mock data; swap to `fetch()`
  against the JSON proxy in a future step.
- [web/dashboard/src/components/ui/](../../../web/dashboard/src/components/ui/) —
  shadcn/ui primitives (Button, Card, Badge).
- [web/dashboard/vite.config.ts](../../../web/dashboard/vite.config.ts) —
  Vite 7 build config; emits `web/dashboard/dist/` with
  content-hashed assets.
- [internal/api/web_handler.go](../../../internal/api/web_handler.go) —
  Go HTTP handler that:
    - Embeds the SPA via `//go:embed web/dist/*`.
    - Exposes the JSON proxy at `/api/admin/*` (one path per
      AdminService RPC).
    - Wraps every JSON path in an auth middleware that reuses
      `auth.ParseToken` + `auth.CheckAdminPermission`.
- [internal/api/web/dist/README.txt](../../../internal/api/web/dist/README.txt) —
  placeholder so the `//go:embed` directive compiles before the SPA
  has been built. Replaced in place by the Makefile's
  `STAGE_DASHBOARD_DIST_CMD` when `make build` runs.
- [cmd/api/main.go](../../../cmd/api/main.go) — wires `WebHandler`
  into the existing `*http.ServeMux` (one block, alongside the
  health checker and `/metrics`).
- [pkg/types/admin.pb.go](../../../pkg/types/admin.pb.go) — the
  gRPC-generated types whose `json:"snake_case,omitempty"` tags
  define the dashboard's wire format.
- [internal/auth/auth.go](../../../internal/auth/auth.go) — exports
  `auth.ParseToken` (added in step 5) which the HTTP middleware
  uses to verify Bearer tokens; same secret/keys as the gRPC
  interceptor.

## Main Flow

```
Browser (SPA)
   │  GET /ui/                 (assets + index.html)
   │  GET /api/admin/topology  (Authorization: Bearer <jwt>)
   ▼
Go binary (cronos-api)
   │  /ui/     → embedded dist (FileServer, strip /ui/)
   │  /api/admin/* → WebHandler.authMiddleware →
   │                 WebHandler.apiMux → AdminServiceHandler.<RPC> →
   │                 json.Marshal(proto response) → 200 application/json
   ▼
AdminService gRPC handler (re-used, no business-logic change)
```

The JSON wire format is the gRPC protobuf's JSON form. Snake-case
field names match `web/dashboard/src/lib/api-mock.ts` exactly, so
swapping the mock hook for `fetch('/api/admin/topology')` is a
one-line change with no view edits.

## Production Decisions

- **Single binary, single listener.** The SPA is served from the
  same `:8080` HTTP listener that already serves `/health` and
  `/metrics`. No new port, no new process. The `*http.ServeMux` is
  shared.
- **`//go:embed` keeps the binary self-contained.** The
  `STAGE_DASHBOARD_DIST_CMD` Makefile rule copies the SPA's
  `dist/` into `internal/api/web/dist/` before `go build`, so the
  embed directive `web/dist/*` resolves at compile time. The
  binary contains the SPA; distribution is one file.
- **No SPA fallback for client-side routes (yet).** Requests to
  `/ui/some/path` that don't match an asset return 404. Adding
  SPA fallback (return `index.html` for any unmatched `/ui/*`
  path) is a known gap; deferred to a follow-up step.
- **Auth mirrors the gRPC service.** Bearer token in
  `Authorization: Bearer <jwt>`, parsed by `auth.ParseToken`,
  claims stashed in `r.Context()`, and gated by
  `auth.CheckAdminPermission(Subject.Admin=true)`. Same policy
  file, same secret/keys, same AllowAll semantics in dev mode.
- **gRPC status codes map to HTTP.** The proxy uses a
  `grpcCodeToHTTP` table (NotFound→404, PermissionDenied→403,
  FailedPrecondition→412, etc.) so the dashboard and the CLI see
  consistent error semantics.
- **All 11 AdminService RPCs are exposed**, including the ones
  whose handler returns a documented "not yet wired" response
  (ListSchemas, GetSchema, GetTenantUsage, TriggerRebalance).
  This keeps the wire surface stable so step 5+ can add the real
  implementations without changing the dashboard.
- **`make build` depends on `make dashboard`.** The `build:`
  target chains `dashboard rust-dedup ensure-build-dir`, so a
  `go build` of the project always produces a binary with the
  current SPA. Go-only builds via `make test-unit` are unaffected.
- **Defer the SPA mock-data swap.** The dashboard renders mock
  data via `useAdmin()`. A future step will replace the hook
  body with a `fetch('/api/admin/topology')` call. The TS types
  already match the wire format, so no view changes are required.

## Debug Pointers

- **Dashboard doesn't load in the browser.** Open
  `http://localhost:8080/ui/` directly; if it 404s, the embed
  is empty — run `make dashboard && make build`. Check the
  binary's logs for the "dashboard dist not embedded" warning
  emitted at startup.
- **JSON proxy returns 401 unexpectedly.** Confirm
  `Authorization: Bearer <jwt>` is set, the JWT is signed with
  the same secret/key as the gRPC service, and the subject has
  `admin: true` in the policy file. The error body is the
  gRPC `PermissionDenied` message verbatim, which is helpful for
  debugging but does leak some auth detail; acceptable for an
  admin-only endpoint.
- **JSON proxy returns 500 with an error body.** Check
  `cmd/api/main.go` startup logs for handler initialization
  errors (e.g. nil AdminServiceHandler, missing cluster manager).
  The proxy does not retry; the underlying gRPC handler is the
  source of truth.
- **CORS errors in the browser console.** The dashboard is
  served from the same origin as the JSON proxy (`:8080`), so
  no CORS preflight is required. If the dashboard is opened
  via a different origin (e.g. `vite dev` on `:5173` while the
  API runs on `:8080`), the browser will block the fetch. For
  local development, set the dashboard's `base: '/'` (or use
  the production build).
- **Stale SPA after rebuild.** `make build` clears and re-stages
  `internal/api/web/dist/` from `web/dashboard/dist/`; restart
  the binary to pick up the new embed. The Go build cache
  (`~/.cache/go-build`) does not affect embed content.

## Related Diagrams

- The dashboard flows are not currently captured in Mermaid. The
  high-level architecture diagram in [ARCHITECTURE.md](../../../../ARCHITECTURE.md)
  shows the AdminService as a separate node; a future
  diagram can show the JSON proxy + SPA wiring.

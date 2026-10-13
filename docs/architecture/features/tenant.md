# Tenant Accounting Architecture

## Purpose

The tenant module contains per-tenant accounting and quota code (publish rate,
in-flight events, storage bytes).

**Limits are not enforced.** `Accountant.SetLimits` has no caller outside
tests, and no flag, environment variable or RPC sets a limit. Without a
configured limit the publish path skips the accountant entirely
(`HasConfiguredLimits` is false), so no publish is checked or counted, and
`RecordDelivery` only adjusts usage entries that already exist, of which there
are none. `AdminService.GetTenantUsage` therefore reports zero usage and
`limits_configured: false`.

## Key Files

- [internal/tenant/tenant.go](../../../internal/tenant/tenant.go)
- [internal/api/handlers.go](../../../internal/api/handlers.go)
- [internal/partition/manager.go](../../../internal/partition/manager.go)

## Main Flow

1. Startup creates a tenant accountant and hands it to the event handler, the
   partition manager (delivery callbacks) and the admin handler.
2. If any limit were configured, the publish handlers would take the tenant
   from the JWT subject (`"default"` without claims), reserve quota with
   `ReservePublishBatch`, refuse with `ResourceExhausted` when it is exceeded,
   and tag the event with `tenant_id`.
3. Delivery completion calls `RecordDelivery` for the event's tenant.

As shipped, step 2 never runs; see Purpose.

## Design Notes

- The accountant is built so that it costs nothing on the hot path when no
  limits are configured.
- Delivery callbacks are wired through the partition manager, including
  partitions created later.
- Tenant identity falls back to `"default"` when a request carries no claims.

## Debug Pointers

- Tenant extraction path: [internal/api/handlers.go](../../../internal/api/handlers.go)
- Accounting state transitions: [internal/tenant/tenant.go](../../../internal/tenant/tenant.go)

## Related Diagrams

- [System overview](../README.md#system-overview)
- [Observability feedback loop](slo.md#observability-feedback-loop)

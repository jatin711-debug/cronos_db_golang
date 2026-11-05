# Tenant Accounting Architecture

## Purpose

The tenant module contains per-tenant accounting and quota code (publish rate,
in-flight events, storage bytes).

**Per-subject publish rates are configurable.** `--tenant-limits-file` loads a
persistent JSON map of JWT subjects to `max_events_per_second`. Authentication
is required. Limits and burst buckets are per node; unlisted subjects are unlimited.
Restart loads changes and resets buckets. See [operator configuration](../../PRODUCTION_VALIDATION.md).

Storage and in-flight quotas are not exposed: usage counters are process-local,
not recovered durable usage or cluster-wide totals. Admin usage values must not be
used as a storage inventory. Without a limits file, accounting remains disabled.

## Key Files

- [internal/tenant/tenant.go](../../../internal/tenant/tenant.go)
- [internal/api/handlers.go](../../../internal/api/handlers.go)
- [internal/partition/manager.go](../../../internal/partition/manager.go)

## Main Flow

1. Startup creates a tenant accountant and hands it to the event handler, the
   partition manager (delivery callbacks) and the admin handler.
2. With a limits file configured, the publish handlers take the tenant
   from the JWT subject (`"default"` without claims), reserve quota with
   `ReservePublishBatch`, refuse with `ResourceExhausted` when it is exceeded,
   and tag the event with `tenant_id`.
3. Delivery completion calls `RecordDelivery` for the event's tenant.

The configuration loader accepts only rate limits; unsupported quota fields fail startup.

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

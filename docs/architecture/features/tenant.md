# Tenant Accounting Architecture

## Purpose

Tenant module enables per-tenant resource accounting and rate control hooks for multi-tenant safety.

## Key Files

- [internal/tenant/tenant.go](../../../internal/tenant/tenant.go)
- [internal/api/handlers.go](../../../internal/api/handlers.go)
- [internal/partition/manager.go](../../../internal/partition/manager.go)

## Main Flow

1. Startup initializes tenant accountant.
2. Handlers extract tenant identity from request context or metadata.
3. Publish and delivery paths record and enforce tenant-level usage.

## Production Decisions

- Shared cluster fairness is improved by tenant-scoped accounting.
- Wiring through partition manager ensures delivery completion is also tracked.
- Default-tenant fallback avoids hard failure in missing-claim cases.

## Debug Pointers

- Tenant extraction path: [internal/api/handlers.go](../../../internal/api/handlers.go)
- Accounting state transitions: [internal/tenant/tenant.go](../../../internal/tenant/tenant.go)

## Related Diagrams

- [system_overview.mmd](../../mermaid/system_overview.mmd)
- [observability_feedback_loop.mmd](../../mermaid/observability_feedback_loop.mmd)

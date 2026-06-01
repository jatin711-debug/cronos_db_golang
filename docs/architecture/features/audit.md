# Audit Architecture

## Purpose

The audit module records important actions and security-relevant requests for compliance and post-incident analysis.

## Key Files

- [internal/audit/audit.go](../../../internal/audit/audit.go)
- [internal/api/audit_interceptor.go](../../../internal/api/audit_interceptor.go)
- [cmd/api/main.go](../../../cmd/api/main.go)

## Main Flow

1. Audit logger is created during startup.
2. API interceptor writes request-level audit events.
3. Records are appended to audit files with structured payloads.

## Production Decisions

- Audit initialization is non-fatal so data plane stays available even if audit path is degraded.
- Write path is guarded for concurrent access.
- Output is structured and parse-friendly for tooling.

## Debug Pointers

- Interceptor wiring: [internal/api/grpc_server.go](../../../internal/api/grpc_server.go)
- Audit event formatting and IO: [internal/audit/audit.go](../../../internal/audit/audit.go)

## Related Diagrams

- [observability_feedback_loop.mmd](../../mermaid/observability_feedback_loop.mmd)
- [system_overview.mmd](../../mermaid/system_overview.mmd)

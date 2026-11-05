# Audit Architecture

## Purpose

The audit module records important actions and security-relevant requests for compliance and post-incident analysis.

## Key Files

- [internal/audit/audit.go](../../../internal/audit/audit.go)
- [internal/api/audit_interceptor.go](../../../internal/api/audit_interceptor.go)
- [cmd/api/main.go](../../../cmd/api/main.go)

## Main Flow

1. Audit logger is created during startup.
2. The unary and stream audit interceptors write request-level audit events,
   but only when authentication is on (`--auth-enabled`). Without it every
   subject is anonymous and the interceptors record nothing
   (`internal/api/grpc_server.go`).
3. Records are appended as one NDJSON line each to `<data-dir>/audit/<UTC date at startup>.ndjson` (`audit.NewLogger`). The file is chosen once when the process starts; there is no rotation or retention.

## What is recorded

- One record per public-listener RPC when auth is on: `action` is the full gRPC method, `subject` the JWT subject (or `anonymous`), `outcome` `success` or `failure` from the call's returned error, and `detail` its duration. `resource` is empty. A refusal carried in a response body (`success: false`) is recorded as `success`.
- The internal listener (replication, Raft) has no audit interceptor, and `EventServiceHandler` holds the logger but writes no records of its own.
- Writing is best-effort: events go through a 4,096-entry channel to one worker, flushed and fsynced every 1,000 events or each second. When the channel is full the event is dropped and counted (`Logger.Dropped`) with a warning, so the audit trail can have gaps under load or after a crash.

## Production Decisions

- Audit initialization is non-fatal so data plane stays available even if audit path is degraded.
- Write path is guarded for concurrent access.
- Output is structured and parse-friendly for tooling.

## Debug Pointers

- Interceptor wiring: [internal/api/grpc_server.go](../../../internal/api/grpc_server.go)
- Audit event formatting and IO: [internal/audit/audit.go](../../../internal/audit/audit.go)

## Related Diagrams

- [Observability feedback loop](slo.md#observability-feedback-loop)
- [System overview](../README.md#system-overview)

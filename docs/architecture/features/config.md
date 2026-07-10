# Config and Reload Architecture

## Purpose

The config module defines defaults, merges runtime settings, and supports safe in-process reload behavior.

## Key Files

- [internal/config/defaults.go](../../../internal/config/defaults.go)
- [internal/config/config.go](../../../internal/config/config.go)
- [internal/config/reload.go](../../../internal/config/reload.go)
- [pkg/types/event.go](../../../pkg/types/event.go)

## Main Flow

1. Startup loads flags and environment overrides into a typed config.
2. System components are constructed from this config in the composition root.
3. Reload wrapper listens for SIGHUP and updates active config references.

## Production Decisions

- `--dev` disables production security requirements and is intended for local development only.
- Production validation requires TLS, auth, encryption at rest, replication mTLS, `--replication-factor>=3`, and `--min-insync-replicas>=2`.
- New environment variables: `CRONOS_DEV`, `CRONOS_MIN_IN_SYNC_REPLICAS`, `CRONOS_EXACTLY_ONCE_COMMITS`, `CRONOS_ENCRYPTION_ENABLED`, `CRONOS_ENCRYPTION_KEY_FILE`.
- New flags include retention settings and all security/TLS/auth flags.
- Helm `config.dev` defaults to `true`; `values-production.yaml` disables dev and requires TLS/auth/encryption/replication-mTLS secrets.
- Strong defaults favor stability and durability.
- Feature flags gate potentially risky behavior changes.
- Reload support avoids full process restart for selected runtime updates.

## Debug Pointers

- Unexpected runtime behavior: [internal/config/config.go](../../../internal/config/config.go)
- Missing default assumptions: [internal/config/defaults.go](../../../internal/config/defaults.go)

## Related Diagrams

- [startup_sequence.mmd](../../mermaid/startup_sequence.mmd)
- [system_overview.mmd](../../mermaid/system_overview.mmd)

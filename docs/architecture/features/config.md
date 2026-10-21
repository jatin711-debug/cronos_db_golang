# Config and reload architecture

For every flag, exact environment variable, default and plain-language use,
see the [server configuration reference](../../CONFIGURATION.md). Application
connection, publishing and consuming settings are in the
[SDK guide](../../CLIENT_GUIDE.md#sdk-settings).

## Key files

- [internal/config/config.go](../../../internal/config/config.go): registers flags, applies environment values and validates startup requirements.
- [internal/config/defaults.go](../../../internal/config/defaults.go): binary defaults.
- [internal/config/reload.go](../../../internal/config/reload.go): limited SIGHUP parsing and the shared config wrapper.
- [cmd/api/main.go](../../../cmd/api/main.go): constructs components, registers CDC sinks and starts listeners.
- [internal/partition/manager.go](../../../internal/partition/manager.go): passes startup settings to each partition and reads admission limits.
- [pkg/types/event.go](../../../pkg/types/event.go): typed server config.

## Main flow

1. `LoadConfig` applies defaults, nonempty environment values, then explicit flags. Invalid values fail startup.
2. `ValidateConfig` checks required values, lifecycle compatibility and production security/durability requirements.
3. The composition root builds listeners, tracer, partition manager and other components. Partition construction copies delivery settings into a dispatcher.
4. The Unix SIGHUP listener updates selected fields in the shared config. The composition root registers no component-update callbacks, so most running components keep their startup values. Admission reads load shedding directly. Restart to apply changes consistently; see [per-field reload limits](../../CONFIGURATION.md#apply-changes-and-sighup-limits).

## Full Flag Reference

The maintained [configuration reference](../../CONFIGURATION.md) lists every
flag with its exact environment variable, units, default and intended use.
`cronos-api --help` lists accepted flags in the binary. The test
`TestEveryFlagIsInTheConfigurationReference` checks that no flag is missing
or invented and that every environment name matches startup parsing.

## Environment Variables

Every flag reads `CRONOS_<FLAG>` with the uppercase name and dashes replaced
by underscores, except `--dedup-ttl` (`CRONOS_DEDUP_TTL_HOURS`) and
`--min-insync-replicas` (`CRONOS_MIN_IN_SYNC_REPLICAS`). Explicit flags win.
Additional CDC, experimental region and reload variables are described in
[the reference](../../CONFIGURATION.md#additional-environment-variables).

## Production decisions and limitations

- Production requires TLS, JWT auth with an explicit policy, WAL encryption, replication mTLS, at least three replicas, at least two write acknowledgements, durable fsync and seeds in cluster mode. `--dev` bypasses the requirements; it does not disable security that was explicitly enabled.
- Event lifecycle mode requires supported persistent formats and, in a cluster, majority acknowledgements and capability agreement. CDC sinks and experimental features cannot be combined with it. Activation is one-way; see the [lifecycle contract](../../RETENTION_AND_EVENT_LIFECYCLE.md).
- `--raft-join`, `--snapshot-catchup-threshold` and `--clock-skew-threshold-ms` are parsed but unused. Join uses seeds; connected followers use incremental catch-up; node clocks must be synchronized operationally.
- Helm configures the binary through environment variables. Named chart values cover common settings; `extraEnv` accepts the rest, including secret references. Base values enable dev mode. Use `values-production.yaml` and the required secrets for production.
- SIGHUP is not a complete component reload, and Windows has no usable SIGHUP path. The acknowledgement-timeout reload parser uses integer seconds, unlike startup's duration syntax. Restart instead of relying on runtime reload.

## Related diagrams

- [Startup lifecycle](../../DEVELOPER_ARCHITECTURE_GUIDE.md#42-startup-and-shutdown-sequence)
- [System overview](../README.md#system-overview)

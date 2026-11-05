# Current production audit probes

The `.go.txt` files are isolated correctness probes, not fixes. Each fails on audited HEAD `579fd79a15b84cf4bcc7bda878aee6fa9d665fc6` because the asserted safe behavior is missing. Output is preserved next to each probe. The ordinary suite passed separately (`go-tests.txt`).

Run each with a Go overlay so production/package source files need not be edited. An overlay JSON has the form:

```json
{"Replace":{"ABSOLUTE_REPO/internal/partition/zz_current_audit_test.go":"ABSOLUTE_REPO/docs/audit-probes/2026-11-05/cursor.go.txt"}}
```

Use absolute platform paths, escaping Windows backslashes in JSON. Set `GOARCH=amd64` and `CGO_ENABLED=0`, then run:

```text
go test -overlay overlay.json ./internal/partition -run ^TestAuditCorruptFeedCursor$ -count=1 -timeout 60s
```

For `config.go.txt`, map `internal/config/zz_current_audit_test.go`, test `./internal/config`, and select `^TestAuditProductionRequiresCluster$`. For `ready.go.txt`, map `internal/api/zz_current_audit_test.go`, test `./internal/api`, and select `^TestAuditReadyWithoutReplicationLeader$`.

The configuration probe checks validation with placeholder certificate paths; it does not start a secured node. The readiness probe exercises the actual partition, publish handler and readiness handler in process. Publishing fails closed: the finding is unusable configuration and false readiness, not unreplicated successful publishes. The CDC probe models damaged cursor metadata after a clean shutdown, not a demonstrated spontaneous filesystem-corruption mechanism.

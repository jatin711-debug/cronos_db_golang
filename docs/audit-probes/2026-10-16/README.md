**Production audit probes — 2026-10-16**

These probes accompany [the audit](../../PRODUCTION_AUDIT_2026-10-16.md) of `f8cc2063534be4aa97ffac602d9297b15d636e56`. They assert desired safety invariants and fail on the audited implementation. They do not alter runtime code or ordinary test discovery.

Run from the repository root:

```text
python docs/audit-probes/2026-10-16/run.py
```

Requires Python 3.9+ and the repository's Go toolchain/dependencies. The runner uses Go's overlay mechanism, Windows/Linux/macOS host OS, `GOARCH=amd64`, and `CGO_ENABLED=0`. It writes captured output to [evidence.txt](evidence.txt) and returns Go's nonzero exit code when an invariant fails. All database paths are test temporary directories; the replication probe listens only on loopback. No production cluster or data directory is touched.

Executed here on Windows, Go 1.26.9. Six test functions have nine leaf cases and reproduce five distinct defects:

- An accepted event with large metadata cannot be read from the WAL (single and batch).
- A producer-supplied log term is preserved (single and batch), and separately reaches a follower over gRPC replication.
- A follower installs dead-letter completion without receiving a dead-letter copy. This is a deterministic disposition/progress boundary test, not a killed-node acceptance campaign.
- A quota rejection leaves the single-publish ID claimed after the quota is lifted. The test explicitly configures quotas; the shipped entry point does not currently call `SetLimits`.
- A rotation failure after append releases the dedup claim, so a successful retry appends the same ID twice (single and batch). Failure injection blocks a next-segment path inside a test temporary directory; it does not exhaust a real disk or emulate power loss.

The local term and retry probes use existing test helpers. The follower term helper uses periodic fsync; its assertion concerns authority and replication contents, not power-loss durability. Other local probes use the existing batch-fsync audit helper.

`evidence.txt` is the output on the audited v0.6.0 source and is kept as it was.
The fixes shipped in v0.7.0; current status and validation limits are in the
[audit](../../PRODUCTION_AUDIT_2026-10-16.md) and
[v0.8.0 release notes](../../RELEASE_V0.8.0.md). The archived overlay is not a
current acceptance suite: for example, its rotation probe expects an initial
append error that the fix deliberately removed. Running this historical runner
also rewrites `evidence.txt`; use the ordinary regression suites for current
validation instead:

```sh
go test ./internal/api ./internal/storage ./internal/tenant
```

After a fix, promote its probe into ordinary tests and add public-transport, restart, encrypted-storage, or fault-campaign coverage as appropriate. Do not change these assertions merely to match the current defect.

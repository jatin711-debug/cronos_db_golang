# Documentation review — 2026-10-21

Reviewed the 47 Markdown documents tracked at release source
`cfba215208b3a50c830020636a417f7aaeaf3845`, plus the documentation build,
release workflows, Helm settings and referenced source paths. This review
aligns descriptions and examples with v0.8.0; it is not a new runtime security
audit, fault campaign or capacity measurement. This report is the 48th
Markdown document.

## Main corrections

- Replaced branch-only lifecycle status with the released, opt-in contract:
  stable references, revision/request-ID retries, majority acceptance,
  one-way capability agreement and recovery of matching WAL/progress/DLQ state.
- Corrected replica-count-only commitment descriptions. Term-start proof is
  required before an older prefix becomes committed; mixed clusters retain
  the legacy limitation until all required nodes are capable.
- Updated retention for accepted terminal records, prefix pruning and optional
  byte-bounded relocation. Unfinished due work and unresolved mutations remain
  blockers; relocation does not imply unconditional disk reclamation.
- Updated Docker examples to 0.8.0 and its immutable manifest. Distinguished
  public image identity, the `latest` index, config digests and signed provenance.
  The image was published locally without a signed workflow attestation.
- Corrected stale scheduler-memory descriptions, quota-enforcement claims,
  transaction state diagrams, append-error claim rollback, client endpoint
  advertisement and misleading data-directory upgrade instructions.
- Preserved dated performance results and probe evidence. Added qualifications
  where an old result could be mistaken for current release validation.

## Per-document disposition

“Retained” means reviewed without a content change because the document is
already qualified or has no release-specific claim to update. Historical
findings and raw evidence retain their original results; closure notes describe
subsequent fixes.

| Document | Disposition |
|---|---|
| [README](../README.md) | Updated current image, lifecycle scope, timing/retention claims, flags and upgrade examples. |
| [Architecture](../ARCHITECTURE.md) | Updated commitment, replication RPCs, retention, recovery, diagrams and limits. |
| [Developer guide](DEVELOPER_ARCHITECTURE_GUIDE.md) | Updated lifecycle/recovery flows, transaction states, auth and unwired tenant quotas. |
| [System-design guide](system-design/README.md) | Updated current-term proof, lifecycle qualification, physical identity, timer costs and snapshot triggers. |
| [Docker guide](../DOCKER.md) | Updated image/digest, runnable image references, advertised endpoint, memory limits and upgrade options. |
| [Contributing](../CONTRIBUTING.md) | Added strict docs build, protocol documentation expectations and current reading links. |
| [Security policy](../.github/SECURITY.md) | Retained latest-release support policy and private reporting route. |
| [PR template](../.github/PULL_REQUEST_TEMPLATE.md) | Added documentation-only category and strict build check. |
| [Code of conduct](../CODE_OF_CONDUCT.md) | Retained community policy; no runtime/version claims. |
| [Release requirements](PRODUCTION_RELEASE.md) | Updated published artifacts, fixes versus gaps, scheduler memory, backup PVC and build prerequisites. |
| [v0.8.0 notes](RELEASE_V0.8.0.md) | Recorded publication, image/config/index identity and absence of signed provenance. |
| [Lifecycle contract](RETENTION_AND_EVENT_LIFECYCLE.md) | Updated released status, standalone development scope and durable activation. |
| [Term-start protocol](REPLICATION_TERM_START.md) | Added release status and mixed-version activation qualification. |
| [v0.7.0 hardening](HARDENING_V0.7.0.md) | Preserved results; qualified historical workflow and legacy snapshot behavior. |
| [September audit](PRODUCTION_AUDIT_2026-09-27.md) | Updated closure date and later leadership, snapshot, DLQ, retention, memory, scan and release status. |
| [October audit](PRODUCTION_AUDIT_2026-10-16.md) | Distinguished shipped runtime fixes from remaining release/capacity evidence. |
| [September publish measurements](CLUSTER_PERFORMANCE_VALIDATION_2026-09-29.md) | Preserved numbers and added explicit pre-v0.8.0 qualification. |
| [October throughput measurements](CLUSTER_PERFORMANCE_VALIDATION_2026-10-16.md) | Preserved matched v0.6.0/v0.7.0 results; no v0.8.0 extrapolation. |
| [Maintenance validation](MAINTENANCE_VALIDATION_2026-09-29.md) | Qualified the historical recovery/retention path against lifecycle generations. |
| [July performance baseline](PERFORMANCE.md) | Retained existing historical/invalid-result qualifications and comparison method. |
| [Throughput harness](../tools/throughput/README.md) | Distinguished available tooling from an executed release measurement. |
| [September probes](audit-probes/README.md) | Retained archived-probe versus ordinary-regression distinction and toolchain prerequisites. |
| [October probes](audit-probes/2026-10-16/README.md) | Removed obsolete open-DLQ statement; distinguished historical overlays from current tests. |
| [Historical remediation plan](../plan.md) | Updated superseding references; retained historical checkboxes. |
| [Historical technical deep dive](../TECHNICAL_DEEP_DIVE.md) | Added protocol references and qualified illustrative old code. |
| [Feature index](architecture/README.md) | Updated current hardening, lifecycle links, retention and deferred-feature claims. |
| [API](architecture/features/api.md) | Updated lifecycle RPCs, authorization, control visibility and append-error diagram. |
| [Audit logging](architecture/features/audit.md) | Retained request-interceptor/auth-gated behavior and non-fatal initialization scope. |
| [Auth](architecture/features/auth.md) | Added lifecycle query/mutation publish-permission requirement. |
| [CDC](architecture/features/cdc.md) | Updated committed/control cursors and lifecycle incompatibility. |
| [Cluster](architecture/features/cluster.md) | Added durable capability requirements for replicas, handoffs and voters. |
| [Compliance](architecture/features/compliance.md) | Added terminal-state and bounded relocation rules. |
| [Config](architecture/features/config.md) | Added missing flags, activation constraints, advertised endpoint and unused join setting. |
| [Consumer groups](architecture/features/consumer.md) | Added physical successor ACK and matching recovery-generation semantics. |
| [Dashboard](architecture/features/dashboard.md) | Corrected quota availability and identified gRPC-only lifecycle operations. |
| [Dedup](architecture/features/dedup.md) | Distinguished publish TTL/claims from lifecycle identity and request recovery. |
| [Delivery](architecture/features/delivery.md) | Added staged/accepted-state gate, stale-timer suppression and recall limits. |
| [Partition](architecture/features/partition.md) | Added lifecycle index, term-start and recovery-state ownership. |
| [Replay](architecture/features/replay.md) | Added committed-boundary and active logical-view semantics. |
| [Replication](architecture/features/replication.md) | Updated format-specific RPCs, commitment proof, snapshots and relocation. |
| [Scheduler](architecture/features/scheduler.md) | Added lifecycle delivery gate and internal-record suppression. |
| [Schema](architecture/features/schema.md) | Retained registry, compatibility and optional publish-validation flow. |
| [SLO/metrics](architecture/features/slo.md) | Retained bounded recording and external operational feedback scope. |
| [Storage](architecture/features/storage.md) | Updated terminal records, prefix pruning, relocation, backup cuts and logical identity. |
| [Tenant accounting](architecture/features/tenant.md) | Retained explicit absence of runtime quota configuration and zero-usage behavior. |
| [Tracing](architecture/features/tracing.md) | Retained startup/shutdown and disabled/sampled exporter behavior. |
| [Transactions](architecture/features/tx.md) | Added lifecycle incompatibility; retained experimental status and actual state names. |

Raw audit `.txt` evidence, archived `.go.txt` probes and benchmark output were
not rewritten. The embed-placeholder README was checked against the build flow;
it remains build guidance, not a rendered dashboard asset. MkDocs navigation now
exposes the lifecycle contract, commitment protocol and this review.

## Verification scope

Validation of this documentation change:

- `python -m mkdocs build --strict --site-dir build/docs-audit-v080-site`.
- 826 relative documentation/source/asset targets checked; none missing.
- Both flag tables cover all 95 flags registered by the server, with no unknown
  table entries.
- All 67 embedded Mermaid diagrams parsed with Mermaid 11.17.2; the 10 changed
  diagrams also rendered successfully.
- Anonymous registry checks confirmed the documented v0.8.0 manifest and the
  single Linux amd64 entry selected by `latest` on 2026-10-21.
- `git diff --check`; historical finding bodies and raw evidence preserved.

The review checks documentation, examples, source-path references, flag coverage
and diagram syntax against the release source. The strict site build validates
the generated documentation and cross-page anchors. These checks do not rerun
the runtime suites or refresh the dated vulnerability scan. Required runtime CI
still applies to the pull request.

Remaining product and validation gaps are maintained in
[release requirements](PRODUCTION_RELEASE.md#what-v080-was-released-without).
This review does not assert that all major issues are fixed.

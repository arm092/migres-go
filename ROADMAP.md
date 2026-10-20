# Roadmap

[Back to README](README.md)

This is the public development direction. Items are proposals or acceptance gates, not features already shipped. Ordering reflects current priorities; there are no committed delivery dates.

## Production readiness

- [x] Complete a continuous 24-hour workload/fault run on a frozen runtime revision, with exact row and checkpoint reconciliation and a final PASS.
- [ ] Publish a reproducible acceptance report for supported MySQL/ClickHouse combinations, including every replica after failover and partial DDL.
- [ ] Validate node-loss fencing, PVC reattachment, and restore on production-equivalent CSI storage.
- [ ] Extend repeated fault injection around queue commits, INSERT acknowledgement, generation publication, reset, and control-connection loss.
- [ ] Benchmark realistic row widths, large transactions, sustained CDC, snapshot contention, and outage recovery under fixed CPU/memory/disk budgets.

Done means evidence from repeatable runs, not only code paths or passing unit tests.

## Operations and recovery

- [x] Offline checked journal backup/restore and compaction, with exclusive-owner protection and same-version recovery tests.
- [ ] Coordinated ClickHouse/journal disaster recovery and journal migrations across binary versions.
- [ ] Configurable history/version retention with an explicit earliest safe replay boundary and protection for active rebuilds/operations.
- [x] Offline journal compaction and live retained-history/free-page growth metrics.
- [ ] A reconciliation command reporting missing, extra, and divergent keys/values before any repair.
- [ ] Exportable operation history, clearer progress, and cancellable long-running operations at safe boundaries.
- [ ] Prometheus alert examples and Grafana dashboards for backlog, free space, blocked delivery, quorum, and replica delay.
- [ ] Durable notification retry/outbox and provider-delivery diagnostics without exposing tokens or data.

Retention and replay changes must never silently remove history required for recovery.

## Distribution and deployment

- [ ] Tagged releases with Linux amd64/arm64 binaries, checksums, and reproducible version information.
- [ ] Published multi-architecture container images with immutable digests, provenance, and an SBOM.
- [ ] Helm or Kustomize packaging for storage, secrets, probes, security policy, and resources.
- [ ] Tested upgrade/rollback and journal-format migration procedures.
- [ ] Automated TLS fixture coverage in CI and optional client certificate authentication.

The current Kubernetes image reference is a placeholder for an image the operator builds and publishes.

## Compatibility and schema evolution

- [ ] Broaden DDL fixtures for mixed-database operations and ALTER statements combining table rename with column changes.
- [ ] Investigate safe replay across DDL using historical generations, without claiming that rewind restores ClickHouse to a past state.
- [ ] Expand charset, temporal edge-case, generated-column, expression DEFAULT, and unsupported type diagnostics.
- [ ] Evaluate transaction compression and partial JSON support with explicit decoder compatibility tests.
- [ ] Evaluate additional sources or multiple target shards only after defining ownership, ordering, routing, and recovery contracts.

Unsupported operations must continue to block safely until their recovery semantics are implemented and tested.

## Throughput and efficiency

- [ ] Reproducible CDC throughput and latency benchmarks, including replication/quorum overhead.
- [ ] Profile serialization, bbolt access, source boundary tracking, and allocation under large backlogs.
- [ ] Adaptive batching within explicit latency, memory, and part-count budgets.
- [ ] Fairness across hot/cold tables while preserving per-table order and DDL barriers.
- [ ] More precise source-to-target freshness metrics instead of relying on queue age alone.

## Propose a change

Open an [issue](https://github.com/arm092/migres-go/issues) with the workload, observable problem, required guarantee, and a proposed acceptance check. See [contributing](CONTRIBUTING.md). [Sponsorship](https://github.com/sponsors/arm092) helps fund maintenance and compatibility testing.

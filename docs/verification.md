# Verification

[Back to README](../README.md)

## Local checks

```sh
go test ./...
go vet ./...
go test -race ./...
```

Database checks are opt-in and use disposable fixture databases. Do not point test endpoints at production.

```sh
docker compose -p migres-go-test -f test/compose.yml up -d --wait mysql clickhouse
MIGRES_INTEGRATION=1 go test ./test ./internal/app -v -count=1 -timeout=10m
```

CI exercises MySQL 5.7/8.0/8.4 against ClickHouse 24.8/26.8. `MIGRES_TEST_HOST`, `MIGRES_MYSQL_PORT`, and `MIGRES_CLICKHOUSE_PORT` select fixture endpoints.

## Replication and source faults

```sh
docker compose -p migres-go-replicated -f test/replicated.yml up -d --wait
MIGRES_INTEGRATION=1 MIGRES_REPLICATED=1 MIGRES_LINEAGE_REPLICATED=1 \
  go test ./test -run 'TestReplicated|TestRenameReplay|TestRebuildSupersedingDDL' -v -count=1 -timeout=5m
```

The following deliberately stops the second replica of the named fixture:

```sh
MIGRES_INTEGRATION=1 MIGRES_FAULTS=1 \
  go test ./test -run '^TestReplicaLoss' -v -count=1 -timeout=3m
```

Run the binlog-loss test separately, after other tests have stopped using the source. It purges the isolated fixture's binlogs:

```sh
MIGRES_INTEGRATION=1 MIGRES_SOURCE_FAULTS=1 \
  go test ./internal/app -run '^TestLostLog$' -v -count=1 -timeout=1m
```

CI also fills a dedicated 4 MiB tmpfs to verify journal recovery after ENOSPC. See [the workflow](../.github/workflows/ci.yml) for its bounded container command.

## Continuous 24-hour acceptance

```sh
MIGRES_INTEGRATION=1 MIGRES_SOAK_DURATION=24h \
  go test ./test -run '^TestSoak$' -v -count=1 -timeout=25h
```

The soak reconciles every key/value and the source checkpoint after transaction cycles, rollbacks, process kills, and binlog rotations. Success requires the entire duration and a final PASS/exit 0. Shutdown time, interrupted attempts, and progress messages alone do not establish a successful run.

On 19 October 2026, runtime revision `90fca7fa9dfb3c373928b629d0ba194d244ebc03` completed the configured continuous 24-hour synthetic run: `TestSoak` PASS in 86,401.58 seconds and container exit 0. The last periodic progress reported 254,670 cycles with exact reconciliation. The runner was limited to 1 CPU and 2 GiB RAM; database fixtures ran on the shared local Docker host. This validates this fixture, not production workload capacity.

## Coverage and limits

Local maintenance acceptance also covers:

- Offline backup/restore of checkpoints, event/task queues, staged transactions, instance identity, schema generations, operation state, task IDs, versions, and deduplication tokens; corruption and live-owner rejection; competing exports cannot overwrite the winner. A full tmpfs export publishes no incomplete backup and succeeds on retry after space is freed.
- Restored CDC catching up source changes made while stopped, including composite-key changes and deletes, against the unchanged target.
- Eight large drained queue bursts reusing pages, 2,000 retained boundary/version pairs surviving compaction, and separate history/free-page metrics.
- Mixed selected/excluded RENAME/DROP and excluded unsupported ALTER isolation, compound column changes/defaults, and replay after name reuse. SET/DROP DEFAULT and compound ALTER with table rename are supported, including lost DDL responses, journal reopen, type-changing rebuild, earlier queued rebuild, and initial snapshot. Unknown operations still fail closed.
- A 200 ms shutdown deadline during snapshot INSERT, CDC INSERT, transformer batching wait, and rebuild shadow INSERT, followed by journal reopen and exact recovery. The INSERT gate is injected at the real driver's cooperative batch boundary; this is not a hung-kernel test. Transformer unit tests also cancel inside row conversion and retain the whole unacknowledged batch.
- Both debug event/task archives expiring old rows while retaining fresh rows through background TTL on ClickHouse 24.8 and 26.8. Only fixture tables use `merge_with_ttl_timeout=1`; production uses normal asynchronous scheduling. Retention changes are reconciled on the next archive write after restart, and normal mode creates no debug archives.

```sh
MIGRES_INTEGRATION=1 go test ./test \
  -run 'TestBackupRestoreCDC|TestMixedTableDDLSelection|TestCompoundAlterCDC|TestUnsupportedAlterStopsBeforeApplying|TestDebugArchiveTTL' \
  -v -count=1 -timeout=5m
MIGRES_INTEGRATION=1 go test ./internal/app \
  -run '^TestShutdownDeadlinePreservesPendingWork$' -race -v -count=3 -timeout=3m
go test ./internal/journal -run 'TestOfflineBackupRestore|TestJournalGrowthAndCompaction' -v
```

The recorded 24-hour result above applies to runtime `90fca7f`. Subsequent maintenance/filter/TTL/cancellation changes require their own continuous qualification; short regression runs do not transfer that result to a new runtime.

DEFAULT regression fixtures assert unchanged negative Int64 values, decimals, nullable values, timestamp precision, actual source-defaulted inserts, and per-replica default metadata after replay. BIT and binary hex defaults are checked at snapshot, ADD, SET, and subsequent insert, including zero bytes. They also simulate virtual columns left by an older target schema: SET/DROP must preserve the old effective values. Compound rename fixtures inject response loss after target rename and after column ADD, reopen the journal, and repeat the task without losing renamed column values or changing the Keeper path. A deliberately partial cluster DEFAULT update cannot receive a completion marker until both replicas match.

```sh
MIGRES_INTEGRATION=1 go test ./test \
  -run 'TestAlterDefault|TestCompoundTableRename' -race -v -count=1 -timeout=5m
```

Implemented checks cover durable handoffs, composite keys and key changes, rollback, restart, schema changes/rebuilds, name reuse, chained rename replay, reset ownership, missing binlogs, quorum failure/failover, partial DDL, disk exhaustion, and snapshot SIGTERM/SIGKILL in GTID/signal modes. TLS fixtures have exercised trusted and unknown CAs; notification payload/rate contracts use local HTTP mocks.

Local Kubernetes exercises have covered CSI ReadWriteOncePod exclusion, persistence across Pod replacement, rolling restart, database-outage liveness, and recovery after a termination-timeout SIGKILL. These do not prove fencing of a failed production node on another storage driver.

## Performance

```sh
MIGRES_INTEGRATION=1 MIGRES_PERFORMANCE=1 \
  go test ./test -run '^TestPerformance$' -v -count=1 -timeout=3m
```

This small synthetic snapshot fixture reports elapsed time, rows/second, and journal size. Record hardware, container limits, database settings, row widths, batch sizes, and background load with results. It is not a production capacity recommendation or CDC latency benchmark.

Before production, validate target storage fencing, reconcile source/target values per replica, test restore, and measure the actual workload. See [release gates](../ROADMAP.md#production-readiness).

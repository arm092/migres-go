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

## Coverage and limits

Implemented checks cover durable handoffs, composite keys and key changes, rollback, restart, schema changes/rebuilds, name reuse, chained rename replay, reset ownership, missing binlogs, quorum failure/failover, partial DDL, disk exhaustion, and snapshot SIGTERM/SIGKILL in GTID/signal modes. TLS fixtures have exercised trusted and unknown CAs; notification payload/rate contracts use local HTTP mocks.

Local Kubernetes exercises have covered CSI ReadWriteOncePod exclusion, persistence across Pod replacement, rolling restart, database-outage liveness, and recovery after a termination-timeout SIGKILL. These do not prove fencing of a failed production node on another storage driver.

## Performance

```sh
MIGRES_INTEGRATION=1 MIGRES_PERFORMANCE=1 \
  go test ./test -run '^TestPerformance$' -v -count=1 -timeout=3m
```

This small synthetic snapshot fixture reports elapsed time, rows/second, and journal size. Record hardware, container limits, database settings, row widths, batch sizes, and background load with results. It is not a production capacity recommendation or CDC latency benchmark.

Before production, complete the continuous soak, validate target storage fencing, reconcile source/target values per replica, test restore, and measure the actual workload. See [release gates](../ROADMAP.md#production-readiness).

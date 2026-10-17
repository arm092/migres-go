# Migres Go

MySQL to ClickHouse change data capture, with an initial online copy and a durable three-stage pipeline.

```text
MySQL → Producer → durable events → Transformer → durable tasks → Consumer → ClickHouse
```

The Producer reads committed binlog transactions. The Transformer creates versioned rows and schema operations. The Consumer delivers tasks, preserving table order and enforcing DDL barriers. Both queues, checkpoints, snapshot progress, ownership and recovery operations live in one bbolt journal on a persistent disk.

## Build and configure

Requires Go 1.26.9 or newer in the 1.26 series. Runtime deployment targets Linux, Docker and a single Kubernetes StatefulSet replica. Windows can be used for development.

```sh
go build -o migres ./cmd/migres
cp config.yml.example config.yml
export MYSQL_PASSWORD='...'
export CLICKHOUSE_PASSWORD='...'
./migres check --config config.yml
./migres run --config config.yml
```

Create the target database with `ENGINE=Atomic` before running. A fresh migration requires a separate target database. Existing tables with the same names are accepted only when their ownership generation matches the journal.

MySQL requires InnoDB, `log_bin=ON`, `binlog_format=ROW`, `binlog_row_image=FULL`, and a primary key or a configured full UNIQUE NOT NULL key. Configure a unique `migration.cdc.server_id` per running pipeline. GTID is optional for binlog reading; set `migration.cdc.use_gtid: true` to use GTID resume.

Online copying requires either GTID enabled or a writable dedicated signal table. For a source without GTID, prepare that table once using a user with CREATE privileges:

```sh
./migres prepare-source --config config.yml
```

The runtime user needs SELECT and replication privileges, plus INSERT/UPDATE on the signal table in signal mode. Snapshot uses keyset pagination and a binlog boundary after each chunk. Snapshot rows have a baseline version; committed CDC versions take precedence. Schema revisions are checked atomically before publishing a chunk. This produces a convergent copy, not a single database-wide point-in-time snapshot.

`migration.mode` accepts `snapshot` and `cdc`. `snapshot_before: true` performs the initial copy before declaring the CDC service ready. Completed tables and chunks resume from the same journal. Do not delete the journal to restart normally.

## Configuration

See [config.yml.example](config.yml.example). YAML is strict: unknown fields and invalid overrides stop startup. Environment overrides use upper-case YAML paths, for example `MYSQL_HOST`, `CLICKHOUSE_DATABASE`, `MIGRATION_MODE`, `MIGRATION_WORKERS`, `CDC_SERVER_ID`, `CDC_USE_GTID`, `CDC_DB_DEBUG`, `DATA_DIR`, `RUNTIME_DIR`, and `SHUTDOWN_TIMEOUT`. Lists use comma-separated values. Durations use Go units such as `250ms`, `5s`, `1m`.

TLS settings are `mysql.ssl_ca`, `mysql.ssl_disabled`, `clickhouse.secure`, `clickhouse.verify`, `clickhouse.ca_certs`. Teams, Slack and Telegram can run together; secrets can be supplied through `NOTIFICATIONS_TEAMS_WEBHOOK_URL`, `NOTIFICATIONS_SLACK_WEBHOOK_URL`, `NOTIFICATIONS_TELEGRAM_BOT_TOKEN` and `NOTIFICATIONS_TELEGRAM_CHAT_ID`.

No Python `buffer.db` or `state.json` import is performed. Environment names and duration syntax follow this Go configuration; old numeric second values must be converted to duration strings.

## ClickHouse engines and reads

Supported engines: `ReplacingMergeTree`, `ReplicatedReplacingMergeTree`, and an explicit append-only `MergeTree` mode. The convergence contract applies to the replacing engines. Append-only mode retains row versions and requires explicit aggregation by the consumer of the data.

```sql
SELECT * FROM orders FINAL WHERE _migres_deleted = 0;
```

Raw tables contain historical row versions and deletion markers. An ordinary SELECT is not a current-state view. There is no cross-table transaction visibility guarantee.

For a replicated target configure one shard, `clickhouse.cluster`, and `clickhouse.addresses` containing every replica endpoint. Keeper paths contain a stable generation ID rather than the SQL table name. `write_ack.mode` accepts `single` or `quorum`; `write_ack.quorum` accepts `auto` (majority) or a positive number. With two replicas, majority requires both. Acknowledgement occurs after the configured level; unavailable quorum retains the task for retry.

DDL is parsed from binlog SQL and its schema history is retained. CREATE, DROP, TRUNCATE, RENAME and column changes are handled in order. Incompatible type/key changes build a shadow table and switch generations. Replica exchanges inspect ownership markers before retrying so a lost response cannot swap generations back. Switching is atomic per server, not simultaneously across replicas. A later ALTER or TRUNCATE invalidates a rebuild and starts a fresh generation. DROP/RENAME during a rebuild can block it and require operator recovery. Mixed-database DDL and combined ALTER with table rename require separate statements.

## Runtime control

Commands contact the running process through its owner-only Unix socket. They do not open the journal as a second writer. Use stable operation IDs when retrying an ambiguous command:

```sh
migres status --config config.yml
migres reposition --config config.yml --file mysql-bin.000123 --pos 456 --operation-id recovery-001
migres reposition --config config.yml --gtid-set 'server-uuid:1-123' --operation-id recovery-002
migres status --config config.yml --operation-id recovery-001
migres reset --config config.yml --operation-id reset-001
migres resnapshot --config config.yml --operation-id copy-001
migres resnapshot --config config.yml --table orders --operation-id orders-copy-001
```

Reposition validates transaction boundaries and retained schema history. By default pending work drains first. `--discard-pending` archives and removes pending work. Forward jumps require `--allow-gap` and mark completeness as lost. A forward gap containing SQL without retained schema history requires resnapshot. Rewinding replays source changes; it does not undo ClickHouse to a historical time. Rewinds across DDL are rejected and require resnapshot. GTID reposition requires an exact retained boundary. In GTID mode, file/position must also have a retained GTID mapping. Positions older than retained history require a new snapshot. A table resnapshot returns `queued`; its operation becomes `done` after publication, and readiness stays false while it rebuilds.

`reset` drops only objects registered to this journal and verifies generation markers before dropping. It resets local replication state and starts a fresh copy when configured. Never reuse a data directory for a different source. Interrupted applying reset operations resume on startup.

Linux SIGUSR1 requests reset. SIGUSR2 requests the configured `force_binlog_position`; the setting is not applied during ordinary startup. The same transaction-boundary and gap checks apply to signals.

SIGTERM/SIGINT stops scheduling work, saves Producer state, completes Transformer transactions, and lets Consumer finish in-flight writes within `shutdown_timeout`. It does not empty the whole backlog. Unknown write outcomes stay unacknowledged for replay.

## Deployment and observability

Docker: `docker compose up -d`. Use a persistent volume and retain it across upgrades.

Kubernetes manifests are in [deploy/kubernetes/statefulset.yaml](deploy/kubernetes/statefulset.yaml). Create a `migres` ConfigMap containing `config.yml` and a `migres` Secret with `mysql-password` / `clickhouse-password`. Build and publish an image to your registry, then replace the manifest image reference with its immutable digest. No image is automatically published by this repository.

Use a CSI StorageClass supporting ReadWriteOncePod; set `volumeClaimTemplates[].spec.storageClassName` if that class is not the cluster default. Configure image pull credentials separately for a private registry.

The StatefulSet uses one replica, a ReadWriteOncePod PVC, non-root execution, and a 90-second termination grace period. The storage driver must support ReadWriteOncePod and reliable exclusive file locking. Never force-delete a Pod and start another owner without fencing the former node.

`/livez` reports process health, `/readyz` reports initial delivery and blocking state, `/metrics` exposes queue and pipeline gauges. A permanent error keeps the process available for diagnostics/recovery while readiness is false. Transient connection failures retry without relying on liveness restarts.

Tune worker counts, row batches, `max_transaction_bytes`, `max_queue_bytes` and container memory together for the source row sizes. Transactions above the staging limit stop without advancing their checkpoint. Replay identity and schema history are retained for the lifetime of the journal; provision and monitor its disk space. The free-space reserve pauses ingestion before disk exhaustion. MySQL transaction compression and partial JSON binlog images must be disabled.

`cdc.db_debug: true` explicitly archives event/task payloads in instance-specific `_migres_debug_*` tables, with `debug_retention` TTL. Normal logs must not include credentials or row payloads. Debug archive access should match source-data access.

## Verification

```sh
go test ./...
go vet ./...
go test -race ./...
docker compose -p migres-go-test -f test/compose.yml up -d --wait
MIGRES_INTEGRATION=1 go test ./test ./internal/app -count=1 -timeout=10m
docker compose -p migres-go-replicated -f test/replicated.yml up -d --wait
MIGRES_INTEGRATION=1 MIGRES_REPLICATED=1 go test ./test -count=1 -timeout=10m
MIGRES_INTEGRATION=1 MIGRES_FAULTS=1 go test ./test -run TestReplicaLoss -v -count=1 -timeout=3m
MIGRES_INTEGRATION=1 MIGRES_SOAK_DURATION=24h go test ./test -run '^TestSoak$' -v -count=1 -timeout=25h
```

These fixtures use isolated databases and published localhost ports. Never point them at production. `MIGRES_FAULTS=1` temporarily stops the fixture's second ClickHouse replica. The soak repeatedly reconciles every key/value after transactions, rollbacks, process kills and binlog rotations. `MIGRES_TEST_HOST`, `MIGRES_MYSQL_PORT` and `MIGRES_CLICKHOUSE_PORT` select the fixture endpoints.

Integration checks have passed against MySQL 5.7/8.0/8.4 and ClickHouse 24.8/26.8, plus two 24.8 replicas with Keeper, quorum failure, connection failover and partial DDL recovery. Linux race tests include real database integration. Docker non-root volume startup, socket control and SIGTERM have been exercised. A local Kubernetes 1.37 cluster with a CSI hostpath driver exercised ReadWriteOncePod exclusion, persistent Pod replacement, rolling restart, SIGKILL during a pending INSERT, database-outage liveness, reset preserving a foreign table, GTID replay and SIGUSR2. Production storage fencing, a completed 24-hour soak, the complete fault-injection matrix and a measured performance baseline remain release acceptance requirements. This repository is an initial implementation, not a production-qualified release.

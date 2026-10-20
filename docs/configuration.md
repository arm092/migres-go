# Configuration

[Back to README](../README.md) | [Complete YAML example](../config.yml.example)

YAML is strict: unknown fields and invalid values stop startup. Omitted fields use defaults. Supply secrets through environment variables; local `config.yml` and `.env` are ignored by Git.

## Source and table selection

```yaml
mysql:
  host: mysql.internal
  port: 3306
  user: migres
  database: shop
  include_tables: [orders, customers, external_records]
  exclude_tables: [audit_events]
  keys:
    external_records: [tenant_id, external_id]
```

Filters use exact names, not globs. Empty includes mean all eligible tables; exclusions win. The signal table is always excluded. A configured key must match a full database-enforced UNIQUE constraint with only NOT NULL columns. Prefix indexes and synthesized row hashes are not accepted identities.

| Source field | Default |
| --- | --- |
| `mysql.host`, `mysql.port` | `127.0.0.1`, `3306` |
| `mysql.user`, `mysql.password` | Empty |
| `mysql.database` | Required |
| `mysql.include_tables`, `mysql.exclude_tables` | Empty lists |
| `mysql.keys` | Empty map; YAML only |

## ClickHouse

Supported engines: `ReplacingMergeTree`, `ReplicatedReplacingMergeTree`, and explicit append-only `MergeTree`. Current-state convergence applies to the replacing engines. Plain MergeTree needs version-aware aggregation by the reader.

| Target field | Default / purpose |
| --- | --- |
| `clickhouse.addresses` | Empty; overrides host/port with replica endpoints |
| `clickhouse.host`, `clickhouse.port` | `127.0.0.1`, `9000` |
| `clickhouse.user`, `clickhouse.password` | `default`, empty |
| `clickhouse.database` | Required dedicated Atomic database |
| `clickhouse.engine` | `ReplacingMergeTree` |
| `clickhouse.cluster` | Required for replicated engine |
| `clickhouse.replication.path_prefix` | `/migres/tables` |
| `clickhouse.replication.replica_macro` | `replica` |
| `clickhouse.write_ack.mode` | `quorum`; `single` or `quorum` |
| `clickhouse.write_ack.quorum` | `auto` or a quoted positive integer |
| `clickhouse.write_ack.timeout` | `30s` |

Replication supports one shard. List every replica in `addresses`; see [the example](../examples/replicated.yml). With two replicas, majority requires both. Unavailable quorum retains the task for retry. Acknowledgement is not simultaneous visibility on every replica.

## Migration and parallelism

| Field | Default / purpose |
| --- | --- |
| `migration.mode` | `cdc`; `snapshot` or `cdc` |
| `migration.cdc.snapshot_before` | `true` |
| `migration.snapshot_mode` | `auto`; `auto`, `gtid`, or `signal` |
| `migration.signal_table` | `_migres_signal` |
| `migration.batch_rows` | `5000`; snapshot chunks and transformed INSERT limit, at most 100000 |
| `migration.workers` | `4`; snapshot workers |
| `migration.consumer_workers` | `4`; independent-table delivery workers |
| `migration.clickhouse_timezone` | `UTC` |
| `migration.low_cardinality_strings` | `false`; eligible string/ENUM/SET columns |
| `migration.debug` | `false`; logging level |

Producer and Transformer preserve serial source ordering. Snapshot and independent-table delivery provide parallelism; there is no worker count for each individual stage.

`auto` uses GTID when enabled, otherwise a writable signal table. Text CDC uses historical column charsets; BINARY/BLOB remains bytes. Unknown or lossy encodings block ingestion.

## CDC settings

All fields below are nested under `migration.cdc`:

| Field | Default | Purpose |
| --- | --- | --- |
| `server_id` | `4379` | Unique active replication client ID |
| `use_gtid` | `false` | GTID resume instead of file/position |
| `heartbeat` | `5s` | Binlog heartbeat |
| `producer_batch_size` | `100` | Event staging batch limit |
| `producer_flush_interval` | `1s` | Staging flush interval |
| `transformer_poll_interval` | `100ms` | Event queue polling |
| `consumer_poll_interval` | `100ms` | Task queue polling |
| `batch_max_wait` | `1s` | Flush partial Transformer batches |
| `checkpoint_interval_rows` | `1000` | Transformer fetch/flush threshold, currently counted as events |
| `prepared_queries_batch_limit` | `100` | Consumer task fetch limit |
| `raw_events_max` | `50000` | Raw event backpressure threshold |
| `raw_events_resume_ratio` | `0.8` | Resume below the high-water mark |
| `force_binlog_position` | Empty | Applied only on SIGUSR2 |
| `db_debug` | `false` | Payload archives in ClickHouse |

The retained `checkpoint_interval_rows` name currently counts events, not rows inside each event. Transactions still become visible only after COMMIT.

## State and resource limits

| Field | Default | Purpose |
| --- | --- | --- |
| `data_dir` | `data` | Durable journal |
| `runtime_dir` | `data/run` | Owner-only Unix socket |
| `listen` | `127.0.0.1:8080` | HTTP probes/metrics |
| `environment` | `prod` | Notification label |
| `shutdown_timeout` | `60s` | Ordered shutdown deadline |
| `max_queue_bytes` | `10737418240` | Active queue threshold |
| `max_transaction_bytes` | `1073741824` | Transaction staging limit |
| `min_free_disk_bytes` | `1073741824` | Free-space reserve |
| `debug_retention` | `24h` | ClickHouse debug archive TTL; whole seconds, minimum `1s` |

These are admission/backpressure limits, not a process RSS cap. Size rows, batches, staging, workers, and container memory together. Replay identities and schema history currently remain for the journal lifetime; active queue thresholds do not cap total historical storage.

## Environment overrides

Names follow uppercase YAML paths, with `migration.cdc.*` shortened to `CDC_*`:

```sh
export MYSQL_HOST='mysql.internal'
export MYSQL_PASSWORD='your-source-password'
export MYSQL_INCLUDE_TABLES='orders,customers'
export CLICKHOUSE_DATABASE='analytics'
export MIGRATION_CONSUMER_WORKERS='8'
export CDC_USE_GTID='true'
export CDC_BATCH_MAX_WAIT='500ms'
export SHUTDOWN_TIMEOUT='60s'
```

Lists use commas without surrounding spaces. An empty list override clears the list. Durations use Go units (`250ms`, `5s`, `1m`); byte limits are integers. Map overrides such as `MYSQL_KEYS` are not supported; use YAML.

## TLS

```yaml
mysql:
  ssl_ca: /etc/migres/certs/mysql-ca.pem
  ssl_disabled: false
clickhouse:
  secure: true
  verify: true
  ca_certs: /etc/migres/certs/clickhouse-ca.pem
```

The MySQL CA enables verified source TLS unless explicitly disabled. ClickHouse uses `secure` for TLS and `verify` for certificate verification. Endpoint hostnames must match certificates. Mount CA files and use container paths. Client certificate authentication is not currently configurable.

## Notifications and debug archives

```yaml
environment: staging
notifications:
  enabled: true
  rate_limit: 1m
  teams:
    enabled: true
  slack:
    enabled: true
  telegram:
    enabled: true
```

Provide `NOTIFICATIONS_TEAMS_WEBHOOK_URL`, `NOTIFICATIONS_SLACK_WEBHOOK_URL`, `NOTIFICATIONS_TELEGRAM_BOT_TOKEN`, and `NOTIFICATIONS_TELEGRAM_CHAT_ID`. Providers can run together. Rate limiting is per notification kind; notifications do not have a durable outbox.

`CDC_DB_DEBUG=true` archives source event/task payloads in instance-specific `_migres_debug_*` tables. Restrict archive access to match source-data access. Debug archive mode is separate from logging verbosity. TTL cleanup follows ClickHouse background processing, not an immediate deletion deadline. Changed retention is applied to each existing owned archive on its next write, including after restart.

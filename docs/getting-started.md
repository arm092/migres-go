# Getting started

[Back to README](../README.md)

## Requirements

| Component | Requirement |
| --- | --- |
| Build | Go 1.26.9 |
| Runtime | Linux, Docker, or Kubernetes |
| MySQL | Tested on 5.7, 8.0, and 8.4; InnoDB tables |
| ClickHouse | Tested on 24.8 and 26.8; a dedicated Atomic database |
| State | Persistent storage with reliable exclusive file locking |
| Row identity | A real PK, including composite keys, or a configured full UNIQUE NOT NULL key |

Enable binary logging with `binlog_format=ROW` and `binlog_row_image=FULL`. Disable transaction compression and partial JSON binlog images on versions that support them. Online copying requires GTID or a prepared writable signal table, including in snapshot-only mode.

## Prepare source access

Illustrative DBA setup; replace the password and permitted client network:

```sql
CREATE USER 'migres'@'10.%' IDENTIFIED BY 'replace-with-a-real-password';
GRANT SELECT ON shop.* TO 'migres'@'10.%';
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'migres'@'10.%';
```

Without GTID, prepare the signal table and grant its write privileges:

```sql
CREATE TABLE shop._migres_signal (
    id VARCHAR(64) PRIMARY KEY,
    marker VARCHAR(64) NOT NULL
) ENGINE = InnoDB;
GRANT INSERT, UPDATE ON shop._migres_signal TO 'migres'@'10.%';
```

Alternatively, run `migres prepare-source --config config.yml` once with an account allowed to create that table. This command changes the source database. The normal account needs SELECT and replication access, plus signal-table INSERT/UPDATE in signal mode.

## Prepare ClickHouse

```sql
CREATE DATABASE analytics ENGINE = Atomic;
```

Use a separate database for the initial migration. The account needs access for SELECT, INSERT, CREATE, ALTER, RENAME/EXCHANGE, TRUNCATE, DROP, and the relevant system metadata. Replicated deployments also need the cluster privileges required by their ClickHouse installation.

Existing tables are accepted only when their generation ownership markers match the journal. Migres does not adopt unrelated tables with the same name.

## Build and run

```sh
go build -o bin/migres ./cmd/migres
cp config.yml.example config.yml
# Edit hosts, database names, filters, and a unique CDC server ID.
export MYSQL_PASSWORD='your-source-password'
export CLICKHOUSE_PASSWORD='your-target-password'
./bin/migres check --config config.yml
./bin/migres run --config config.yml
```

`check` opens the state directory exclusively and records source identity metadata. Run it before starting the service. For a running instance, use socket-based `status` instead of opening the journal again.

## Choose a mode

```yaml
migration:
  mode: cdc
  cdc:
    snapshot_before: true
```

This default copies existing data, reconciles concurrent writes, and continues CDC. Readiness becomes true after initial delivery.

- **Snapshot only:** `migration.mode: snapshot`; delivers the reconciled copy, then exits.
- **CDC only:** `mode: cdc` and `snapshot_before: false`; starts at the captured source boundary without copying existing rows.
- **Resume:** restart with the same state volume. Durable progress is reused.

Changing modes does not erase completed snapshot state. Request a resnapshot or reset for a fresh copy. A state directory belongs to one source.

## Verify

```sh
./bin/migres status --config config.yml
curl --fail http://127.0.0.1:8080/readyz
curl http://127.0.0.1:8080/metrics
```

```sql
SELECT * FROM analytics.orders FINAL WHERE _migres_deleted = 0;
```

Raw tables contain historical versions. Use the documented current-state query when comparing with MySQL. Import of legacy Python queues/state is not provided; start with a new snapshot into a separate target database.

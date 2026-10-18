# Runnable examples

[Back to README](../README.md)

## Local snapshot and CDC demo

From the repository root:

```sh
docker compose -f examples/compose.yml up -d --build --wait
curl --fail http://127.0.0.1:18080/readyz
```

The separate `migres-demo` project contains MySQL 8.4, ClickHouse 26.8, and Migres. Database ports are internal. Only the Migres HTTP endpoint is published on localhost port 18080. The volume and service names do not overlap the integration test project.

The initialization SQL creates a read-only replication user and three orders. Credentials named `demo-only-*` are public demo values. MySQL root is used below solely to generate sample writes; Migres does not need that account.

Read the initial copy:

```sh
docker compose -f examples/compose.yml exec clickhouse \
  clickhouse-client --user migres --password demo-only-clickhouse \
  --database analytics \
  --query 'SELECT id, customer_id, status, amount FROM orders FINAL WHERE _migres_deleted = 0 ORDER BY id'
```

Expected IDs are `1, 2, 3`. Change a value, change a primary key, and delete a row:

```sh
docker compose -f examples/compose.yml exec mysql \
  mysql -uroot -pdemo-only-root shop \
  -e "UPDATE orders SET status = 'paid' WHERE id = 1; UPDATE orders SET id = 20 WHERE id = 2; DELETE FROM orders WHERE id = 3;"
```

Repeat the target query until CDC catches up. Expected current rows:

| id | customer_id | status | amount |
| --- | --- | --- | --- |
| 1 | 101 | paid | 49.90 |
| 20 | 102 | paid | 120.00 |

Check queue/checkpoint status and restart with the same state:

```sh
docker compose -f examples/compose.yml exec migres \
  /usr/local/bin/migres status --config /app/config.yml
docker compose -f examples/compose.yml restart migres
curl --fail http://127.0.0.1:18080/readyz
```

Readiness may briefly return 503 during startup. The original demo data is initialized only when the MySQL volume is new. Ordinary restarts preserve both source data and Migres progress.

## Stop or reset the disposable demo

Stop while preserving state:

```sh
docker compose -f examples/compose.yml down
```

To delete **only this demo's data volumes** and return to its initial three orders:

```sh
docker compose -f examples/compose.yml down --volumes
```

## Replicated target

[replicated.yml](replicated.yml) is a fragment for a cluster you already operate. It is not a complete standalone configuration. Merge its `clickhouse` section into your config and provide all replica endpoints, cluster name, database, and credentials.

Keeper and every replica must be configured consistently. The `replica` macro must be distinct per replica. With two replicas and `quorum: auto`, both must acknowledge a write. For a runnable two-replica test topology, use [the verification fixtures](../docs/verification.md#replication-and-source-faults).

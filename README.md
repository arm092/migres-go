# Migres Go

**Keep ClickHouse in sync with MySQL using an online snapshot and continuous binlog replication.**

[![CI](https://github.com/arm092/migres-go/actions/workflows/ci.yml/badge.svg?branch=master)](https://github.com/arm092/migres-go/actions/workflows/ci.yml)
[![Go](https://img.shields.io/badge/Go-1.26.9-00ADD8?logo=go&logoColor=white)](go.mod)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Sponsor](https://img.shields.io/badge/Sponsor-%E2%99%A5-ea4aaa?logo=githubsponsors&logoColor=white)](https://github.com/sponsors/arm092)

Migres Go copies existing rows, streams committed changes, and resumes from durable state after a restart. It is intended for analytical replicas, reporting databases, and moving MySQL workloads into ClickHouse.

```text
MySQL -> Producer -> durable events -> Transformer -> durable tasks -> Consumer -> ClickHouse
```

**Project status:** early implementation with integration and fault recovery coverage. Production storage fencing, a completed continuous 24-hour acceptance run, and validation against your workload are release gates. See [verification](docs/verification.md) and the [roadmap](ROADMAP.md).

## Features

- Snapshot only, CDC only, or an online snapshot followed by continuous replication.
- Durable bbolt queues, checkpoints, schema history, and resumable operations.
- Full primary keys, including composite keys, or configured full `UNIQUE NOT NULL` keys.
- Inserts, updates, deletes, and key changes using row versions and deletion markers.
- Table filters, type conversion, DEFAULT handling, timezones, source character sets, and LowCardinality strings.
- Historical DDL handling and generation rebuilds for incompatible type or key changes.
- `ReplacingMergeTree` and `ReplicatedReplacingMergeTree`, with configurable quorum acknowledgement.
- File/position or GTID repositioning, reset, and table resnapshot through a local control socket.
- Graceful shutdown, persistent Docker storage, Kubernetes manifests, and Prometheus metrics.
- Optional debug archives and Teams, Slack, and Telegram notifications.

## Try it locally

The demo starts isolated MySQL and ClickHouse services, creates sample orders, and runs Migres. It uses publicly known demo credentials. Only the Migres HTTP endpoint is published, on `127.0.0.1:18080`.

```sh
git clone https://github.com/arm092/migres-go.git
cd migres-go
docker compose -f examples/compose.yml up -d --build --wait
curl --fail http://127.0.0.1:18080/readyz
```

Read current rows:

```sh
docker compose -f examples/compose.yml exec clickhouse \
  clickhouse-client --user migres --password demo-only-clickhouse \
  --database analytics \
  --query 'SELECT id, customer_id, status, amount FROM orders FINAL WHERE _migres_deleted = 0 ORDER BY id'
```

Change a source row, then repeat the read:

```sh
docker compose -f examples/compose.yml exec mysql \
  mysql -uroot -pdemo-only-root shop \
  -e "UPDATE orders SET status = 'paid' WHERE id = 1;"
```

See [the complete demo](examples/README.md) for deletes, key changes, status, and cleanup. CDC is asynchronous; repeat the query after the queues catch up.

## Use your own databases

Requires Go **1.26.9** to build. Runtime deployment targets Linux, Docker, and Kubernetes. Windows is supported for development; exercise Unix socket control and Linux signals on Linux.

```sh
go build -o bin/migres ./cmd/migres
cp config.yml.example config.yml
# Edit hosts, database names, filters, and a unique CDC server ID.
export MYSQL_PASSWORD='your-source-password'
export CLICKHOUSE_PASSWORD='your-target-password'
./bin/migres check --config config.yml
./bin/migres run --config config.yml
```

Create a dedicated ClickHouse target database with `ENGINE = Atomic` first. The source needs InnoDB tables, ROW/FULL binlog images, and a stable unique row identity. Online copying requires GTID or a writable prepared signal table. [Setup and permissions](docs/getting-started.md) explain the prerequisites.

## Read current state correctly

```sql
SELECT id, customer_id, status, amount
FROM analytics.orders FINAL
WHERE _migres_deleted = 0;
```

Raw tables retain row versions and deletion markers. Use `FINAL` and the deletion filter for current-state reads. Delivery can be retried; Migres does not promise a globally atomic snapshot, cross-table transaction visibility, or simultaneous publication on every replica. [Data guarantees](docs/architecture.md) describe the contract.

## Documentation

| Guide | Contents |
| --- | --- |
| [Getting started](docs/getting-started.md) | Prerequisites, permissions, build, and first run |
| [Configuration](docs/configuration.md) | YAML, environment variables, filters, tuning, TLS, notifications |
| [Architecture and guarantees](docs/architecture.md) | Stages, durable handoffs, snapshots, DDL, replication |
| [Operations and recovery](docs/operations.md) | Status, reposition, reset, resnapshot, signals, troubleshooting |
| [Docker and Kubernetes](docs/deployment.md) | Persistent volumes, manifests, probes, upgrades, metrics |
| [Verification](docs/verification.md) | Unit, integration, fault, soak, and performance checks |
| [Runnable examples](examples/README.md) | Local demo and replicated target configuration |
| [Roadmap](ROADMAP.md) | Release gates and planned improvements |

## Contributing

Bug reports should include database versions, redacted configuration, triggering DDL, operation status, and a minimal reproduction. Do not include passwords, row payloads, private keys, or production debug archives. See [CONTRIBUTING.md](CONTRIBUTING.md).

## Support development

If Migres helps your work, [sponsor Arman on GitHub](https://github.com/sponsors/arm092). Sponsorship supports maintenance, recovery testing, and compatibility work.

[![Sponsor on GitHub](https://img.shields.io/badge/Sponsor_on_GitHub-Support_Migres-ea4aaa?style=for-the-badge&logo=githubsponsors&logoColor=white)](https://github.com/sponsors/arm092)

## License

[MIT](LICENSE).

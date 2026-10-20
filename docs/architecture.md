# Architecture and data guarantees

[Back to README](../README.md)

```mermaid
flowchart TD
    M[MySQL snapshot and binlog] --> P[Producer]
    P --> E[Durable event queue]
    E --> T[Transformer]
    T --> J[Durable task queue]
    J --> C[Consumer]
    C --> H[ClickHouse]
    K[Coordinator and bbolt state] --- P
    K --- T
    K --- C
```

## Stage ownership

| Stage | Responsibility | Durable boundary |
| --- | --- | --- |
| Producer | Read source changes and historical DDL | Committed events and checkpoint become durable together |
| Transformer | Convert events into typed tasks | Tasks and acknowledgement of events are one journal transaction |
| Consumer | Deliver in table order | Acknowledges after execution; unresolved outcomes stay replayable |
| Coordinator | Snapshot, DDL barriers, manual operations | Operation and generation progress survive restarts |

Transformer does not write to ClickHouse. Independent tables can be delivered in parallel; each table keeps its order and DDL forms a delivery barrier. In-flight tasks are not dispatched twice by the same Consumer.

bbolt stores queues, checkpoints, schemas, version identities, snapshot progress, ownership, and operations. A journal has one writer process. Its storage must support exclusive locking; Kubernetes also needs storage fencing to prevent competing owners after a node failure.

## Delivery and identity

Transport delivery can repeat after a crash or a lost response. Tasks retain their payload, versions, and deduplication identity during retries. Versions come from persisted source ordering, not wall-clock time.

The full primary key identifies a row. A fallback key must be a configured full UNIQUE NOT NULL constraint. A key update emits a deletion marker for the old key and an insertion for the new one.

Once queues catch up, replacing-engine current-state reads converge with the supported source representation:

```sql
SELECT * FROM analytics.orders FINAL WHERE _migres_deleted = 0;
```

No automatic `OPTIMIZE FINAL` is used. Plain MergeTree is an explicit append-only mode requiring version-aware aggregation. Cross-table read atomicity is not guaranteed.

## Online snapshot

CDC starts before copying. Reads use bounded keyset pages and binlog windows; CDC versions take precedence over baseline rows. Progress is saved between completed chunks. An unfinished chunk is reread.

`auto` chooses GTID when available, otherwise a writable signal table. No global write lock or database-wide instantaneous snapshot is promised. Only InnoDB is supported. Queries and normal metadata-lock waits have timeouts.

## Schema changes

Historical rows use persisted schemas instead of current INFORMATION_SCHEMA metadata. CREATE, DROP, TRUNCATE, RENAME, column changes, ALTER COLUMN SET/DROP DEFAULT, and compound ALTER statements including table rename are parsed from binlog SQL. Unsupported DDL blocks the stream.

Compatible changes apply directly. Incompatible type/key changes and ADD COLUMN with a nonconstant DEFAULT build a new generation, copy actual source values, catch up CDC, and publish through `EXCHANGE TABLES` in an Atomic database.

Name intervals distinguish a renamed table from a new table using its old name. Chained renames retain pair identities and acknowledgements. Unknown outcomes are checked against actual ownership markers before retry.

A compound ALTER with table rename remains one ordered delivery barrier. The target name transition uses a persisted generation plan, followed by resumable column steps under the new name. Compatible changes retain the generation and Keeper path. Incompatible changes use the existing rebuild protocol after the rename. If an earlier rebuild already copied the compound ALTER's final source shape, the journal preserves its naming barrier and records that its column changes were absorbed; replay does not DROP/ADD those columns a second time. Initial snapshots invalidated by rename are reconciled before readiness.

### DEFAULT changes

```sql
-- MySQL source statements:
ALTER TABLE orders ALTER COLUMN status SET DEFAULT 'new';
ALTER TABLE orders ALTER COLUMN status DROP DEFAULT;
ALTER TABLE orders ADD COLUMN priority INT NOT NULL DEFAULT 0,
  RENAME TO orders_v2;
```

Setting a default does not change the source type or existing values. Migres retains the historical default SQL and translates numeric/text literals, bit/hex literals, NULL, and CURRENT_TIMESTAMP into safe ClickHouse metadata. Bit defaults remain numbers; binary defaults retain their actual bytes, including zero bytes, rather than a textual `0x...` representation. Other expressions remain evaluated by MySQL and are delivered as explicit row values; they are not copied verbatim into ClickHouse SQL. Adding a column with a nonconstant default still requires a source-backed rebuild to copy its actual backfill.

ClickHouse default syntax differs from MySQL. Migres emits `MODIFY COLUMN name <explicit type> DEFAULT expression` and `MODIFY COLUMN name REMOVE DEFAULT`. It never inserts `SET` before DEFAULT: ClickHouse treats `SET` as a UInt64 datatype alias, so `MODIFY COLUMN c SET DEFAULT 6` changes an Int64 column to UInt64 and can wrap negative values. This was reproduced on both tested ClickHouse versions. See the [column syntax](https://clickhouse.com/docs/reference/statements/alter/column) and [official alias registration](https://github.com/ClickHouse/ClickHouse/blob/v24.8.14.39-lts/src/DataTypes/DataTypesNumber.cpp#L86).

ClickHouse can also calculate added columns lazily for old parts. Before changing/removing defaults, Migres freezes their existing values through synchronous column materialization; added-column backfills are frozen before the next step. Explicitly supplied source values remain unchanged. A durable per-step freeze acknowledgement prevents replay after a lost DEFAULT response from applying a new expression to old rows. This work can scan existing parts and increase DDL latency; it is not an instant metadata-only operation. Each expected replica's final type/default metadata is checked before acknowledging the step.

## Replication and failures

The supported topology is one shard with multiple replicas. INSERT connects to one available endpoint. Acknowledgement is `single` or configured quorum; `auto` means majority. Quorum does not imply simultaneous readability on every replica.

Keeper paths contain stable generation IDs independent of SQL names. Cluster DDL is verified on expected replicas. Exchanges inspect each endpoint and resume partial publication; switching is atomic per server.

Temporary failures retry with backoff and jitter. Permanent failures stop delivery and preserve the task. Queue and free-space thresholds apply backpressure, while history currently remains for the journal lifetime. A transaction over the staging limit does not advance its checkpoint.

Missing binlogs, unsafe schema rewinds, foreign ownership, mixed-database DDL, and unrecognized ALTER operations are rejected rather than guessed. See [operations](operations.md) and [the roadmap](../ROADMAP.md).

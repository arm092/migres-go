# Operations and recovery

[Back to README](../README.md)

## CLI

| Command | Effect |
| --- | --- |
| `run` | Start the service and local control socket |
| `check` | Preflight connections/source identity; exclusively opens state |
| `prepare-source` | Create the dedicated source signal table if absent |
| `status` | Read queue/checkpoint diagnostics or operation status |
| `reposition` | Coordinated, validated change of start position |
| `resnapshot` | Fresh instance copy, or queued rebuild of one owned table |
| `reset` | Remove owned target objects and reset replication state |
| `backup --output PATH` | Export a checked offline journal snapshot to a new file |
| `compact --output PATH` | Export live journal pages to a new, smaller file |
| `restore --input PATH` | Restore a checked journal into an empty data directory |

All commands accept `--config`. Control commands contact the running process through `runtime_dir/control.sock`; they do not open its journal as another writer.

Backup, compact, and restore are offline maintenance commands. Stop Migres first. They do not connect to MySQL or ClickHouse, require exclusive ownership of the source journal, and refuse an existing output file. They check bbolt page integrity and required journal buckets. They are not a source/target database backup or a journal format migration tool.

```sh
migres status --config config.yml
migres status --config config.yml --operation-id recovery-001
```

## Reposition

Use an exact transaction boundary and a stable operation ID:

```sh
migres reposition --config config.yml \
  --file mysql-bin.000123 --pos 456 --operation-id recovery-001
```

Or use an actual GTID set captured for your source:

```sh
migres reposition --config config.yml \
  --gtid-set '24bc7850-2c16-11e6-a073-0242ac110002:1-123' \
  --operation-id recovery-002
```

File/position and GTID flags are mutually exclusive. Migres checks the source, retained history, and boundary validity, gracefully pauses workers, applies the operation, and resumes. A GTID set needs an exact retained boundary mapping. In GTID resume mode, a file/position also needs its retained GTID mapping.

By default pending work is delivered before the move. Explicit `--discard-pending` archives it and removes it from active queues. Explicit `--allow-gap` is required for a forward jump; it records lost completeness until verification or a new snapshot. A forward gap containing unretained selected-table DDL is rejected.

Rewinding replays changes; it does not undo ClickHouse to an earlier time. Rewinds across DDL, positions before retained history, or unavailable binlogs can require resnapshot.

Retry an ambiguous connection result with the **same operation ID and arguments**. Reusing an ID for another request is rejected. Inspect its status before submitting a different recovery operation.

## Resnapshot and reset

```sh
migres resnapshot --config config.yml --table orders --operation-id orders-copy-001
migres status --config config.yml --operation-id orders-copy-001
```

A table resnapshot returns `queued`; completion becomes `done` after publication. Readiness stays false while rebuilding. The table must belong to this instance.

```sh
migres resnapshot --config config.yml --operation-id full-copy-001
migres reset --config config.yml --operation-id reset-001
```

A full resnapshot and reset remove registered owned objects and local replication state, then start the configured mode afresh. Ownership markers are verified; unrelated tables are preserved. Applying operations can resume after interruption. Ensure `snapshot_before: true` or snapshot mode when recovery requires recopying existing data.

## Signals and shutdown

| Signal | Action |
| --- | --- |
| SIGTERM / SIGINT | Disable readiness and stop Producer, Transformer, then Consumer |
| SIGUSR1 | Request reset |
| SIGUSR2 | Request the configured forced file/position |

```yaml
migration:
  cdc:
    force_binlog_position: mysql-bin.000123:456
shutdown_timeout: 60s
```

The forced setting is applied on SIGUSR2, not on ordinary restart. Signals use the same validation and default pending-work policy as CLI control. They do not bypass gap safeguards.

Shutdown preserves committed events, finishes Transformer handoffs, and waits for in-flight delivery within the deadline. It does not drain the entire backlog. Unacknowledged work remains for replay.

Cancellation is checked between transformed rows/events and before the atomic handoff. An individual key conversion, serialization, filesystem transaction, or unresponsive driver/kernel call can still delay cancellation. Docker's termination grace remains the final process deadline; committed state is recovered after a forced kill.

## Offline backup, restore, and compaction

Use the same Migres binary/configuration version for backup and restore. No cross-version journal migration is promised. Config files, credentials, CA files, MySQL binlogs, and ClickHouse data must be protected separately. The exported journal contains source rows and must be treated as sensitive data.

Stop Migres, then export its state. The output directory must already exist and have room for another journal copy:

```sh
migres backup --config config.yml --output /backups/migres-001.db
migres compact --config config.yml --output /backups/migres-001-compact.db
```

For Docker Compose, stop the service and run a maintenance container sharing its volume. This example keeps the copy on that volume; move it to independent protected storage for an actual backup:

```sh
docker compose stop migres
docker compose run --rm --no-deps --entrypoint /usr/local/bin/migres migres \
  backup --config /app/config.yml --output /app/data/migres-001.db
```

Restore into a configured **empty** `data_dir`:

```sh
migres restore --config restore.yml --input /backups/migres-001.db
migres run --config restore.yml
```

Keep the old journal until recovery is verified. A compacted export can be used as restore input. The tool never replaces the live journal in place. Output is published atomically without overwrite; Linux file and directory metadata are synced. An interrupted export may leave an unpublished `.migres-journal-*` temporary file; it is not a valid published backup.

The verified local recovery scenario stops Migres, copies its journal, leaves ClickHouse at that stopped point, allows new MySQL changes, and resumes from the restored journal. Preserve source UUID, database identity, instance ownership, generations, and required binlogs. A journal-only restore against an arbitrarily newer/older ClickHouse database is **not** a point-in-time restore: schema generations and DDL may no longer agree. For disaster recovery, coordinate a stopped journal backup with a ClickHouse backup of the same stopped state. If that state or required binlogs cannot be recovered, use a fresh snapshot in a separate target database.

## Journal growth

Delivered events/tasks are removed and their bbolt pages are reused. The file keeps its high-water allocation until offline compaction. Compaction removes unused pages, not replay history. A local eight-burst test reused a 4 MiB journal; after retaining 2,000 source boundaries and versions, compaction reduced the file to 512 KiB while preserving replay lookup.

Version mappings, source boundaries, schema history, operation records, and explicitly discarded work are intentionally retained. They continue to grow with traffic even when queues are empty. There is no automatic history pruning or fixed total journal-size guarantee. Removing those records blindly would change versions or make rewind/rebuild unsafe.

Monitor `migres_storage_bytes`, `migres_journal_reusable_bytes`, `migres_journal_history_bytes`, and `migres_journal_history_entries` alongside disk free space. History bytes count live bucket data, not the whole allocated file. `max_queue_bytes` limits active queues; it does not cap retained history. `min_free_disk_bytes` pauses ingestion near the disk reserve. Size storage for history plus outage backlog, exports/compaction, and the largest transaction. Use compaction for free pages; expand storage or plan a new snapshot/generation for retained-history growth.

## Troubleshooting

| Symptom | Check / recovery |
| --- | --- |
| Live but not ready | Initial copy/rebuild may be active; inspect status and logs for a blocking error |
| Journal already locked | Locate the existing owner; do not force another owner onto the same storage |
| Missing binlog | Retention was insufficient; recover through an explicit fresh snapshot |
| Unsupported DDL | Preserve the event and reproduce the SQL; do not skip it by editing the journal |
| Rebuild capacity error | Increase queue/disk capacity and restart using the same state |
| Quorum unavailable | Check replica health and Keeper; tasks remain pending |
| Unexpected duplicates | Query with `FINAL` and `_migres_deleted = 0`; ordinary reads expose versions |
| Unknown CA | Check mounted CA files and endpoint certificate hostnames |
| Foreign ownership error | Use a dedicated target database; investigate ownership rather than deleting markers |

Retain the state volume on upgrades. For a backup, stop the owner before copying the journal or use storage snapshots with a verified consistency procedure. A raw copy of a live mmap-backed file is not a documented backup method.

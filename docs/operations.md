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

All commands accept `--config`. Control commands contact the running process through `runtime_dir/control.sock`; they do not open its journal as another writer.

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

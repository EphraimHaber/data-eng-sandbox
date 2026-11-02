---
title: Table maintenance
description: Expiring snapshots and compacting the small files that frequent syncs create.
sidebar:
  order: 3
---

Iceberg never changes a file once it's written. Every commit adds new files and a new snapshot, and the
old ones stay around so readers and time travel keep working. That's what makes commits atomic, and it's
also why a table that's written every ten minutes needs regular cleanup.

## Why ten-minute syncs need it

At one commit every ten minutes, the table gets **144 snapshots a day**. Each one adds:

- **Small data files.** The generator's updates hit orders from every one of the last 30 days, so each
  sync touches nearly every day-partition. In this sandbox, that's about **31 new data files and 31 new
  delete files per run**, many of them a few kilobytes.
- **A manifest** listing those files, plus a manifest list and a `metadata.json`.

Left alone, that hurts in three ways:

1. **Reads slow down.** A query opens many small Parquet files, each with its own fixed overhead, and has
   to apply every delete file to the data files it covers. This is the classic "small files problem" of
   data lakes.
2. **Planning slows down.** Before the first task can run, the driver reads every manifest to find out
   which files to scan.
3. **Storage grows forever.** Old snapshots keep every replaced file reachable, and nothing is deleted on
   its own.

## The maintenance DAG

`iceberg_maintenance` runs daily at 03:17 UTC as four tasks. Each task is its own Spark application
(`jobs/maintain_orders.py`), so a failure retries on its own and shows up clearly in the grid view:

```
compact_data_files  >>  rewrite_manifests  >>  expire_snapshots  >>  remove_orphan_files
```

Each step calls an Iceberg stored procedure. They run as normal distributed Spark jobs: executors read and
rewrite files, and the driver only plans and commits.

### 1. Compact data files

```sql
CALL lake.system.rewrite_data_files(
  table   => 'lake.sales.orders',
  options => map(
    'min-input-files',          '2',
    'delete-file-threshold',    '1',
    'remove-dangling-deletes',  'true',
    'partial-progress.enabled', 'true'
  )
)
```

Bin-packs small files into fewer files close to the target size, one partition at a time.

- **`min-input-files = 2`:** a partition with two or more small files is worth rewriting. The default of
  5 would skip most day-partitions here.
- **`delete-file-threshold = 1`:** a data file with even one delete marker is rewritten, so merge-on-read
  deletes are applied for good and readers stop paying for them.
- **`remove-dangling-deletes`:** after their data files are rewritten, delete files point at files that
  are no longer live. They're harmless, but they stay tracked until this option drops them.
- **`partial-progress`:** commits in batches, so one conflicting concurrent write doesn't throw away the
  whole compaction.

Bin-packing needs no shuffle: each group of files is read and rewritten by a single task.
`strategy => 'sort'` would add one, to cluster rows by a column.

### 2. Rewrite manifests

```sql
CALL lake.system.rewrite_manifests(table => 'lake.sales.orders')
```

Merges the many small manifests (one or more per commit) into a few, grouped by partition, so query
planning reads a handful of files instead of hundreds.

### 3. Expire snapshots

```sql
CALL lake.system.expire_snapshots(
  table       => 'lake.sales.orders',
  older_than  => TIMESTAMP '<now minus 24 hours>',
  retain_last => 10
)
```

Removes snapshots older than 24 hours, always keeping at least the last 10, and **deletes every file that
only those snapshots referenced**. That includes the small files compaction just replaced. This is the step
that actually frees storage, and it's irreversible: you can no longer time travel past the cutoff.

### 4. Remove orphan files

```sql
CALL lake.system.remove_orphan_files(
  table          => 'lake.sales.orders',
  older_than     => TIMESTAMP '<now minus 72 hours>',
  prefix_listing => true
)
```

Orphan files are files under the table's location that no metadata references at all. They're typically
debris from a task that wrote files and then died before committing. The 72-hour threshold has to be longer
than any write can take, or the procedure could delete the files of a commit that's still in flight.

`prefix_listing => true` lists the location with Iceberg's own S3 client. Without it, the procedure goes
through Hadoop's FileSystem API, and fails here with `No FileSystem for scheme "s3"`, because the stack
deliberately ships no `hadoop-aws`.

## Why the order matters

Compaction makes the old small files unreferenced **by the current snapshot**, but the older snapshots
still point at them. Expiring those snapshots is what lets Iceberg delete the files. Run expiry first and
the next compaction's leftovers wait a whole extra day.

## What it looks like

After about 35 minutes of syncing, one manual maintenance run produced:

| Step | Result |
| --- | --- |
| `compact_data_files` | 155 data files rewritten into 31 (one per day-partition); all 58 delete files dropped |
| `rewrite_manifests` | 16 manifests turned into 1 |
| `expire_snapshots` | Nothing to expire: every snapshot was younger than 24 hours |
| `remove_orphan_files` | No orphans |

To watch it yourself, compare the file counts before and after:

```sh
docker compose exec spark-master spark-sql -e \
  "SELECT content, count(*) FROM sales.orders.files GROUP BY content"
docker compose exec airflow-scheduler airflow dags trigger iceberg_maintenance
```

To see expiry delete files without waiting a day, run that step by hand with a zero-hour retention:

```sh
docker compose exec airflow-scheduler spark-submit --conf spark.driver.host=airflow-scheduler \
  /opt/airflow/jobs/maintain_orders.py --action expire-snapshots --retain-hours 0 --retain-last 2
```

In testing, that deleted 54 superseded data files. It also throws away your time-travel history, so only do
it in the sandbox.

## Running alongside the sync

All four tasks share the one-slot `iceberg_orders` pool with the sync. The pool serializes **tasks**, not
DAG runs, so a sync can run between two maintenance steps. That's fine: each step is its own atomic
commit, and compaction's partial progress keeps conflicts small. A sync during the 03:17 window may just
wait a minute for the slot.

## Settings that help on their own

The table's properties cut some of the growth before maintenance runs:

- `write.metadata.delete-after-commit.enabled = true` with `write.metadata.previous-versions-max = 50`:
  keep only the last 50 `metadata.json` files instead of all 144 a day.
- `write.merge.mode = merge-on-read`: write small delete files instead of rewriting data files on every
  sync, and let compaction do the rewriting once a day.

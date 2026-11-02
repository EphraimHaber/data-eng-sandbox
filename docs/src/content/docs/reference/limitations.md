---
title: Limitations and test status
description: What this sandbox deliberately leaves out, and what has and hasn't been verified.
sidebar:
  order: 3
---

## Known limitations

### `catchup=False` can skip windows

If Airflow is down or the DAG is paused between 08:00 and 09:00, those windows never run, and the changes
made in them stay unsynced until the rows change again. Close the gap with `orders_full_load`, or see
[Airflow DAGs](/data-eng-sandbox/pipeline/dags/#catchup-and-gaps) for automatic options.

### Late-committing transactions

`updated_at` is set when a transaction *writes* a row, but the row only becomes visible when the
transaction *commits*. A transaction that updates a row at 06:49 and commits at 06:51 is invisible to the
06:40–06:50 run, which ran before the commit, and its timestamp is outside the 06:50–07:00 window. That
change is lost.

The change generator commits instantly, so the sandbox doesn't hit this, but real systems with long
transactions do. The usual fix is to read a small overlap, for example
`updated_at >= start - 5 minutes`. The MERGE makes re-reading rows harmless.

### Hard deletes aren't captured

See [Hard deletes](/data-eng-sandbox/pipeline/deletes/).

### The parallel read isn't one consistent snapshot

Each JDBC slice is a separate query in its own transaction, run at slightly different times. That's fine for
upserts keyed on `id`, because every id lives in exactly one slice. A job that needed a consistent
cross-table snapshot of the source would need a different approach, such as reading from a replica at a
fixed point, or CDC.

### No-op runs still commit

A MERGE that matches nothing still creates an empty snapshot. That's harmless, and expiry removes it.

### The REST catalog is a reference server

`apache/iceberg-rest-fixture` is the Iceberg project's reference and test implementation, a thin REST layer
over a SQLite-backed JDBC catalog. For anything beyond learning, use Apache Polaris, Lakekeeper, Nessie,
Unity Catalog or AWS Glue. They speak the same REST protocol, so the Spark configuration barely changes.

### Security is off

- Airflow has no login (`SIMPLE_AUTH_MANAGER_ALL_ADMINS`).
- Credentials are static values in a committed `.env`.
- Everything uses plain HTTP, and `spark.authenticate` is off.
- The JDBC password travels to the executors inside the serialized task plan.

That's fine on a laptop and nowhere else.

### No Spark History Server

The driver UI at `:4040` disappears when a job ends. Enabling `spark.eventLog.enabled`, writing the event
logs to the bucket, and adding a history-server container is a good next exercise.

## What was tested

All of this ran on Docker Engine 29.9 on Linux (amd64, 4 CPUs, 16 GB RAM), one stage at a time.

**1. Containers up and talking.**
- The source DB was seeded with 100k rows, and the change generator was writing to it.
- `create-bucket` created `s3://warehouse`, and the REST catalog was healthy.
- Both workers registered with the master: 4 cores and 4 GB in total.

**2. Spark to Iceberg.**
- `spark-sql` created, wrote and read a table through the REST catalog, and the Parquet and metadata files
  appeared in RustFS.
- `spark-submit` from the Airflow image ran the full load as 8 JDBC slices on 4 executors across both
  workers.
- The same incremental window, run twice, wrote 88 rows the first time and **0** the second.

**3. Airflow.**
- After `docker compose down -v`, the first scheduled sync bootstrapped the table with a full load, and
  later runs were incremental.
- With the change generator stopped, `verify_sync.py` reported source and Iceberg **identical**: 100,930
  rows, with 0 missing, extra, duplicate or differing.
- `orders_full_load` on a populated table wrote only the 307 rows that had changed.
- Manual syncs worked with and without a logical date, through the CLI and through the REST call the UI's
  trigger form makes.
- The maintenance DAG compacted 155 data files into 31 and dropped 58 delete files.
- Every Spark SQL example in these docs was run.

## What wasn't tested

- **Docker Desktop on macOS or Windows, and ARM hosts such as Apple Silicon.** Every base image publishes
  a `linux/arm64` build, but the stack has only run on Linux amd64.
- **The Airflow UI in a browser.** It served pages, and triggering used the same REST endpoint the UI form
  calls, but nobody clicked through it.
- **`docker compose up --build` exactly as written.** The test machine sits behind a TLS-intercepting
  proxy, so the Airflow image's `pip install` step was built from a base image with that proxy's CA added.
  The Dockerfiles in the repository contain nothing specific to that environment.
- **Long-running behaviour.** The 03:17 maintenance schedule wasn't observed firing on its own, and snapshot
  expiry was exercised with a zero-hour retention rather than after a real 24 hours.

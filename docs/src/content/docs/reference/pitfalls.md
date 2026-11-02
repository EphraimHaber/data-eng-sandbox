---
title: Pitfalls
description: The traps this setup works around, what each one looks like when it bites, and the fix.
sidebar:
  order: 2
---

Each of these is easy to hit when wiring Spark, Iceberg and Airflow together in Docker. For each, the page
gives the symptom you'd see and how the sandbox avoids it.

## The driver lives in the Airflow container

**Symptom:** the job hangs at *"Initial job has not accepted any resources"*, or executors die with
connection timeouts to some address like `172.18.0.9` or a container ID.

**Cause:** PySpark on a standalone cluster only runs in **client** deploy mode, so the driver is the
`spark-submit` process inside `airflow-scheduler`. Executors don't just receive work; they open
connections **back** to the driver, so the driver has to advertise an address they can reach.

**Fix,** in `dags/sandbox_common.py`:

```python
SPARK_DRIVER_CONF = {
    "spark.driver.host": "airflow-scheduler",   # the name it ADVERTISES to executors
    "spark.driver.bindAddress": "0.0.0.0",      # the interface it LISTENS on
    "spark.driver.port": "7078",                # fixed instead of random
    "spark.blockManager.port": "7079",
}
```

- **Resolvable from the workers.** `airflow-scheduler` is a service name on the shared Docker network.
  `localhost` would point each executor at itself.
- **A fixed `hostname:`.** The compose service sets `hostname: airflow-scheduler`, so the name the
  container uses for itself matches its DNS name.
- **Tasks run in the scheduler.** With the LocalExecutor every task does, so this is the only place a
  driver ever starts.

If you run `spark-submit` by hand, do it in `airflow-scheduler` and pass
`--conf spark.driver.host=airflow-scheduler`.

## Jars on the driver and on the executors

**Symptom:** `ClassNotFoundException: org.apache.iceberg.spark.SparkCatalog` on the driver, or
`No suitable driver` / `ClassNotFoundException: org.postgresql.Driver` inside an executor task.

**Cause:** the driver needs the Iceberg classes to plan the MERGE, and the JDBC driver for the bounds
query. The executors need all three jars to read Postgres and write Iceberg files. Putting them on only one
side fails on the other.

**Fix:** the same three jars sit at `/opt/spark-extra-jars/` in both images, and
`spark.driver.extraClassPath` and `spark.executor.extraClassPath` add them when each JVM starts. See
[Pinned versions](/data-eng-sandbox/reference/versions/#how-the-jars-get-into-the-images).

## Version drift between driver and executors

**Symptom:** `InvalidClassException` or `NoSuchMethodError` during task serialization, or *"Python in
worker has different version 3.10 than that in driver 3.12"* the first time someone adds a UDF.

**Fix:** both images copy Java and Spark from the same official image tag, and the cluster image is built
on Python 3.12 to match Airflow. There's exactly one Spark, Java and Python minor version in the stack.

## S3-compatible storage isn't AWS

**Symptom:** `UnknownHostException: warehouse.rustfs`, requests going to `s3.amazonaws.com`, or
`SdkClientException: Unable to load region`.

**Cause:** the AWS SDK assumes AWS. By default it builds virtual-host style URLs
(`http://warehouse.rustfs:9000/key`), which would need wildcard DNS, and it wants a region.

**Fix:**

```properties
spark.sql.catalog.lake.io-impl              org.apache.iceberg.aws.s3.S3FileIO
spark.sql.catalog.lake.s3.endpoint          http://rustfs:9000
spark.sql.catalog.lake.s3.path-style-access true
```

plus `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` and `AWS_REGION` as environment variables on every
container that touches S3. Path style means URLs like `http://rustfs:9000/warehouse/key`.

**Configure the catalog server too.** The REST catalog writes each new `metadata.json` itself, so
`iceberg-rest` gets the same settings through its environment: `CATALOG_S3_ENDPOINT`,
`CATALOG_S3_PATH__STYLE__ACCESS` and `CATALOG_IO__IMPL`. Setting them only on the Spark side gives a
confusing failure at commit time.

## `remove_orphan_files` and `s3://`

**Symptom:** `UnsupportedFileSystemException: No FileSystem for scheme "s3"`, from orphan removal only.
Everything else works.

**Cause:** that procedure lists files through Hadoop's FileSystem API by default, and this stack ships no
`hadoop-aws`.

**Fix:** `prefix_listing => true`, which lists with Iceberg's own `S3FileIO`.

## Airflow 3 data intervals

**Symptom 1:** every sync reads 0 rows, and the log shows `start` equal to `end`.
**Cause:** in Airflow 3, `schedule="*/10 * * * *"` means `CronTriggerTimetable`, whose runs have
zero-length data intervals.
**Fix:** `schedule=CronDataIntervalTimetable("*/10 * * * *", timezone="UTC")`.

**Symptom 2:** a manually triggered sync fails three times with
`UndefinedError: 'data_interval_start' is undefined`.
**Cause:** a manual run with no logical date has no data interval in Airflow 3.
**Fix:** the `window_bound()` macro falls back to the latest complete interval. See
[Airflow DAGs](/data-eng-sandbox/pipeline/dags/#manual-runs-and-window_bound).

## The plain JDBC read

**Symptom:** the read stage in the Spark UI has **one** task, and three of the four executors sit idle.

**Fix:** `partitionColumn`, `lowerBound`, `upperBound` and `numPartitions`, with the bounds taken from a
`min`/`max` query. See [The sync job](/data-eng-sandbox/pipeline/sync-job/#parallel-jdbc-reads).

## Appending instead of merging

**Symptom:** `count(*)` is larger than `count(DISTINCT id)` after a retry.

**Fix:** `MERGE INTO … ON t.id = s.id` with an `updated_at` guard. See
[The sync job](/data-eng-sandbox/pipeline/sync-job/#idempotent-writes-with-merge).

## Dangling delete files after compaction

**Symptom:** after `rewrite_data_files`, `sales.orders.files` still lists position-delete files, even
though every row they point at was already rewritten.

**Fix:** the `remove-dangling-deletes` compaction option. See
[Table maintenance](/data-eng-sandbox/operations/maintenance/#1-compact-data-files).

## Standalone mode takes every core

**Symptom:** a second Spark application, such as a hand-run `spark-sql`, sits at *"waiting for
resources"* while a sync runs.

**Cause:** by default, a standalone master gives an application every free core.

**Fix:** `spark.cores.max 4` makes the cap explicit, and the one-slot `iceberg_orders` Airflow pool keeps
the scheduled jobs from competing. Lower `spark.cores.max` to 2 if you want two applications side by side.

## Smaller ones

- **`spark-sql` and Derby.** The CLI enables Hive support and creates a local `metastore_db/` in the
  current directory. The cluster image's working directory is writable (`/opt/spark/work-dir`) for that
  reason. Nothing is stored there; the tables live in the REST catalog.
- **`ADD --chmod` on a new directory.** Docker applies the mode to the directory it creates as well, and a
  `644` directory can't be entered. The Dockerfiles create `/opt/spark-extra-jars` first.
- **Seeding and healthchecks.** Postgres runs `/docker-entrypoint-initdb.d` scripts on a temporary server
  that only listens on a Unix socket. The healthcheck uses `pg_isready -h 127.0.0.1`, so the database only
  counts as healthy once seeding has finished.
- **Short table names in procedures.** `table => 'sales.orders'` works, but Iceberg first tries `sales` as
  a catalog and logs a `CATALOG_NOT_FOUND` stack trace. The jobs always pass `lake.sales.orders`.

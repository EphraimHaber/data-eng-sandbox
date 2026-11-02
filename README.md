# Data-engineering sandbox: Postgres → Spark → Iceberg, orchestrated by Airflow

📖 **Docs site:** <https://ephraimhaber.github.io/data-eng-sandbox/> (source in [`docs/`](docs/)).

A local, laptop-sized stack for learning how these tools fit together. A Postgres
`orders` table keeps changing. Every 10 minutes Airflow starts a Spark job that reads
the rows that changed, in parallel, and upserts them into an Apache Iceberg table
stored on S3-compatible object storage (RustFS).

```
docker compose up -d --build     # then open http://localhost:8080
```

- [Architecture: which process runs where](#architecture-which-process-runs-where)
- [Versions and why they fit together](#versions-and-why-they-fit-together)
- [Starting the sandbox](#starting-the-sandbox)
- [How the sync works](#how-the-sync-works)
- [Triggering and watching a run](#triggering-and-watching-a-run)
- [Verifying the results (Spark SQL)](#verifying-the-results-spark-sql)
- [Table maintenance](#table-maintenance)
- [Pitfalls this setup handles](#pitfalls-this-setup-handles)
- [What this approach does not capture: deletes](#what-this-approach-does-not-capture-deletes)
- [Known limitations and next steps](#known-limitations-and-next-steps)
- [What was tested, and what wasn't](#what-was-tested-and-what-wasnt)
- [Project layout](#project-layout)

---

## Architecture: which process runs where

```
                        ┌──────────────────────── Docker network "desandbox" ────────────────────────┐
                        │                                                                             │
 ┌──────────────────┐   │  ┌──────────────────────────┐   spark-submit (client mode)                  │
 │ change-generator │   │  │ airflow-scheduler        │ ─────────────────────────┐                     │
 │ psql loop: INSERT│   │  │  LocalExecutor runs the  │                          ▼                     │
 │ + UPDATE every 5s│   │  │  task ⇒ Spark DRIVER     │◀──── executors ──┐  ┌──────────────┐           │
 └────────┬─────────┘   │  │  lives here (port 7078)  │   connect back   │  │ spark-master │ book-     │
          ▼             │  └────────────┬─────────────┘                  │  │  :7077       │ keeping   │
 ┌──────────────────┐   │               │ REST: load table, commit       │  └──────┬───────┘ only      │
 │ source-db        │◀──┼── JDBC, one   ▼                                │         │ launch executors  │
 │ Postgres: orders │   │  connection ┌──────────────┐  metadata.json    │  ┌──────▼───────┐ ┌──────────┴───┐
 └──────────────────┘   │  per task   │ iceberg-rest │ ────────────────┐ └──│spark-worker-1│ │spark-worker-2│
                        │             │ (catalog)    │                 │    │ 2 executors  │ │ 2 executors  │
 ┌──────────────────┐   │             └──────────────┘                 ▼    └──────┬───────┘ └──────┬───────┘
 │ airflow-db       │   │                                      ┌───────────────┐   │ Parquet data   │
 │ Airflow metadata │   │                                      │ rustfs (S3)   │◀──┴────────────────┘
 └──────────────────┘   │                                      │ s3://warehouse│
                        └──────────────────────────────────────┴───────────────┴────────────────────┘
```

| Container | What runs in it | Notes |
|---|---|---|
| `source-db` | Postgres 18 with the `orders` table (~100k seeded rows) | The "production" system. Host port 5433. |
| `change-generator` | A `psql` loop (stock postgres image) | Every 5 s: 10 inserts and 25 updates. Never deletes. |
| `airflow-db` | Postgres 18 for **Airflow's own** metadata | Kept separate from the source data on purpose. |
| `airflow-init` | One-shot: `airflow db migrate`, creates the `iceberg_orders` pool | Exits when done. |
| `airflow-apiserver` | Airflow 3 UI, REST API and Task Execution API | http://localhost:8080 (no login). |
| `airflow-scheduler` | Scheduler **and**, with the LocalExecutor, the task processes | So `spark-submit` and **every Spark driver run here**. Driver UI at http://localhost:4040 while a job runs. |
| `airflow-dag-processor` | Parses the DAG files | A separate component in Airflow 3. |
| `airflow-triggerer` | Runs deferrable tasks | Unused by these DAGs, but part of a standard deployment. |
| `spark-master` | Spark standalone master | Bookkeeping only: tracks workers, assigns executors. No data passes through it. UI: http://localhost:8090 |
| `spark-worker-1/2` | Spark workers, 2 cores / 2 GB each | Launch the **executors** that read Postgres, shuffle and write Parquet. UIs: :8081, :8082 |
| `iceberg-rest` | Iceberg REST catalog (reference implementation) | Maps table names to their current `metadata.json`. Its own state is a SQLite file on a volume. |
| `rustfs` | S3-compatible object store | Holds the Iceberg warehouse: data files, delete files, manifests, metadata. Console: http://localhost:9001/rustfs/console/ |
| `create-bucket` | One-shot AWS CLI: creates `s3://warehouse` | Idempotent; exits when done. |

How a sync run flows:

1. The **scheduler** sees that the 06:40–06:50 interval has closed and queues an `orders_sync` run.
2. With the **LocalExecutor**, the scheduler forks a task process. `SparkSubmitOperator` runs
   `spark-submit --master spark://spark-master:7077 --deploy-mode client sync_orders.py --start … --end …`.
3. `spark-submit` starts the **driver** (a JVM plus the Python process running `sync_orders.py`) *inside
   `airflow-scheduler`*. The driver registers with the **master**, and the master tells the **workers** to
   launch executors.
4. The executors connect **back to the driver** at `airflow-scheduler:7078` to fetch tasks.
5. Executors open their own JDBC connections to `source-db`, one per read partition, and later write
   Parquet files directly to `rustfs`.
6. The driver commits the MERGE through `iceberg-rest`. The REST server writes the new `metadata.json` to
   RustFS and swaps the table's pointer. That atomic pointer swap is what makes the commit all-or-nothing.

---

## Versions and why they fit together

Every image and jar is pinned to an exact version. Compatibility was checked before writing any config, and
then exercised end to end (see [What was tested](#what-was-tested-and-what-wasnt)).

| Component | Version | Why this one / what it must match |
|---|---|---|
| Apache Spark | **4.1.3** (Scala **2.13**, Java **17**) | Latest Spark line that has an Iceberg runtime (there is no Iceberg runtime for Spark 4.2 yet). 4.1.3 is a mature patch release. Official image `apache/spark:4.1.3-scala2.13-java17-ubuntu`. |
| Iceberg Spark runtime | **`iceberg-spark-runtime-4.1_2.13:1.12.0`** | The artifact name encodes the **Spark minor version (4.1)** and **Scala version (2.13)**, which must match the Spark build exactly. 1.12.0 is the latest Iceberg release. |
| Iceberg AWS bundle | **`iceberg-aws-bundle:1.12.0`** | Provides `S3FileIO` plus a shaded AWS SDK v2. It **must be the same Iceberg version** as the runtime. Iceberg talks S3 natively, so no `hadoop-aws`/s3a jars are needed. |
| Postgres JDBC driver | **`org.postgresql:postgresql:42.7.9`** | Latest pgjdbc. Supports Postgres 8.4 through 18 and Java 8+. |
| PySpark (driver) | **4.1.3** from the Spark distribution | The Airflow image copies `/opt/spark` and the JDK **from the same `apache/spark:4.1.3` image** as the cluster, so driver and executors run byte-identical Spark and Java builds. |
| Python | **3.12** on driver *and* executors | PySpark requires the same Python minor version on both sides once any Python runs on executors (UDFs). `apache/spark` ships Python 3.10, so the cluster image puts the Spark distribution on `python:3.12.13-slim-bookworm` to match Airflow's Python 3.12. |
| Apache Airflow | **3.3.2** (`apache/airflow:3.3.2-python3.12`) | Latest stable 3.x (3.4 is still a release candidate). |
| `apache-airflow-providers-apache-spark` | **6.3.2** | The version in Airflow 3.3.2's constraints file. Requires Python ≥ 3.11, which also rules out Python 3.10. Its `pyspark-client` dependency is pinned to **4.1.3** to match the cluster. |
| Iceberg REST catalog | **`apache/iceberg-rest-fixture:1.10.1`** | The newest tagged reference server. The REST protocol is versioned separately from the client libraries, so a 1.10 server works with 1.12 clients (verified here). |
| RustFS | **1.0.4** | Latest stable release. S3-compatible, Apache-2.0 licensed. |
| Postgres | **18.4** | For both the source and the Airflow metadata DB. In Postgres 18 images the volume mounts at `/var/lib/postgresql`, not `.../data`. |
| AWS CLI | **2.38.0** | Only used to create the bucket. |

The jars are downloaded at **build time** with checksum-verified `ADD --checksum=sha256:…`. They go into
`/opt/spark-extra-jars/` in **both** images and are put on the classpath through
`spark.driver.extraClassPath` and `spark.executor.extraClassPath` (`conf/spark-defaults.conf`). Nothing is
fetched when a job starts.

---

## Starting the sandbox

**Requirements:** Docker with Compose v2, about 8 GB of RAM for Docker (the Spark workers alone get 4 GB), and
about 10 GB of disk space.

```bash
# Linux only: let the containers write ./logs as your user
sed -i "s/^AIRFLOW_UID=.*/AIRFLOW_UID=$(id -u)/" .env

docker compose up -d --build      # first build takes a few minutes (downloads a few GB of images and jars)
docker compose ps                 # wait until the services report "healthy"
```

| UI | URL |
|---|---|
| Airflow | http://localhost:8080 (no login: `SIMPLE_AUTH_MANAGER_ALL_ADMINS`) |
| Spark master | http://localhost:8090 |
| Spark workers | http://localhost:8081, http://localhost:8082 |
| Spark driver (only while a job runs) | http://localhost:4040 |
| RustFS console | http://localhost:9001/rustfs/console/ (log in with `S3_ACCESS_KEY` / `S3_SECRET_KEY` from `.env`; the bare `:9001` root returns a 403) |
| Iceberg REST catalog | http://localhost:8181/v1/config |
| Source Postgres | `psql -h localhost -p 5433 -U shop shop` (password `shop`) |

The DAGs are unpaused automatically. Within 10 minutes `orders_sync` runs for the first time. It sees that the
Iceberg table doesn't exist yet and **bootstraps it with a parallel full load**. After that, every run is
incremental.

Stop with `docker compose down`. Your data survives in named volumes. `docker compose down -v` deletes
everything, including the seeded source data, which is regenerated on the next start.

---

## How the sync works

The job is `jobs/sync_orders.py`. Every design decision is commented in the code. Here is the summary.

### 1. Parallel JDBC reads

A plain `spark.read.jdbc(url, "orders")` is **one task**. A single executor core pulls every row through a
single connection while the rest of the cluster sits idle. The job instead does this:

1. Runs a cheap `SELECT min(id), max(id), count(*) … WHERE <window>` in Postgres. This returns one row.
2. Reads with `partitionColumn=id`, `lowerBound=min`, `upperBound=max`, and
   `numPartitions = clamp(count / rows_per_partition, 1, 8)`. Spark splits the id range into equal strides
   and runs one query per stride (`… AND id >= 25001 AND id < 50001`) on different executors, each with its
   own connection.

The bounds only set the stride. They never filter rows: the first and last slices are open-ended. The split
is only even if ids are spread evenly. The full load shows the effect: 8 partitions over 4 executors on 2
workers.

### 2. Incremental windows owned by the Airflow run

The DAG passes `--start {{ data_interval_start }} --end {{ data_interval_end }}`, and the job reads
`updated_at >= start AND updated_at < end` (half-open, so consecutive windows tile time without overlap). The
job stores no watermark, which has three consequences:

- **Retrying or clearing** a run re-reads exactly the same window.
- The job is stateless and easy to test: give it any window.
- A trigger in the source DB bumps `updated_at` on every UPDATE, and an index on `updated_at` makes the
  window query cheap.

> **Airflow 3 gotcha #1:** `schedule="*/10 * * * *"` now means `CronTriggerTimetable`, which gives each run
> a zero-length data interval (start == end). The DAG uses `CronDataIntervalTimetable` to get real 10-minute
> intervals.
>
> **Airflow 3 gotcha #2:** a *manual* run triggered without a logical date (the CLI default, or the UI form
> with the date left empty) has **no** data interval, so `{{ data_interval_start }}` is undefined and the
> task fails while rendering its templates. The DAG's `window_bound()` macro falls back to the latest
> complete interval, which is the same window Airflow infers when you do pass a logical date of "now".

### 3. Idempotent writes with MERGE

```sql
MERGE INTO lake.sales.orders t USING source_orders s ON t.id = s.id
WHEN MATCHED AND s.updated_at > t.updated_at THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

**Why not append?** Airflow retries failed tasks, and you will rerun windows by hand. An append writes the
same rows again on every attempt. A retry after a crash halfway through, or after a write that succeeded but
whose task then timed out, leaves duplicate orders. With MERGE the result depends only on the source rows,
not on how many times the job ran. The `s.updated_at > t.updated_at` guard makes a rerun a no-op, and it stops
an old window replayed late from overwriting a newer version. Iceberg commits the whole MERGE atomically as
one snapshot.

Before the MERGE, the job calls `source.localCheckpoint()`. A MERGE may evaluate its source more than once,
and each evaluation would be a fresh JDBC read of a table that keeps changing. The checkpoint reads Postgres
once and keeps the partitions **on the executors**. Nothing goes to the driver.

### 4. Initial load

- `orders_sync` bootstraps automatically: if the table doesn't exist, the first run does a full load.
- `orders_full_load` is a manually triggered DAG that reads the whole table in parallel and MERGEs it. Because
  it's a MERGE, running it on a populated table is safe. Use it to **repair gaps** (see the limitations
  section).

### 5. The Iceberg table and partition transforms

```sql
CREATE TABLE lake.sales.orders (...) USING iceberg PARTITIONED BY (days(created_at))
TBLPROPERTIES ('format-version'='2', 'write.merge.mode'='merge-on-read', ...)
```

`days(created_at)` is a **partition transform**. Iceberg derives the partition value (the date) from the
column itself and records it in metadata. Unlike Hive-style partitioning, there's no extra `created_date`
column to maintain, and a query filtering on `created_at` gets partition pruning automatically. Other
transforms are `years`, `months`, `hours`, `bucket(N, col)`, `truncate(W, col)` and `identity`. The table
partitions on `created_at` rather than `updated_at` because a row's partition should never change.

**Merge-on-read:** updates write small *delete files* instead of rewriting every data file that contains a
changed row (copy-on-write). Our updates are scattered across all day-partitions, so copy-on-write would
rewrite most of the table every 10 minutes. The cost moves to readers until compaction folds the deletes
back in.

### 6. Staying distributed

The main data path uses only DataFrame and SQL operations: no `.collect()`, `.toPandas()` or Python UDFs.
The driver only collects two single-row results: the bounds query and the commit summary. Shuffles are marked
in the code:

- **MERGE join:** the source is joined to the target on `id`. Spark either broadcasts the small source or
  shuffles both sides by `hash(id)`.
- **Write distribution:** before writing, Iceberg repartitions rows by partition key
  (`write.distribution-mode=hash`), so each task writes one file per day instead of every task writing a tiny
  file into every day.

---

## Triggering and watching a run

**In the UI** (http://localhost:8080):
- **Dags → orders_sync → Trigger** starts a run now. A manual run syncs the **latest complete** 10-minute
  window (triggered at 07:28, it covers 07:10–07:20). That window has usually been synced already, so the
  rerun changes nothing, which is a nice way to see idempotency in action. To (re)sync a specific window,
  set its logical date or data interval in the trigger form.
- Click a task instance, then **Logs**, to see the full `spark-submit` command, the executors being added on
  `spark-worker-1/2`, and the job's own lines, for example:
  ```
  sync_orders: mode=incremental window: updated_at >= '2026-11-02T07:20:00+00:00'::timestamptz AND updated_at < '2026-11-02T07:30:00+00:00'::timestamptz
  sync_orders: JDBC read: 3014 rows, ids 29..101810, 2 partitions
  sync_orders: snapshot 4564330655857283829 (overwrite): +2716 records, position deletes +3451/-1510, total records in data files 105248
  ```
  Reading that line: 2,716 new row versions were written. Position deletes hide the old versions of updated
  rows; Iceberg keeps one delete file per data file and rewrites earlier markers into it, so the net
  `3451 - 1510 = 1941` rows were updated and `2716 - 1941 = 775` were inserted. "Total records in data files"
  still counts the hidden versions until compaction. Fewer rows were written than read because rows
  already synced with the same `updated_at` are skipped by the MERGE guard.
- **Admin → Pools** shows `iceberg_orders` (1 slot). Sync, full load and maintenance queue behind each other
  there.

**From the CLI:**
```bash
docker compose exec airflow-scheduler airflow dags trigger orders_sync                 # latest complete window
docker compose exec airflow-scheduler airflow dags trigger orders_sync -l 2026-11-02T07:15:00+00:00   # a chosen window
docker compose exec airflow-scheduler airflow dags trigger orders_full_load
docker compose exec airflow-scheduler airflow dags list-runs orders_sync
tail -f logs/dag_id=orders_sync/run_id=*/task_id=merge_changed_orders/attempt=1.log
```

**In Spark:**
- http://localhost:8090: the master shows the running application and which workers host its executors.
- http://localhost:4040: the driver UI, live while a job runs. Under **Jobs/Stages**, the JDBC read stage has
  one task per read partition, running on different executors. **SQL** shows the MERGE plan with its
  exchanges (shuffles).

---

## Verifying the results (Spark SQL)

Open a Spark SQL shell. It uses the same `conf/spark-defaults.conf` as the jobs, so `lake` is the default
catalog:

```bash
docker compose exec spark-master spark-sql
```

Query the table:

```sql
SELECT count(*), count(DISTINCT id), max(updated_at) FROM sales.orders;

SELECT status, count(*) AS orders, sum(amount) AS revenue
FROM sales.orders GROUP BY status ORDER BY orders DESC;

-- partition pruning in action: only the files of the last 2 days are scanned
SELECT count(*) FROM sales.orders WHERE created_at >= current_timestamp() - INTERVAL 2 DAYS;
```

List snapshots. Each sync is one snapshot, and "overwrite" is how a merge-on-read MERGE shows up:

```sql
SELECT committed_at, snapshot_id, operation,
       summary['added-records'] AS added, summary['added-position-deletes'] AS deletes
FROM sales.orders.snapshots ORDER BY committed_at;

SELECT * FROM sales.orders.history;    -- which snapshot was current when
```

Time travel to an earlier snapshot. Replace `<snapshot_id>` and the timestamp with values from the queries
above. Timestamps are UTC (`spark.sql.session.timeZone`).

```sql
-- by snapshot id, e.g. the first one: the state right after the initial full load
SELECT count(*) FROM sales.orders VERSION AS OF <snapshot_id>;

-- by time: the snapshot that was current at that moment
SELECT count(*) FROM sales.orders TIMESTAMP AS OF '2026-11-02 07:09:00';

-- what changed between that snapshot and now?
SELECT id, status, updated_at FROM sales.orders
EXCEPT
SELECT id, status, updated_at FROM sales.orders VERSION AS OF <snapshot_id>
LIMIT 20;

-- the same thing via the DataFrame API (pyspark shell). Iceberg 1.12 removed its own
-- `snapshot-id` / `as-of-timestamp` options in favour of Spark's built-in ones:
--   spark.read.option("versionAsOf", <snapshot_id>).table("sales.orders")
--   spark.read.option("timestampAsOf", "2026-11-02 07:09:00").table("sales.orders")
```

Time travel only reaches back as far as the oldest snapshot not yet expired by maintenance (~24 h here).

Look under the hood:

```sql
SELECT partition, record_count, file_size_in_bytes, file_path FROM sales.orders.files LIMIT 10;
SELECT content, count(*) FROM sales.orders.files GROUP BY content;  -- 0 = data, 1 = position deletes
SELECT * FROM sales.orders.partitions ORDER BY partition DESC LIMIT 5;
SELECT * FROM sales.orders.manifests;
```

The files themselves are visible in the RustFS console (http://localhost:9001/rustfs/console/, bucket `warehouse`) or with:
```bash
docker compose run --rm create-bucket "aws s3 ls --recursive s3://warehouse/sales/orders/ | head"
```

### Compare with the source, row by row

`jobs/verify_sync.py` reads both sides in parallel and diffs them with distributed set operations:

```bash
docker compose stop change-generator        # freeze the source
# wait for the next orders_sync run to succeed (or trigger one), then:
docker compose exec airflow-scheduler spark-submit --conf spark.driver.host=airflow-scheduler \
    /opt/airflow/jobs/verify_sync.py
docker compose start change-generator
```

It prints row counts, missing, extra and duplicate ids, and differing rows, then `RESULT: IDENTICAL` or
`RESULT: DIFFERENT`. If you run it while the generator is still running, the result is expected to differ.

---

## Table maintenance

`iceberg_maintenance` runs daily at 03:17 UTC as four tasks, each its own Spark application
(`jobs/maintain_orders.py`):

```
compact_data_files  >>  rewrite_manifests  >>  expire_snapshots  >>  remove_orphan_files
```

**Why it matters:** at one commit every 10 minutes, the table gets 144 snapshots a day, and each one adds
small data files, delete files and a manifest. Without maintenance:

- **Reads slow down.** Every query opens many small Parquet files and must apply every delete file. Lots of
  tiny files is the classic "small files problem" of data lakes.
- **Planning slows down.** There are hundreds of manifests to read before the first task can start.
- **Storage grows forever.** Old snapshots keep every replaced file reachable. That's what makes time travel
  possible, and it's also why nothing is ever deleted on its own.

| Task | Procedure | Effect |
|---|---|---|
| `compact_data_files` | `rewrite_data_files` with `min-input-files=2`, `delete-file-threshold=1`, `remove-dangling-deletes=true` | Bin-packs small files and applies merge-on-read deletes into new data files. Also drops delete files left pointing at already-rewritten files: without that option they stay tracked even though they no longer apply. |
| `rewrite_manifests` | `rewrite_manifests` | Turns many small manifests into a few. |
| `expire_snapshots` | `expire_snapshots(older_than => now-24h, retain_last => 10)` | Removes old snapshots and **deletes the files only they referenced**, including the small files compaction replaced. You can no longer time travel past this point. |
| `remove_orphan_files` | `remove_orphan_files(older_than => now-72h, prefix_listing => true)` | Deletes files no metadata references, such as debris from crashed writes. The 72 h threshold protects writes still in progress. |

The order matters. Compaction makes the old files *unreferenced by the current snapshot*, and expiry is what
actually deletes them.

What a run looked like in this sandbox, after about 35 minutes of syncing: compaction rewrote **155 data files into
31** (one per day-partition) and dropped all **58 delete files**, and `rewrite_manifests` turned 16 manifests into
1. The snapshot history shows why this is needed: every 10-minute sync touches nearly every day-partition
(updates hit old orders), so each run adds about 31 data files and 31 delete files.

Two things you may notice:
- The pool serializes *tasks*, not whole DAG runs, so an `orders_sync` run can slot in between two
  maintenance tasks. That's harmless, because every step commits atomically on its own.
- On the very first start, the maintenance DAG runs once immediately for the most recent 03:17 slot. The
  table doesn't exist yet, so it's a no-op.

To try it now instead of waiting for 03:17, run
`docker compose exec airflow-scheduler airflow dags trigger iceberg_maintenance`, then compare
`SELECT content, count(*) FROM sales.orders.files GROUP BY content` before and after.

---

## Pitfalls this setup handles

**The Spark driver runs inside the Airflow container.** On a standalone cluster, PySpark only supports
*client* deploy mode, so the driver is the `spark-submit` process itself. With the LocalExecutor that's the
`airflow-scheduler` container. Executors on the workers must open connections back to the driver, so
`dags/sandbox_common.py` sets:

- `spark.driver.host=airflow-scheduler`: the address the driver *advertises*. It must resolve from the
  workers. The container ID or `localhost` would not.
- `spark.driver.bindAddress=0.0.0.0`: the address it *listens* on inside its container.
- `spark.driver.port=7078` and `spark.blockManager.port=7079`: fixed instead of random ports, so the traffic
  is easy to reason about.

All containers share one Docker network, so service names resolve everywhere. If you move to the
Celery or Kubernetes executor, drivers run elsewhere and `spark.driver.host` must follow.

**Jars on both driver and executors.** The same three jars sit at the same path in both images, and
`extraClassPath` adds them when each JVM starts. (`spark.jars.packages` would also work, but then every job
start depends on Maven Central being reachable.)

**Same Spark, Java and Python on both sides.** See the versions table. A mismatch shows up as
serialization errors (Spark/Scala) or "Python in worker has different version" (Python).

**S3-compatible storage is not AWS.** Iceberg needs `s3.endpoint=http://rustfs:9000` and
`s3.path-style-access=true`. Path style means `http://rustfs:9000/warehouse/key`; the AWS default,
virtual-host style `http://warehouse.rustfs:9000/key`, would need wildcard DNS. This is configured twice: in
`spark-defaults.conf` for the driver and executors, and in the `iceberg-rest` environment, because the
catalog server writes `metadata.json` itself. Credentials are standard `AWS_*` environment variables on every
container that touches S3.

**`remove_orphan_files` and `s3://`.** By default it lists files through Hadoop's FileSystem API, which knows
nothing about `s3://` here. `prefix_listing => true` makes it list with Iceberg's own S3FileIO.

**The Airflow 3 cron timetable** (see section 2 of the sync design): a plain cron string gives zero-length
data intervals.

**Commit conflicts and an oversubscribed cluster.** All writers of `lake.sales.orders` share a 1-slot Airflow
pool, and each Spark app is capped at the cluster's 4 cores (`spark.cores.max`).

---

## What this approach does not capture: deletes

The sync asks "which rows have `updated_at` in this window?". A row that is **hard-deleted**
(`DELETE FROM orders …`) simply disappears from the source, so no window ever contains it, and it stays in
Iceberg forever. Options:

1. **Soft deletes.** The application sets `deleted_at = now()` (and so `updated_at` changes) instead of
   deleting. The row flows through the normal sync. Consumers filter `WHERE deleted_at IS NULL`, or the MERGE
   gets a `WHEN MATCHED AND s.deleted_at IS NOT NULL THEN DELETE` clause. This is simple, but it relies on
   every writer cooperating.
2. **Change data capture (CDC)**, for example with **Debezium**. Debezium reads Postgres's write-ahead log
   through logical replication and emits every INSERT, UPDATE and DELETE as an event, usually into Kafka.
   Spark (batch or Structured Streaming) then applies the events with `MERGE … WHEN MATCHED AND op = 'd' THEN
   DELETE`. This captures deletes and every intermediate version, and puts no query load on the source. It
   needs more infrastructure: Kafka, Kafka Connect, replication slots.
3. **Periodic reconciliation.** Occasionally compare the full set of source ids with Iceberg and delete the
   ones that are gone, like an anti-join version of `verify_sync.py`. It's cheap to build but expensive to
   run on big tables, and deletes land late.

---

## Known limitations and next steps

- **`catchup=False` can skip intervals.** If Airflow is down or the DAG is paused from 08:00 to 09:00, those
  windows never run and their changes are not synced until the row changes again. Fixes:
  - Run `orders_full_load`, which is safe at any time.
  - Set `catchup=True` so Airflow runs every missed interval in order (`max_active_runs=1` keeps them
    sequential).
  - Pass `{{ prev_data_interval_end_success or data_interval_start }}` as `--start`, so each run covers
    everything since the last *successful* run.
- **Late commits.** `updated_at` is set when a source transaction *writes* the row, but the row only becomes
  visible when it *commits*. A long transaction that writes at 06:49 and commits at 06:51 is invisible to the
  06:40–06:50 run and outside the 06:50–07:00 window. The generator commits instantly, but real systems should
  read a small overlap (`start - 5 min`). The MERGE makes overlaps harmless.
- **Read consistency.** Each JDBC partition is a separate query and transaction, so the parallel read is not
  one consistent snapshot of the source. That's fine for upserts keyed on id, because each id lives in
  exactly one slice.
- **Iceberg REST fixture.** It is the Iceberg project's reference/test server. For anything beyond learning,
  use Apache Polaris, Lakekeeper, Nessie, Unity Catalog or AWS Glue. They speak the same REST protocol, so the
  Spark config barely changes.
- **Security is off:** no Airflow login, static credentials in `.env`, plain HTTP, and no `spark.authenticate`.
  The JDBC password travels to executors inside the task plan. This is fine on a laptop and not anywhere else.
- **No-op reruns still commit.** A MERGE that matches no rows still creates an (empty) snapshot. That's
  harmless, and expiry cleans it up.
- **No Spark History Server.** The driver UI disappears when the job ends. Adding `spark.eventLog.enabled` and
  a history-server container is a good next exercise.

---

## What was tested, and what wasn't

Everything was tested on a Linux Docker Engine 29.9 host (amd64, 4 CPUs, 16 GB RAM), building in stages:

1. **Containers up and talking.** Source DB seeded (100k rows); change generator running; `create-bucket`
   created `s3://warehouse`; REST catalog healthy; both workers registered with the master (4 cores / 4 GB).
2. **Spark → Iceberg.** `spark-sql` created, wrote and read a table through the REST catalog, and the
   Parquet and metadata files appeared in RustFS. `spark-submit` from the Airflow image ran the full load
   with 8 JDBC partitions on 4 executors across both workers. Running the same incremental window twice
   gave +88 records the first time and **+0 the second**.
3. **Airflow.** After `docker compose down -v && up`, the first scheduled `orders_sync` run bootstrapped the
   table with a full load, and later runs were incremental. With the generator stopped, `verify_sync.py`
   reported source and Iceberg **IDENTICAL** (100,930 rows; 0 missing, extra, duplicate or differing). Also
   tested: `orders_full_load` on a populated table (only the 307 changed rows written), manual
   `orders_sync` triggers with and without a logical date (CLI and the REST call the UI makes), and the
   `iceberg_maintenance` DAG (results above). Every Spark SQL example in this README was run.

What could **not** be verified here:

- **Docker Desktop on macOS/Windows and ARM (Apple Silicon).** All the base images publish `linux/arm64`
  builds, but the stack only ran on Linux/amd64.
- **The Airflow UI in a browser.** The UI was served (HTTP 200), and triggering used the same REST endpoint
  the UI form calls, but nobody clicked through the pages.
- **`docker compose up --build` exactly as written.** The test environment sits behind a TLS-intercepting
  proxy, so the Airflow image's `pip install` step was built from a base image with that proxy's CA added.
  The Dockerfiles in this repo are otherwise unchanged and contain nothing specific to that environment.
- **Long-running behaviour.** The 03:17 schedule was not observed firing naturally; the DAG was triggered by
  hand. Snapshot expiry was exercised with `--retain-hours 0` (it deleted 54 superseded files) but not
  after a real 24 hours.

---

## Project layout

```
.
├── docker-compose.yml          all services, one network, healthchecks, startup order
├── .env                        ports, credentials, sizes (sandbox values only)
├── conf/spark-defaults.conf    Iceberg catalog, jars, resources: shared by every Spark process
├── docker/
│   ├── spark/Dockerfile        Spark 4.1.3 + Java 17 on Python 3.12 + Iceberg/AWS/JDBC jars
│   └── airflow/Dockerfile      Airflow 3.3.2 + the same Spark/Java/jars + Spark provider
├── source-db/init/01_orders.sql    schema, updated_at trigger and index, ~100k seed rows
├── change-generator/generate_changes.sh   psql loop: inserts and status updates
├── dags/
│   ├── orders_sync.py          every 10 min: incremental MERGE of the data interval
│   ├── orders_full_load.py     manual: parallel full load / repair
│   ├── iceberg_maintenance.py  daily: compact, rewrite manifests, expire, remove orphans
│   └── sandbox_common.py       driver networking conf, retries, pool name
└── jobs/
    ├── sync_orders.py          the PySpark sync job
    ├── maintain_orders.py      Iceberg maintenance procedures
    └── verify_sync.py          row-by-row source vs. Iceberg comparison
```

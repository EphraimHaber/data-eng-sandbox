"""
Sync the source Postgres `orders` table into the Iceberg table `lake.sales.orders`.

    spark-submit sync_orders.py --mode incremental \
        --start 2026-11-02T06:40:00+00:00 --end 2026-11-02T06:50:00+00:00
    spark-submit sync_orders.py --mode full

Where the code runs
-------------------
This file executes in the Spark DRIVER: a Python process plus a JVM inside the
airflow-scheduler container (client deploy mode). The driver only *plans* work.
Every DataFrame operation below becomes tasks that run on the EXECUTORS on
spark-worker-1/2: they open their own JDBC connections to Postgres, shuffle
among themselves, and write Parquet files straight to RustFS. No table data
passes through the driver; the only things the driver collects are two tiny
single-row results (the bounds query and the commit summary).

Modes
-----
incremental  Read rows with  start <= updated_at < end  (the Airflow run's data
             interval) and upsert them. If the target table does not exist yet,
             a full load is done instead (bootstrap).
full         Read the whole source table in parallel and upsert it. Safe to
             re-run at any time, e.g. to repair a gap after downtime.
"""

from __future__ import annotations

import argparse
import logging
import math
import os
import sys
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

log = logging.getLogger("sync_orders")

SOURCE_TABLE = "public.orders"
COLUMNS = ["id", "customer_id", "amount", "status", "created_at", "updated_at"]


# -----------------------------------------------------------------------------
# Arguments
# -----------------------------------------------------------------------------
def parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--mode", choices=["incremental", "full"], required=True)
    p.add_argument("--start", type=parse_ts, help="window start, inclusive (ISO-8601 with offset)")
    p.add_argument("--end", type=parse_ts, help="window end, exclusive (ISO-8601 with offset)")
    p.add_argument("--table", default="lake.sales.orders", help="target Iceberg table")
    # How many parallel JDBC reads (= Spark tasks = Postgres connections) at most.
    p.add_argument("--max-partitions", type=int, default=8)
    # Aim for about this many rows per read task. Deliberately small so you can
    # SEE parallelism in the Spark UI with sandbox-sized data. In production
    # you'd aim for hundreds of thousands to millions of rows per task.
    p.add_argument("--rows-per-partition", type=int, default=2_000)
    args = p.parse_args(argv)

    if args.mode == "incremental":
        if args.start is None or args.end is None:
            p.error("--mode incremental requires --start and --end")
        if args.start >= args.end:
            p.error(f"empty window: start {args.start} >= end {args.end}")
    return args


def parse_ts(value: str) -> datetime:
    """Parse an ISO-8601 timestamp and insist on an explicit UTC offset.

    A naive timestamp is ambiguous (whose local time?), and an off-by-hours
    window silently skips or duplicates data, so we refuse it.
    Parsing also guarantees the value is a real timestamp before it is put
    into SQL text below (no SQL injection through arguments).
    """
    ts = datetime.fromisoformat(value)
    if ts.tzinfo is None:
        raise argparse.ArgumentTypeError(f"timestamp needs a UTC offset: {value!r}")
    return ts


# -----------------------------------------------------------------------------
# Source (Postgres over JDBC)
# -----------------------------------------------------------------------------
def jdbc_options() -> dict[str, str]:
    # Connection details come from the environment (set in docker-compose.yml),
    # so the password isn't visible in the spark-submit command line.
    # Note: these options are embedded in the query plan, which the driver sends
    # to the executors - each executor task opens its OWN connection.
    return {
        "url": os.environ["SOURCE_JDBC_URL"],
        "user": os.environ["SOURCE_DB_USER"],
        "password": os.environ["SOURCE_DB_PASSWORD"],
        "driver": "org.postgresql.Driver",
        # Rows fetched per round trip. pgjdbc's default (0) buffers the WHOLE
        # result set in executor memory before returning the first row.
        "fetchsize": "5000",
    }


def window_predicate(args: argparse.Namespace) -> str:
    """SQL WHERE clause selecting the rows this run is responsible for.

    Half-open interval [start, end): a row updated exactly at 06:50:00 belongs
    to the 06:50-07:00 run only, never to two runs. Consecutive Airflow data
    intervals therefore tile the time line with no gaps and no overlaps.
    The explicit ::timestamptz literal makes Postgres interpret the value as an
    absolute instant, independent of the JDBC session's time zone.
    """
    if args.mode == "full":
        return "TRUE"
    return (
        f"updated_at >= '{args.start.isoformat()}'::timestamptz "
        f"AND updated_at < '{args.end.isoformat()}'::timestamptz"
    )


def read_bounds(spark: SparkSession, predicate: str) -> tuple[int | None, int | None, int]:
    """Cheap min/max/count query, executed by Postgres, returning ONE row.

    Spark needs lowerBound/upperBound to split the id range into slices. We
    ask Postgres for them instead of guessing: an aggregate over the
    (indexed) window is far cheaper than moving the rows. Bringing this single
    row back to the driver with .first() is fine - it's metadata, not data.
    """
    query = f"SELECT min(id) AS lo, max(id) AS hi, count(*) AS n FROM {SOURCE_TABLE} WHERE {predicate}"
    row = spark.read.format("jdbc").options(**jdbc_options()).option("query", query).load().first()
    return row["lo"], row["hi"], int(row["n"])


def read_source(spark: SparkSession, predicate: str, lo: int, hi: int, n: int, args) -> DataFrame:
    """Parallel JDBC read.

    WHY THIS MATTERS: a plain spark.read.jdbc(url, table) is ONE task. One
    executor core opens one connection and pulls every row while the rest of
    the cluster sits idle - and that single partition is then what every later
    stage starts from.

    With partitionColumn/lowerBound/upperBound/numPartitions, Spark splits
    [lowerBound, upperBound] into numPartitions equal strides and runs one
    query per stride, e.g. for numPartitions=4 over ids 1..100000:

        task 0: ... WHERE <window> AND (id < 25001 OR id IS NULL)
        task 1: ... WHERE <window> AND id >= 25001 AND id < 50001
        task 2: ... WHERE <window> AND id >= 50001 AND id < 75001
        task 3: ... WHERE <window> AND id >= 75001

    Each task runs on whichever executor is free, with its own connection.
    Note the bounds only decide the stride; they never filter rows (the first
    and last slices are open-ended). Rows are spread evenly only if ids are
    spread evenly - a skewed column gives skewed tasks.
    """
    num_partitions = max(1, min(args.max_partitions, math.ceil(n / args.rows_per_partition)))
    if lo == hi:
        num_partitions = 1  # Spark rejects lowerBound == upperBound with >1 partition
    log.info("JDBC read: %s rows, ids %s..%s, %s partitions", n, lo, hi, num_partitions)

    # partitionColumn can't be combined with the `query` option, so the window
    # filter goes into a subquery passed as `dbtable`. It's executed inside
    # Postgres, which can use the index on updated_at.
    subquery = f"(SELECT {', '.join(COLUMNS)} FROM {SOURCE_TABLE} WHERE {predicate}) AS src"
    df = (
        spark.read.format("jdbc")
        .options(**jdbc_options())
        .option("dbtable", subquery)
        .option("partitionColumn", "id")
        .option("lowerBound", str(lo))
        .option("upperBound", str(hi))
        .option("numPartitions", str(num_partitions))
        .load()
    )

    # Pin the schema explicitly so a type change in the source fails loudly
    # here instead of silently changing the Iceberg table. select/cast are
    # narrow transformations: each partition is processed where it is, no shuffle.
    return df.select(
        F.col("id").cast("bigint"),
        F.col("customer_id").cast("bigint"),
        F.col("amount").cast("decimal(12,2)"),
        F.col("status").cast("string"),
        F.col("created_at").cast("timestamp"),
        F.col("updated_at").cast("timestamp"),
    )


# -----------------------------------------------------------------------------
# Target (Iceberg)
# -----------------------------------------------------------------------------
def ensure_table(spark: SparkSession, table: str) -> None:
    namespace = table.rsplit(".", 1)[0]
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
    # PARTITIONED BY days(created_at) uses an Iceberg *partition transform*.
    # Iceberg derives the partition value from the column (here: the date of
    # created_at) and records it in metadata. Unlike Hive-style partitioning
    # there is no extra `created_date` column to populate, and queries that
    # filter on created_at itself (WHERE created_at >= '2026-10-30') get
    # partition pruning automatically. Other transforms: years, months, hours,
    # bucket(N, col) (hash into N buckets), truncate(W, col), identity.
    # Partition by created_at, not updated_at: a row's partition must never
    # change, and created_at is immutable.
    #
    # TBLPROPERTIES:
    #  * merge-on-read: an UPDATE/MERGE writes small "delete files" that mark
    #    old row versions as deleted, instead of rewriting every data file that
    #    contains a changed row (copy-on-write). Our 10-minute syncs touch rows
    #    scattered across many days, so copy-on-write would rewrite most of the
    #    table every run. The price is extra work at read time until the
    #    nightly compaction folds the deletes back into data files.
    #  * metadata.delete-after-commit: every commit writes a new metadata.json;
    #    keep only the last 50 instead of 144 more per day forever.
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (
            id          BIGINT        NOT NULL,
            customer_id BIGINT,
            amount      DECIMAL(12,2),
            status      STRING,
            created_at  TIMESTAMP,
            updated_at  TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (days(created_at))
        TBLPROPERTIES (
            'format-version' = '2',
            'write.delete.mode' = 'merge-on-read',
            'write.update.mode' = 'merge-on-read',
            'write.merge.mode'  = 'merge-on-read',
            'write.metadata.delete-after-commit.enabled' = 'true',
            'write.metadata.previous-versions-max' = '50'
        )
        """
    )


def merge_into(spark: SparkSession, source: DataFrame, table: str) -> None:
    """Idempotent upsert keyed on id.

    Why not just append? Airflow retries failed tasks, and you will re-run
    windows by hand ("clear" in the UI). An append writes the same rows again
    on every attempt: a retry after a crash halfway through - or after a
    successful write whose task then timed out - leaves duplicate orders.
    MERGE makes the outcome depend only on the source rows, not on how many
    times we ran:
      * id not in the table           -> insert it
      * id present, source is newer   -> overwrite it with the source version
      * id present, not newer         -> leave it alone (a re-run is a no-op,
                                         and an older window replayed late can't
                                         overwrite a newer version)
    Iceberg commits the whole MERGE atomically as one snapshot: readers see
    either all of it or none of it, and a failed attempt leaves no trace in
    the table (only unreferenced files, cleaned up by remove_orphan_files).
    """
    source.createOrReplaceTempView("source_orders")
    # SHUFFLE: MERGE is a join of source and target on id. Spark either
    #   broadcasts the (small) source to every executor and scans the target
    #   in place, or - for big sources - shuffles both sides by hash(id) so
    #   matching ids meet in the same task. AQE picks at runtime.
    # SHUFFLE: before writing, Iceberg (write.distribution-mode=hash, the
    #   default for partitioned tables) repartitions the output by the
    #   partition key, so each task writes one file per day-partition instead
    #   of every task writing a tiny file into every partition.
    spark.sql(
        f"""
        MERGE INTO {table} AS t
        USING source_orders AS s
        ON t.id = s.id
        WHEN MATCHED AND s.updated_at > t.updated_at THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )


def log_last_commit(spark: SparkSession, table: str) -> None:
    # Reads Iceberg *metadata* tables (no data files scanned); one row comes back.
    row = spark.sql(
        f"SELECT snapshot_id, operation, summary FROM {table}.snapshots "
        f"ORDER BY committed_at DESC LIMIT 1"
    ).first()
    if row is None:
        return
    s = row["summary"]
    # Merge-on-read bookkeeping, per snapshot:
    #  * added-records: new row versions written (inserts + new versions of updated rows)
    #  * position deletes: markers hiding the OLD versions of updated rows. Iceberg
    #    keeps one delete file per data file, so a commit that touches a data file
    #    again rewrites its earlier markers into the new delete file: "removed"
    #    counts those, and added - removed = rows actually updated in this commit.
    #  * total-records counts rows in data files, INCLUDING versions hidden by
    #    deletes, so it is larger than SELECT count(*) until compaction runs.
    log.info(
        "snapshot %s (%s): +%s records, position deletes +%s/-%s, total records in data files %s",
        row["snapshot_id"], row["operation"], s.get("added-records", "0"),
        s.get("added-position-deletes", "0"), s.get("removed-position-deletes", "0"),
        s.get("total-records"),
    )


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
def main(argv: list[str]) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    args = parse_args(argv)

    # Cluster, catalog and jar settings come from spark-defaults.conf and the
    # --conf flags SparkSubmitOperator passes; nothing is hard-coded here.
    spark = SparkSession.builder.appName(f"sync_orders[{args.mode}]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    if args.mode == "incremental" and not spark.catalog.tableExists(args.table):
        log.info("%s does not exist yet: bootstrapping with a full load", args.table)
        args.mode = "full"

    ensure_table(spark, args.table)

    predicate = window_predicate(args)
    log.info("mode=%s window: %s", args.mode, predicate)
    lo, hi, n = read_bounds(spark, predicate)
    if n == 0:
        log.info("no changed rows in this window; nothing to do")
        spark.stop()
        return

    source = read_source(spark, predicate, lo, hi, n, args)

    # Materialize the source ONCE, distributed across the executors. A MERGE
    # may evaluate its source more than once (e.g. once to find which target
    # files are affected, once to write). Each evaluation would be a fresh JDBC
    # read, and since the source keeps changing, two reads could disagree.
    # localCheckpoint() runs the parallel JDBC read now, keeps the partitions
    # in executor memory/disk, and cuts the lineage back to Postgres.
    # (This is NOT a collect(): nothing is sent to the driver.)
    source = source.localCheckpoint(eager=True)

    merge_into(spark, source, args.table)
    log_last_commit(spark, args.table)
    spark.stop()


if __name__ == "__main__":
    main(sys.argv[1:])

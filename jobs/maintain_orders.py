"""
Iceberg table maintenance for lake.sales.orders.

    spark-submit maintain_orders.py --action compact
    spark-submit maintain_orders.py --action rewrite-manifests
    spark-submit maintain_orders.py --action expire-snapshots --retain-hours 24 --retain-last 10
    spark-submit maintain_orders.py --action remove-orphans --orphan-age-hours 72

Why a 10-minute sync needs this
-------------------------------
Every MERGE commit creates:
  * a new SNAPSHOT (plus a manifest list and manifests describing it), and
  * new, usually SMALL, data files and (merge-on-read) position-delete files.
That is 144 snapshots and hundreds of small files per day. Left alone:
  * Reads get slower: every query opens many small Parquet files and must
    apply every delete file to the data files it covers.
  * Planning gets slower: more manifests to read before the first task runs.
  * Storage grows forever: old snapshots keep every replaced file reachable,
    so nothing is ever deleted (that's what makes time travel possible).

Each action below is an Iceberg stored procedure (CALL lake.system.<name>).
They run as normal distributed Spark jobs: executors read and rewrite files;
the driver only plans and commits.

Order matters (see dags/iceberg_maintenance.py):
  compact -> rewrite-manifests -> expire-snapshots -> remove-orphans
Compaction writes new big files but the old small ones stay referenced by
older snapshots; expiring those snapshots is what finally deletes them.
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timedelta, timezone

from pyspark.sql import SparkSession

log = logging.getLogger("maintain_orders")
CATALOG = "lake"


def parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--action", required=True,
                   choices=["compact", "rewrite-manifests", "expire-snapshots", "remove-orphans"])
    # Fully qualified (catalog.namespace.table). With just "sales.orders" the
    # procedures would first try "sales" as a catalog name and log a scary
    # (but harmless) CATALOG_NOT_FOUND stack trace before falling back.
    p.add_argument("--table", default="lake.sales.orders")
    p.add_argument("--retain-hours", type=int, default=24,
                   help="expire snapshots older than this (limits how far back you can time travel)")
    p.add_argument("--retain-last", type=int, default=10,
                   help="...but always keep at least this many recent snapshots")
    p.add_argument("--orphan-age-hours", type=int, default=72,
                   help="only delete unreferenced files older than this")
    return p.parse_args(argv)


def ts_literal(hours_ago: int) -> str:
    t = datetime.now(timezone.utc) - timedelta(hours=hours_ago)
    return f"TIMESTAMP '{t.strftime('%Y-%m-%d %H:%M:%S')}'"  # session time zone is UTC


def compact(spark: SparkSession, table: str) -> None:
    # rewrite_data_files: bin-pack many small data files into fewer files close
    # to the target size, per partition.
    #  * min-input-files = 2: a partition holding 2+ small files is worth
    #    rewriting (the default of 5 would skip most of our day-partitions).
    #  * delete-file-threshold = 1: a data file with even one delete file
    #    attached is rewritten too, so the merge-on-read deletes get applied
    #    ("folded in") and readers no longer pay for them.
    #  * remove-dangling-deletes: once their data files have been rewritten,
    #    delete files point at files that are no longer live. They are harmless
    #    but still tracked (and opened by readers) until this option drops them.
    #  * partial-progress: commit in several smaller batches so one conflicting
    #    concurrent write doesn't throw away the whole compaction.
    # SHUFFLE: none for bin-packing; each file group is read and rewritten by
    # one task. (strategy => 'sort' would add a shuffle to cluster rows.)
    run(spark, f"""
        CALL {CATALOG}.system.rewrite_data_files(
            table   => '{table}',
            options => map(
                'min-input-files',          '2',
                'delete-file-threshold',    '1',
                'remove-dangling-deletes',  'true',
                'partial-progress.enabled', 'true'
            )
        )""")


def rewrite_manifests(spark: SparkSession, table: str) -> None:
    # Each commit adds at least one small manifest. Rewriting groups them by
    # partition so query planning reads a handful of manifests, not hundreds.
    run(spark, f"CALL {CATALOG}.system.rewrite_manifests(table => '{table}')")


def expire_snapshots(spark: SparkSession, table: str, retain_hours: int, retain_last: int) -> None:
    # Removes snapshots older than the cutoff (always keeping retain_last), then
    # deletes data/delete/manifest files that no remaining snapshot references.
    # This is what actually frees storage - and it is irreversible: you can no
    # longer time travel to an expired snapshot.
    run(spark, f"""
        CALL {CATALOG}.system.expire_snapshots(
            table       => '{table}',
            older_than  => {ts_literal(retain_hours)},
            retain_last => {retain_last}
        )""")


def remove_orphans(spark: SparkSession, table: str, age_hours: int) -> None:
    # Orphan files are files under the table's location that no table
    # metadata references at all - typically debris from a task or job that
    # died after writing files but before committing them. The age threshold
    # must exceed the longest-running write, or you could delete the files of
    # a commit that is still in flight.
    # prefix_listing => true: list the table location with Iceberg's own
    # S3FileIO. Without it, the procedure lists through Hadoop's FileSystem
    # API, which has no implementation for s3:// in this setup ("No FileSystem
    # for scheme s3") - we deliberately don't ship hadoop-aws/s3a at all.
    run(spark, f"""
        CALL {CATALOG}.system.remove_orphan_files(
            table          => '{table}',
            older_than     => {ts_literal(age_hours)},
            prefix_listing => true
        )""")


def run(spark: SparkSession, sql: str) -> None:
    log.info("running: %s", " ".join(sql.split()))
    # Procedures return a tiny summary (counts of files rewritten/deleted);
    # showing it is fine - it's a handful of rows, not table data.
    spark.sql(sql).show(truncate=False)


def main(argv: list[str]) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    args = parse_args(argv)
    spark = SparkSession.builder.appName(f"maintain_orders[{args.action}]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    if not spark.catalog.tableExists(args.table):
        log.info("%s does not exist yet; nothing to maintain", args.table)
        spark.stop()
        return

    if args.action == "compact":
        compact(spark, args.table)
    elif args.action == "rewrite-manifests":
        rewrite_manifests(spark, args.table)
    elif args.action == "expire-snapshots":
        expire_snapshots(spark, args.table, args.retain_hours, args.retain_last)
    elif args.action == "remove-orphans":
        remove_orphans(spark, args.table, args.orphan_age_hours)
    spark.stop()


if __name__ == "__main__":
    main(sys.argv[1:])

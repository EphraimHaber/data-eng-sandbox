"""
iceberg_maintenance: daily housekeeping for lake.sales.orders.

orders_sync commits every 10 minutes, i.e. 144 snapshots a day, each adding
small data files, merge-on-read delete files and manifests. Without
maintenance, queries slow down (many small files, deletes applied at read
time, many manifests to plan with) and storage grows forever (old snapshots
keep every replaced file reachable).

    compact_data_files  >>  rewrite_manifests  >>  expire_snapshots  >>  remove_orphan_files

  * compact_data_files   bin-pack small files, fold delete files into data files
  * rewrite_manifests    merge many small manifests into a few
  * expire_snapshots     drop snapshots older than 24h (keeping >= 10) and DELETE
                         the files only they referenced, incl. the small files
                         compaction just replaced. Limits time travel to ~24h.
  * remove_orphan_files  delete files no metadata references (crashed writes)

Each task is its own Spark application, so a failure is retried on its own
and shows up clearly in the grid view. They share the `iceberg_orders` pool
with the sync, so maintenance never runs concurrently with a MERGE.
"""

from __future__ import annotations

from datetime import datetime, timezone

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sdk import DAG

from sandbox_common import (
    DEFAULT_ARGS,
    ICEBERG_POOL,
    JOBS_DIR,
    ORDERS_TABLE,
    SPARK_CONN_ID,
    SPARK_DRIVER_CONF,
)


def maintenance_task(task_id: str, *action_args: str) -> SparkSubmitOperator:
    return SparkSubmitOperator(
        task_id=task_id,
        conn_id=SPARK_CONN_ID,
        application=f"{JOBS_DIR}/maintain_orders.py",
        name=f"iceberg_maintenance.{task_id}",
        conf=SPARK_DRIVER_CONF,
        application_args=["--table", ORDERS_TABLE, *action_args],
        pool=ICEBERG_POOL,
    )


with DAG(
    dag_id="iceberg_maintenance",
    # A plain cron string is fine here: maintenance doesn't use the data
    # interval. 03:17 UTC daily - an off-peak, non-round minute.
    schedule="17 3 * * *",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["sandbox", "spark", "iceberg", "maintenance"],
    doc_md=__doc__,
) as dag:
    (
        maintenance_task("compact_data_files", "--action", "compact")
        >> maintenance_task("rewrite_manifests", "--action", "rewrite-manifests")
        >> maintenance_task("expire_snapshots", "--action", "expire-snapshots",
                            "--retain-hours", "24", "--retain-last", "10")
        >> maintenance_task("remove_orphan_files", "--action", "remove-orphans",
                            "--orphan-age-hours", "72")
    )

"""
orders_full_load: read the ENTIRE source `orders` table in parallel and upsert
it into lake.sales.orders. Manual trigger only.

When to use it
  * Initial load. (orders_sync also bootstraps automatically: if the Iceberg
    table doesn't exist yet, its first run does a full load.)
  * Repair. If incremental windows were skipped (Airflow was down, the DAG was
    paused - catchup=False means those intervals never run), a full load
    brings the table back in line. Because it's a MERGE keyed on id, running
    it on a populated table is safe: unchanged rows are left alone.

It shares the 1-slot `iceberg_orders` pool with the other writers, so it will
wait for a running sync or maintenance task to finish first.
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

with DAG(
    dag_id="orders_full_load",
    schedule=None,  # manual only
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["sandbox", "spark", "iceberg"],
    doc_md=__doc__,
) as dag:
    SparkSubmitOperator(
        task_id="full_load_orders",
        conn_id=SPARK_CONN_ID,
        application=f"{JOBS_DIR}/sync_orders.py",
        name="orders_full_load",
        conf=SPARK_DRIVER_CONF,
        application_args=["--mode", "full", "--table", ORDERS_TABLE],
        pool=ICEBERG_POOL,
    )

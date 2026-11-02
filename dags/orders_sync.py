"""
orders_sync: every 10 minutes, upsert the rows that changed in the source
`orders` table during this run's data interval into lake.sales.orders.

Classic operators are used here rather than TaskFlow: the work happens in a
Spark application, not in Python code inside Airflow, and SparkSubmitOperator
already expresses "run spark-submit with these arguments" cleanly.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pendulum
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sdk import DAG
from airflow.timetables.interval import CronDataIntervalTimetable

from sandbox_common import (
    DEFAULT_ARGS,
    ICEBERG_POOL,
    JOBS_DIR,
    ORDERS_TABLE,
    SPARK_CONN_ID,
    SPARK_DRIVER_CONF,
)

# Why not schedule="*/10 * * * *"? In Airflow 3 a plain cron string means
# CronTriggerTimetable: runs fire every 10 minutes, but each run's
# data_interval_start == data_interval_end (a zero-length interval).
# CronDataIntervalTimetable gives each run the interval [previous tick,
# this tick), e.g. the run that starts at 06:50 covers 06:40-06:50.
SCHEDULE = CronDataIntervalTimetable("*/10 * * * *", timezone="UTC")


def window_bound(dag_run, bound: str) -> str:
    """Start or end of the window this run owns, as ISO-8601 with offset.

    Scheduled runs always have a data interval. In Airflow 3, a MANUAL run
    triggered without a logical date (`airflow dags trigger orders_sync`, or
    the UI form with the date left empty) has none - data_interval_start is
    undefined. Rather than failing, such a run gets the most recent complete
    interval, exactly what a manual run WITH a logical date of "now" gets.
    That window was normally already synced, so this is a safe, idempotent
    re-run. To sync a specific window, trigger with that logical date or set
    the data interval explicitly in the trigger form.
    """
    start, end = dag_run.data_interval_start, dag_run.data_interval_end
    if start is None or end is None:
        # Timetables work with pendulum datetimes; run_after is a stdlib datetime.
        interval = SCHEDULE.infer_manual_data_interval(run_after=pendulum.instance(dag_run.run_after))
        start, end = interval.start, interval.end
    return (start if bound == "start" else end).isoformat()


with DAG(
    dag_id="orders_sync",
    schedule=SCHEDULE,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    # catchup=False: when the DAG is (re)enabled, run only the most recent
    # interval instead of creating a run for every 10 minutes since start_date.
    # Trade-off: intervals that were never run (Airflow down, DAG paused) are
    # SKIPPED and their changes are not synced. Fix gaps with orders_full_load,
    # or see the README for alternatives.
    catchup=False,
    # Never run two syncs at once: the window order matters for readability of
    # the snapshot history, and concurrent MERGEs into one table would conflict.
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["sandbox", "spark", "iceberg"],
    doc_md=__doc__,
    # Makes window_bound() callable from templates below.
    user_defined_macros={"window_bound": window_bound},
) as dag:
    SparkSubmitOperator(
        task_id="merge_changed_orders",
        conn_id=SPARK_CONN_ID,
        application=f"{JOBS_DIR}/sync_orders.py",
        name="orders_sync",
        conf=SPARK_DRIVER_CONF,
        # The window is passed IN, not stored by the job. Each run therefore
        # owns a fixed, well-defined slice of time: retrying or clearing a run
        # re-reads exactly the same window, and the job itself is stateless.
        application_args=[
            "--mode", "incremental",
            # = data_interval_start / data_interval_end (see window_bound)
            "--start", "{{ window_bound(dag_run, 'start') }}",
            "--end", "{{ window_bound(dag_run, 'end') }}",
            "--table", ORDERS_TABLE,
        ],
        pool=ICEBERG_POOL,
    )

"""
Settings shared by the DAGs in this folder.

Airflow puts the dags/ folder on sys.path, so DAG files can `import sandbox_common`.
It is listed in .airflowignore because it defines no DAGs itself.
"""

from __future__ import annotations

from datetime import timedelta

JOBS_DIR = "/opt/airflow/jobs"   # ./jobs, mounted read-only into the Airflow containers
SPARK_CONN_ID = "spark_default"  # defined by AIRFLOW_CONN_SPARK_DEFAULT in docker-compose.yml
ICEBERG_POOL = "iceberg_orders"  # 1-slot pool created by airflow-init
ORDERS_TABLE = "lake.sales.orders"

# -----------------------------------------------------------------------------
# Driver networking - the #1 pitfall of Spark standalone + Docker.
#
# PySpark on a standalone cluster runs in CLIENT deploy mode: the driver is
# the spark-submit process itself, running inside the container that executes
# the Airflow task. With the LocalExecutor that is `airflow-scheduler`.
#
# Executors don't just receive work from the driver; they open connections
# BACK to it (to fetch tasks, report results, and serve/fetch blocks). So:
#
#   spark.driver.host        the address the driver ADVERTISES to executors.
#                            It must be resolvable and reachable from the
#                            workers. The compose service/hostname
#                            "airflow-scheduler" is, because every container is
#                            on the same Docker network. (A container ID, or
#                            "localhost", would not be.)
#   spark.driver.bindAddress the local interface the driver LISTENS on.
#                            0.0.0.0 = all interfaces in the container.
#   spark.driver.port /      fixed ports instead of random ones, so the traffic
#   spark.blockManager.port  is easy to reason about (and to open in a firewall
#                            if executors ever live outside this network).
#
# If you switch to the Celery or Kubernetes executor, tasks (and therefore
# drivers) run elsewhere and spark.driver.host must change accordingly.
# -----------------------------------------------------------------------------
SPARK_DRIVER_CONF = {
    "spark.driver.host": "airflow-scheduler",
    "spark.driver.bindAddress": "0.0.0.0",
    "spark.driver.port": "7078",
    "spark.blockManager.port": "7079",
}
# Everything else (master URL, Iceberg catalog, jars, resources) lives in
# conf/spark-defaults.conf, shared with the cluster and with `spark-sql`.

DEFAULT_ARGS = {
    "owner": "data-eng",
    # Spark jobs fail transiently (a worker restarting, a commit conflict with
    # a concurrent writer, Postgres briefly unavailable). Because every job is
    # idempotent, retrying is always safe.
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,
    # A 10-minute sync that runs for half an hour is stuck; fail it so the next
    # run can start (max_active_runs=1 would otherwise block all later runs).
    "execution_timeout": timedelta(minutes=30),
}

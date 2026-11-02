"""
Compare the source Postgres `orders` table with the Iceberg table, row by row.

    docker compose exec airflow-scheduler spark-submit \
        --conf spark.driver.host=airflow-scheduler /opt/airflow/jobs/verify_sync.py

While the change generator is running the two will always differ a little
(the source moves on between syncs). For an exact check:
    docker compose stop change-generator
    # wait for the next orders_sync run to finish, then run this script
    docker compose start change-generator

Both sides are read in parallel and compared with distributed set operations;
only the resulting counts (and a few sample ids) come back to the driver.
"""

from __future__ import annotations

import argparse
import sys

from pyspark.sql import SparkSession

# Python puts this script's own directory on sys.path, so we can reuse the
# connection settings and column list from the sync job.
from sync_orders import COLUMNS, SOURCE_TABLE, jdbc_options


def main(argv: list[str]) -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--table", default="lake.sales.orders")
    args = p.parse_args(argv)

    spark = SparkSession.builder.appName("verify_sync").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    bounds = (
        spark.read.format("jdbc").options(**jdbc_options())
        .option("query", f"SELECT min(id) lo, max(id) hi FROM {SOURCE_TABLE}").load().first()
    )
    source = (
        spark.read.format("jdbc").options(**jdbc_options())
        .option("dbtable", SOURCE_TABLE)
        .option("partitionColumn", "id")
        .option("lowerBound", str(bounds["lo"])).option("upperBound", str(bounds["hi"]))
        .option("numPartitions", "8")
        .load()
        .selectExpr("id", "customer_id", "CAST(amount AS DECIMAL(12,2)) AS amount",
                    "status", "created_at", "updated_at")
        .localCheckpoint()  # read Postgres once, so both comparisons see the same data
    )
    target = spark.table(args.table).select(*COLUMNS)

    # SHUFFLE: exceptAll / join repartition both sides by the compared columns.
    only_in_source = source.exceptAll(target)
    only_in_target = target.exceptAll(source)
    missing_ids = source.select("id").exceptAll(target.select("id"))
    extra_ids = target.select("id").exceptAll(source.select("id"))
    dup_ids = target.groupBy("id").count().where("count > 1")

    results = {
        "source rows": source.count(),
        "iceberg rows": target.count(),
        "ids missing from iceberg": missing_ids.count(),
        "ids only in iceberg (deleted in source?)": extra_ids.count(),
        "duplicate ids in iceberg": dup_ids.count(),
        "rows that differ (source side)": only_in_source.count(),
        "rows that differ (iceberg side)": only_in_target.count(),
    }
    width = max(map(len, results))
    print("\n=== verify_sync ===")
    for k, v in results.items():
        print(f"{k:<{width}}  {v}")
    if results["rows that differ (source side)"]:
        print("sample of differing ids:", [r.id for r in only_in_source.select("id").limit(10).collect()])
    ok = all(results[k] == 0 for k in list(results)[2:])
    print("RESULT:", "IDENTICAL" if ok else "DIFFERENT")
    spark.stop()


if __name__ == "__main__":
    main(sys.argv[1:])

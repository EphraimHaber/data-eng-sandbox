---
title: Pinned versions
description: Every pinned image and package version, and why they are compatible.
sidebar:
  order: 1
---

Every image tag and jar is pinned to an exact version. Most of these versions are chained: the Spark
version decides the Iceberg artifact, the Iceberg version decides the AWS bundle, and the Airflow image's
Python decides the cluster's Python.

## The compatibility chain

```
Spark 4.1.3, built for Scala 2.13
 ├─ iceberg-spark-runtime-4.1_2.13 : 1.12.0     "4.1" = Spark minor, "2.13" = Scala
 │   └─ iceberg-aws-bundle         : 1.12.0     must equal the runtime version
 ├─ Java 17                                     same JDK build on driver and executors
 └─ Python 3.12                                 same minor version on driver and executors
     └─ Airflow 3.3.2 (python3.12 image)
         └─ apache-airflow-providers-apache-spark 6.3.2   needs Python ≥ 3.11
             └─ pyspark-client 4.1.3                       pinned to the cluster's Spark
```

## Spark, Scala and PySpark

| Component | Version | Why |
| --- | --- | --- |
| Apache Spark | **4.1.3** | The newest Spark line with an Iceberg runtime (there's no runtime for Spark 4.2 yet). 4.1.3 is a mature patch release. |
| Scala | **2.13** (2.13.17) | Spark 4 is built only for Scala 2.13. Every Scala jar on the classpath must be a `_2.13` build. |
| Java | **17** (Temurin 17.0.19) | Copied from the official `apache/spark:4.1.3-scala2.13-java17-ubuntu` image into **both** custom images, so driver and executors run the same JDK. |
| PySpark (driver) | **4.1.3** | The Airflow image copies `/opt/spark` from the same official image, and `spark-submit` puts `$SPARK_HOME/python` first on the driver's `PYTHONPATH`. |
| Python | **3.12** | PySpark requires the same minor version on driver and executors as soon as any Python runs on an executor (a UDF, `mapInPandas`, RDD lambdas). The official Spark image ships Python 3.10, so the cluster image puts the Spark distribution on `python:3.12.13-slim-bookworm` to match Airflow's 3.12. |

A mismatch shows up at runtime, not at build time. Different Spark or Scala builds typically fail with
`NoSuchMethodError` or `InvalidClassException` during serialization. A Python mismatch fails with *"Python
in worker has different version 3.10 than that in driver 3.12"*.

## Iceberg runtime and AWS bundle

| Jar | Version | Why |
| --- | --- | --- |
| `org.apache.iceberg:iceberg-spark-runtime-4.1_2.13` | **1.12.0** | The latest Iceberg release. The artifact name encodes the Spark minor version and the Scala version, and both must match the Spark build exactly. |
| `org.apache.iceberg:iceberg-aws-bundle` | **1.12.0** | Provides `S3FileIO` and a shaded AWS SDK v2. It must be the same Iceberg version as the runtime. Iceberg talks to S3 itself, so no `hadoop-aws`, s3a or AWS SDK v1 jars are needed. |
| `apache/iceberg-rest-fixture` (catalog image) | **1.10.1** | The newest tagged build of Iceberg's reference REST server. The REST protocol is versioned separately from the client libraries, so a 1.10 server serves 1.12 clients; that was checked end to end. |

## Postgres JDBC driver

| Jar | Version | Why |
| --- | --- | --- |
| `org.postgresql:postgresql` | **42.7.9** | The latest pgjdbc. It supports Postgres 8.4 through 18 and Java 8 and up. |

## Airflow image

| Package | Version | Why |
| --- | --- | --- |
| `apache/airflow` | **3.3.2-python3.12** | The latest stable Airflow 3 (3.4 is still a release candidate). Python 3.12 is the newest version every piece supports. |
| `apache-airflow-providers-apache-spark` | **6.3.2** | The version in Airflow 3.3.2's constraints file. It provides `SparkSubmitOperator`, and it requires Python ≥ 3.11, which rules out matching Spark's Python 3.10 the other way round. |
| `pyspark-client` | **4.1.3** | A dependency of the provider (the Spark Connect client). Airflow's constraints pin 4.2.0; it's overridden to match the cluster, so there's only one PySpark version in the image. |

`pip install` pins `apache-airflow==3.3.2` on the same command line, so pip can't "helpfully" upgrade
Airflow itself while resolving the provider.

## Everything else

| Image | Version | Notes |
| --- | --- | --- |
| `postgres` | **18.4** | Source DB, Airflow metadata DB, and the `psql` client for the change generator. Postgres 18 images keep data under `/var/lib/postgresql/18/…`, so volumes mount at `/var/lib/postgresql`, not the old `…/data`. |
| `rustfs/rustfs` | **1.0.4** | S3-compatible object storage, Apache-2.0 licensed. |
| `amazon/aws-cli` | **2.38.0** | Only used by `create-bucket`. |
| `python` | **3.12.13-slim-bookworm** | Base of the Spark cluster image. |

## How the jars get into the images

Both Dockerfiles download the three jars at **build time**, verified against SHA-256 checksums:

```dockerfile
ADD --chmod=644 --checksum=sha256:3123c379…0a0f82 \
    ${MAVEN}/org/apache/iceberg/iceberg-spark-runtime-4.1_2.13/1.12.0/iceberg-spark-runtime-4.1_2.13-1.12.0.jar \
    /opt/spark-extra-jars/iceberg-spark-runtime-4.1_2.13-1.12.0.jar
```

They go into `/opt/spark-extra-jars/` at the same path in both images, and `conf/spark-defaults.conf` puts
that directory on both classpaths:

```properties
spark.driver.extraClassPath    /opt/spark-extra-jars/*
spark.executor.extraClassPath  /opt/spark-extra-jars/*
```

Nothing is fetched when a job starts. The alternative, `spark.jars.packages=<maven coordinates>`, makes the
driver download the jars at start-up and serve them to executors. It's convenient, but every job then
depends on Maven Central being reachable.

## Upgrading

1. Pick a Spark version that Iceberg publishes a runtime for. Check Maven Central for
   `iceberg-spark-runtime-<spark minor>_2.13`.
2. Change `SPARK_IMAGE` in **both** Dockerfiles, and `pyspark-client` in the Airflow one.
3. Change `ICEBERG_VERSION` in both Dockerfiles. The runtime and the AWS bundle always move together.
4. Update the `--checksum` values. Get them with `curl -sL <jar url> | sha256sum`.
5. If the new Spark image ships a different Python, check it still matches the Airflow image's.
6. `docker compose build && docker compose up -d`, then let a sync run and check the task log.

# syntax=docker/dockerfile:1.7
# =============================================================================
# Spark image for the standalone cluster (master + workers).
#
# Why not just use apache/spark directly?
#   1. Python version. PySpark requires the driver and the executors to run the
#      SAME Python minor version whenever Python code runs on executors (Python
#      UDFs, RDD lambdas, mapInPandas, ...). Our driver runs inside the Airflow
#      image, which ships Python 3.12, while apache/spark ships Ubuntu 22.04's
#      Python 3.10. Our job is DataFrame-only, so no Python workers start today,
#      but the first UDF someone adds would fail with
#      "Python in worker has different version 3.10 than that in driver 3.12".
#      So we put the official Spark distribution on top of a Python 3.12 base.
#   2. Extra jars. Executors need the Iceberg runtime, the Iceberg AWS bundle
#      (S3FileIO) and the Postgres JDBC driver on their classpath.
#
# Nothing is compiled or downloaded by a package manager here: we copy the JDK
# and the Spark distribution out of the official image (multi-stage build) and
# download the jars with checksum-verified ADD. The Airflow image does the same
# copy from the same tag, so the driver and executors run byte-identical Spark
# and Java builds.
# =============================================================================

ARG SPARK_IMAGE=apache/spark:4.1.3-scala2.13-java17-ubuntu
ARG PYTHON_IMAGE=python:3.12.13-slim-bookworm

FROM ${SPARK_IMAGE} AS spark-dist

FROM ${PYTHON_IMAGE}

# --- Java 17 + Spark 4.1.3 (Scala 2.13), straight from the official image ----
COPY --from=spark-dist /opt/java/openjdk /opt/java/openjdk
COPY --from=spark-dist /opt/spark /opt/spark

ENV JAVA_HOME=/opt/java/openjdk \
    SPARK_HOME=/opt/spark \
    PATH=/opt/java/openjdk/bin:/opt/spark/bin:/opt/spark/sbin:$PATH \
    # Python that executors use for any Python workers (must match the driver).
    PYSPARK_PYTHON=python3

# --- Extra jars (same files, same path in the Airflow image) ------------------
# They go into a dedicated directory instead of $SPARK_HOME/jars so it is obvious
# what we added. spark-defaults.conf puts this directory on BOTH the driver and
# executor classpath via spark.{driver,executor}.extraClassPath.
#
#  * iceberg-spark-runtime-4.1_2.13 : Iceberg's Spark integration. The "4.1" is
#    the Spark minor version and "2.13" the Scala version; both must match the
#    Spark build exactly or you get NoSuchMethodError at runtime.
#  * iceberg-aws-bundle : S3FileIO + a shaded AWS SDK v2. Iceberg talks to S3
#    (here: RustFS) directly instead of through Hadoop's s3a connector. Its
#    version must equal the iceberg-spark-runtime version.
#  * postgresql : JDBC driver used to read the source database.
ARG ICEBERG_VERSION=1.12.0
ARG POSTGRES_JDBC_VERSION=42.7.9
ARG MAVEN=https://repo1.maven.org/maven2
# Pre-create the directory: otherwise ADD --chmod=644 would apply 644 to the
# directory it creates as well, and a directory without +x can't be entered.
RUN mkdir -m 755 -p /opt/spark-extra-jars
ADD --chmod=644 --checksum=sha256:3123c3798af642dd537230923548e013ef41cc28b6626e5225808cf5670a0f82 \
    ${MAVEN}/org/apache/iceberg/iceberg-spark-runtime-4.1_2.13/${ICEBERG_VERSION}/iceberg-spark-runtime-4.1_2.13-${ICEBERG_VERSION}.jar \
    /opt/spark-extra-jars/iceberg-spark-runtime-4.1_2.13-${ICEBERG_VERSION}.jar
ADD --chmod=644 --checksum=sha256:dd7e49ffa4ac9b215bed846581991259e3aec9bb2fa08d6cec029c46cd66d957 \
    ${MAVEN}/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar \
    /opt/spark-extra-jars/iceberg-aws-bundle-${ICEBERG_VERSION}.jar
ADD --chmod=644 --checksum=sha256:88f1fc3992e80ec3b048f798030e9a014aa4783c40afb56d3e7a87ee0adf166f \
    ${MAVEN}/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar \
    /opt/spark-extra-jars/postgresql-${POSTGRES_JDBC_VERSION}.jar

# Run as an unprivileged user, like the official image does (uid 185).
RUN groupadd --system --gid 185 spark \
 && useradd --system --uid 185 --gid spark --home-dir /opt/spark --no-create-home spark \
 && mkdir -p /opt/spark/work /opt/spark/logs /opt/spark/work-dir \
 && chown -R spark:spark /opt/spark/work /opt/spark/logs /opt/spark/work-dir
USER spark
# A writable working directory. Interactive tools like `spark-sql` enable Hive
# support and create a local Derby metastore (metastore_db/) in the current
# directory; we never use it (our tables live in the Iceberg REST catalog),
# but it must be creatable. /opt/spark/work holds the workers' executor dirs.
WORKDIR /opt/spark/work-dir

# The role (master or worker) is chosen by `command:` in docker-compose.yml.
CMD ["/opt/spark/bin/spark-class", "org.apache.spark.deploy.master.Master"]

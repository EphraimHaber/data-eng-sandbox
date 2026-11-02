# syntax=docker/dockerfile:1.7
# =============================================================================
# Airflow image that can also act as a Spark DRIVER.
#
# SparkSubmitOperator shells out to `spark-submit`. For a PySpark app on a
# standalone cluster, spark-submit only supports *client* deploy mode, which
# means the driver JVM (and the Python process that talks to it) runs right
# here, inside the Airflow container that executes the task. With the
# LocalExecutor, that is the airflow-scheduler container. So this image needs:
#
#   * Java 17                     - the driver is a JVM
#   * The exact Spark 4.1.3 build - driver and executors must agree on Spark
#                                   and Scala versions (RPC/serialization)
#   * The same extra jars         - the driver plans the Iceberg MERGE and opens
#                                   the JDBC connection for the bounds query
#   * Python 3.12                 - must match PYSPARK_PYTHON on the executors
#   * The Airflow Spark provider  - provides SparkSubmitOperator
#
# We copy Java and Spark from the same official image tag used by the cluster
# image, so there is exactly one Spark version in the whole sandbox.
# =============================================================================

ARG SPARK_IMAGE=apache/spark:4.1.3-scala2.13-java17-ubuntu
ARG AIRFLOW_IMAGE=apache/airflow:3.3.2-python3.12

FROM ${SPARK_IMAGE} AS spark-dist

FROM ${AIRFLOW_IMAGE}

USER root
COPY --from=spark-dist /opt/java/openjdk /opt/java/openjdk
COPY --from=spark-dist /opt/spark /opt/spark

# Same three jars as docker/spark/Dockerfile, at the same path, verified by the
# same checksums. (See that file for what each jar is for.)
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

ENV JAVA_HOME=/opt/java/openjdk \
    SPARK_HOME=/opt/spark \
    PATH=/opt/java/openjdk/bin:/opt/spark/bin:$PATH \
    # The Python the driver uses. spark-submit puts $SPARK_HOME/python on the
    # PYTHONPATH, so the driver imports the full PySpark 4.1.3 that ships with
    # the Spark distribution above.
    PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3

USER airflow

# The Spark provider gives us SparkSubmitOperator. Pinning apache-airflow on
# the same command line stops pip from "helpfully" upgrading Airflow itself.
# The provider depends on pyspark-client (the Spark Connect thin client); we pin
# it to the cluster's Spark version so there is only one PySpark version in the
# image. (spark-submit'd jobs import the full PySpark from $SPARK_HOME/python,
# which comes first on the PYTHONPATH.)
ARG AIRFLOW_VERSION=3.3.2
RUN pip install --no-cache-dir \
      "apache-airflow==${AIRFLOW_VERSION}" \
      "apache-airflow-providers-apache-spark==6.3.2" \
      "pyspark-client==4.1.3"

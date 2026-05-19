import os
import random
import string
import sys
import time
from dataclasses import dataclass
from typing import Generator, List, Optional

import pytest


@dataclass
class TableConfig:
    location: str = ""
    schema: Optional["StructType"] = None  # pyspark.sql.types.StructType
    table_name: Optional[str] = None  # defaults to dict key when None
    partition_by: Optional[List[str]] = None
    liquid_clustering: bool = False


def pytest_addoption(parser):
    group = parser.getgroup("pyspark-delta-caching")
    group.addoption(
        "--delta-jar",
        action="store",
        dest="delta_jar",
        default=None,
        help=(
            "Delta Lake Maven coordinates for spark.jars.packages, "
            "e.g. io.delta:delta-spark_2.13:4.0.1"
        ),
    )
    parser.addini(
        "delta_jar",
        "Delta Lake Maven coordinates for spark.jars.packages",
        default=None,
    )
    parser.addini(
        "spark_app_name",
        "Spark application name used in tests",
        default="pytest-pyspark",
    )


@pytest.fixture(scope="session")
def set_utc_timezone():
    os.environ["TZ"] = "UTC"
    time.tzset()


@pytest.fixture(scope="session")
def spark(set_utc_timezone, request) -> Generator:
    from pyspark.sql import SparkSession

    delta_jar = request.config.getoption("--delta-jar") or request.config.getini("delta_jar") or None
    app_name = request.config.getini("spark_app_name")
    database_name = "pytest_" + "".join(
        random.choice(string.ascii_lowercase + string.digits) for _ in range(4)
    )

    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    builder = (
        SparkSession.builder.master("local[*]")
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.databricks.delta.snapshotPartitions", "2")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.dagGraph.retainedRootRDDs", "1")
        .config("spark.ui.retainedJobs", "1")
        .config("spark.ui.retainedStages", "1")
        .config("spark.ui.retainedTasks", "1")
        .config("spark.sql.ui.retainedExecutions", "1")
        .config("spark.worker.ui.retainedExecutors", "1")
        .config("spark.worker.ui.retainedDrivers", "1")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC -XX:+UseCompressedOops")
        .config("spark.executor.extraJavaOptions", "-Duser.timezone=UTC")
        .config("spark.sql.session.timeZone", "UTC")
    )

    if delta_jar:
        builder = (
            builder
            .config("spark.jars.packages", delta_jar)
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
        )

    spark_session = builder.getOrCreate()
    spark_session.sparkContext.setLogLevel("ERROR")
    spark_session.sql(f"create database if not exists {database_name}")
    spark_session.sql(f"use {database_name}")

    try:
        yield spark_session
    finally:
        spark_session.stop()

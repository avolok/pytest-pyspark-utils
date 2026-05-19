import os
import random
import shutil
import string
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Generator, List, Optional

import pytest

from pytest_pyspark_delta_caching.delta_caching import DeltaCaching


@dataclass
class TableConfig:
    source: str = "input"  # "input", "expected", or an absolute path
    schema: Optional["StructType"] = None
    table_name: Optional[str] = None
    partition_by: Optional[List[str]] = None
    liquid_clustering: bool = False


SOURCE_DIR_MAP = {
    "input": "input",
    "expected": "expected",
}


def determine_file_path(base_path: str, filename: str) -> str:
    file_matches = [
        file for file in Path(base_path).glob(f"{filename}.*")
        if file.suffix in [".jsonl", ".csv"]
    ]

    if not file_matches:
        raise FileNotFoundError(f"No file found for {filename} in {base_path}")
    elif len(file_matches) > 1:
        raise FileExistsError(
            f"Multiple files found for {filename} in {base_path}: {[file.name for file in file_matches]}. "
            f"Please ensure there is only one file for {filename} in the directory."
        )
    else:
        return f"{base_path}/{file_matches[0].name}"


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
    parser.addini(
        "delta_cache_dir",
        "Directory for persistent delta table cache (relative to rootdir)",
        default="_delta_cache",
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


# --- Internal fixtures ---


@pytest.fixture(scope="session")
def _pyspark_tmp_dir(tmp_path_factory):
    base = tmp_path_factory.mktemp("delta")
    yield base
    shutil.rmtree(base, ignore_errors=True)


@pytest.fixture(scope="module")
def _pyspark_module_delta_path(_pyspark_tmp_dir, request):
    return (_pyspark_tmp_dir / Path(request.node.name).stem).as_posix()


# --- Public fixtures ---


@pytest.fixture(scope="module")
def get_test_paths():
    def _get_test_paths(test_file_location: Path):
        test_group_location = test_file_location.parent
        test_input_path = (test_group_location / "input").as_posix()
        test_expected_output_path = (test_group_location / "expected_output").as_posix()
        return test_input_path, test_expected_output_path

    yield _get_test_paths


@pytest.fixture(scope="module")
def prepare_tables_for_test(spark, _pyspark_module_delta_path, request):
    def _prepare_tables_for_test(files: dict):
        start = datetime.now()
        test_dir = request.path.parent
        cache_base_dir = test_dir / request.config.getini("delta_cache_dir")
        temp_delta = Path(_pyspark_module_delta_path)
        output = {}

        for filename, config in files.items():
            table_name = config.table_name or filename

            if config.source in SOURCE_DIR_MAP:
                location = (test_dir / SOURCE_DIR_MAP[config.source]).as_posix()
            else:
                location = config.source

            file_path = determine_file_path(base_path=location, filename=filename)

            delta_caching = DeltaCaching(
                source_path=file_path,
                cache_base_dir=cache_base_dir,
                spark=spark,
                schema=config.schema,
                partition_by=config.partition_by,
                liquid_clustering=config.liquid_clustering,
            )
            _df = delta_caching.cache()

            delta_target_path = temp_delta / table_name
            shutil.copytree(delta_caching.cached_path, delta_target_path)

            spark.sql(f"DROP TABLE IF EXISTS {table_name}")
            spark.sql(f"CREATE TABLE {table_name} USING DELTA LOCATION '{delta_target_path.as_posix()}'")

            output[filename] = _df
            print(f"successfully created delta table for {filename}")

        duration = round((datetime.now() - start).total_seconds(), 1)
        print(f"done with creating tables ({duration}s).")

        output["delta_tables_path"] = _pyspark_module_delta_path
        return output

    return _prepare_tables_for_test


@pytest.fixture(scope="module")
def _delta_tables_cached(prepare_tables_for_test, delta_tables_config):
    return prepare_tables_for_test(delta_tables_config)


@pytest.fixture(scope="function")
def delta_tables(spark, _delta_tables_cached, _pyspark_module_delta_path, _pyspark_tmp_dir, tmp_path):
    source = _pyspark_module_delta_path
    dest = Path(str(_pyspark_tmp_dir)) / "isolated_tables" / tmp_path.name
    shutil.copytree(Path(source), dest, dirs_exist_ok=True)

    tables = spark.sql("SHOW TABLES").collect()
    for table in tables:
        fqn = f"{table.namespace}.{table.tableName}" if table.namespace else table.tableName
        spark.sql(f"DROP TABLE IF EXISTS {fqn}")

    for filename, df in _delta_tables_cached.items():
        if filename == "delta_tables_path":
            continue
        table_path = dest / filename
        spark.sql(f"CREATE TABLE {filename} USING DELTA LOCATION '{table_path.as_posix()}'")

    output = dict(_delta_tables_cached)
    output["delta_tables_path"] = dest.as_posix()
    return output


@pytest.fixture(scope="function")
def drop_hive_objects(spark):
    tables = spark.sql("SHOW TABLES").collect()
    for table in tables:
        fqn = f"{table.namespace}.{table.tableName}" if table.namespace else table.tableName
        spark.sql(f"DROP TABLE IF EXISTS {fqn}")




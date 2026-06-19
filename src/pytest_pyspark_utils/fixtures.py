"""PySpark pytest fixtures with Delta Lake table caching.

Fixtures:
    spark: Session-scoped SparkSession with optional Delta Lake support.
    delta_tables: Function-scoped ``DeltaTablesResult`` with per-test isolation.
    set_utc_timezone: Sets TZ=UTC for the test session.
    drop_hive_objects: Drops all Hive tables (utility, not auto-used).
"""

import logging
import os
import random
import shutil
import string
import sys
import time
from datetime import datetime
from pathlib import Path

import pytest

from pyspark.sql import DataFrame

from pytest_pyspark_utils.delta_caching import DeltaCaching
from pytest_pyspark_utils.models import DeltaTablesResult, _CachedTables
from pytest_pyspark_utils.utils import determine_file_path, determine_delta_jar

logger = logging.getLogger(__name__)


# --- Internal fixtures ---


@pytest.fixture(scope="session")
def _pyspark_tmp_dir(tmp_path_factory):
    base = tmp_path_factory.mktemp("delta")
    yield base
    shutil.rmtree(base, ignore_errors=True)


@pytest.fixture(scope="module")
def _pyspark_module_delta_path(_pyspark_tmp_dir, request):
    return (_pyspark_tmp_dir / Path(request.node.name).stem).as_posix()


@pytest.fixture(scope="module")
def _prepare_tables_for_test(spark, _pyspark_module_delta_path, request):
    def _prepare_tables_for_test(files: dict) -> _CachedTables:
        start = datetime.now()
        test_dir = request.path.parent
        cache_base_dir = test_dir / request.config.getini("delta_cache_dir")
        temp_delta = Path(_pyspark_module_delta_path)
        entries: dict[str, tuple[str, DataFrame]] = {}

        for filename, config in files.items():
            table_name = config.table_name or filename

            location = (test_dir / config.source).as_posix()

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

            entries[filename] = (table_name, _df)
            print(f"successfully created delta table for {filename}")

        duration = round((datetime.now() - start).total_seconds(), 1)
        print(f"done with creating tables ({duration}s).")

        return _CachedTables(entries=entries, path=_pyspark_module_delta_path)

    return _prepare_tables_for_test


@pytest.fixture(scope="module")
def _delta_tables_cached(_prepare_tables_for_test, delta_tables_config) -> _CachedTables:
    return _prepare_tables_for_test(delta_tables_config)


# --- Public fixtures ---


@pytest.fixture(scope="session")
def set_utc_timezone():
    """Set the process timezone to UTC for the duration of the test session."""
    os.environ["TZ"] = "UTC"
    time.tzset()


@pytest.fixture(scope="session")
def spark(set_utc_timezone, request, _pyspark_tmp_dir):
    """Create a session-scoped SparkSession configured for local testing.

    Enables Delta Lake support when ``delta_jar`` is configured.  The session
    uses a randomly-named database so parallel test runs remain isolated in the
    Hive metastore.

    Yields:
        SparkSession ready for use in tests.
    """
    from pyspark.sql import SparkSession

    delta_jar = request.config.getoption("--delta-jar") or request.config.getini("delta_jar") or None
    app_name = request.config.getini("spark_app_name")
    database_name = "pytest_" + "".join(random.choice(string.ascii_lowercase + string.digits) for _ in range(4))

    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    if delta_jar is None:
        try:
            print(
                "No delta_jar specified in pytest.ini, attempting to determine automatically based on PySpark version..."
            )
            delta_jar = determine_delta_jar()
            print(f"Determined Delta Lake JAR: {delta_jar}")
        except ValueError as e:
            print(f"Warning: {e}")

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
        .config("spark.sql.warehouse.dir", (_pyspark_tmp_dir / "spark-warehouse").as_posix())
        .config(
            "spark.driver.extraJavaOptions",
            "-Duser.timezone=UTC -XX:+UseCompressedOops",
        )
        .config("spark.executor.extraJavaOptions", "-Duser.timezone=UTC")
        .config("spark.sql.session.timeZone", "UTC")
    )

    if delta_jar:
        builder = (
            builder.config("spark.jars.packages", delta_jar)
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
        )
    else:
        print("Delta Lake support is disabled for this Spark session.")

    spark_session = builder.getOrCreate()
    spark_session.sparkContext.setLogLevel("ERROR")
    spark_session.sql(f"create database if not exists `{database_name}`")
    spark_session.sql(f"use {database_name}")

    try:
        yield spark_session
    finally:
        spark_session.sql(f"drop database if exists `{database_name}` cascade")
        spark_session.stop()


@pytest.fixture(scope="function")
def delta_tables(spark, _delta_tables_cached: _CachedTables, _pyspark_tmp_dir, tmp_path) -> DeltaTablesResult:
    """Provide per-test isolated Delta tables as a :class:`DeltaTablesResult`.

    Copies the module-level cached tables to a function-specific directory,
    drops all existing Hive tables, and re-registers fresh copies.  Mutations
    made during a test do not affect sibling tests.

    Args:
        spark: The session-scoped SparkSession.
        _delta_tables_cached: Module-level cached table entries.
        _pyspark_tmp_dir: Session-scoped temp directory.
        tmp_path: pytest-provided per-test temp directory (used as a unique suffix).

    Returns:
        A :class:`DeltaTablesResult` with ``tables`` (filename → DataFrame) and
        ``path`` (directory holding the isolated Delta copies).
    """
    source = _delta_tables_cached.path
    dest = Path(str(_pyspark_tmp_dir)) / "isolated_tables" / tmp_path.name
    shutil.copytree(Path(source), dest, dirs_exist_ok=True)

    tables = spark.sql("SHOW TABLES").collect()
    for table in tables:
        fqn = f"{table.namespace}.{table.tableName}" if table.namespace else table.tableName
        spark.sql(f"DROP TABLE IF EXISTS {fqn}")

    result_tables: dict[str, DataFrame] = {}
    for filename, (table_name, df) in _delta_tables_cached.entries.items():
        table_path = dest / table_name
        spark.sql(f"CREATE TABLE {table_name} USING DELTA LOCATION '{table_path.as_posix()}'")
        result_tables[filename] = df

    os.environ["UNIT_TEST_TMP_DIR"] = dest.as_posix()

    return DeltaTablesResult(tables=result_tables, path=dest.as_posix())


@pytest.fixture(scope="function")
def drop_hive_objects(spark):
    """Drop all Hive tables in the current database.

    Useful as an explicit teardown step in tests that create their own tables
    outside of the ``delta_tables`` fixture.
    """
    tables = spark.sql("SHOW TABLES").collect()
    for table in tables:
        fqn = f"{table.namespace}.{table.tableName}" if table.namespace else table.tableName
        spark.sql(f"DROP TABLE IF EXISTS {fqn}")

"""pytest plugin providing PySpark fixtures with Delta Lake table caching.

Fixtures:
    spark: Session-scoped SparkSession with optional Delta Lake support.
    delta_tables: Function-scoped ``DeltaTablesResult`` with per-test isolation.
    set_utc_timezone: Sets TZ=UTC for the test session.
    drop_hive_objects: Drops all Hive tables (utility, not auto-used).

Configuration (pytest.ini / pyproject.toml / CLI):
    delta_jar: Maven coordinates for Delta Lake JAR.
    spark_app_name: Spark application name (default: pytest-pyspark).
    delta_cache_dir: Cache directory name (default: _delta_cache).

Usage:
    Define a module-scoped ``delta_tables_config`` fixture returning
    ``dict[str, TableConfig]``, then use ``delta_tables`` in your tests.
"""

import logging
import os
import random
import shutil
import string
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

import pytest

from pytest_pyspark_utils.delta_caching import DeltaCaching
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

logger = logging.getLogger(__name__)


@dataclass
class TableConfig:
    """Configuration for a single test table loaded from CSV or JSONL.

    Args:
        source: Subdirectory under the test folder where the source file lives
            (``"input"`` or ``"expected"``). Defaults to ``"input"``.
        schema: Explicit Spark schema. If ``None``, schema is inferred from the file.
        table_name: SQL table name to register. Defaults to the source filename stem.
        partition_by: Column names to partition the Delta table by.
            Mutually exclusive with ``liquid_clustering``.
        liquid_clustering: Enable Delta Lake liquid clustering (requires ``schema``).
            Mutually exclusive with ``partition_by``.
    """

    source: str = "input"
    schema: StructType | None = None
    table_name: str | None = None
    partition_by: list[str] | None = None
    liquid_clustering: bool = False


@dataclass
class DeltaTablesResult:
    """Result returned by the :func:`delta_tables` fixture.

    Attributes:
        tables: Mapping from config key (filename stem) to the corresponding DataFrame.
        path: Filesystem path to the isolated Delta table copies for this test.
    """

    tables: dict[str, DataFrame]
    path: str


@dataclass
class _CachedTables:
    """Internal module-level cache returned by ``_prepare_tables_for_test``."""

    entries: dict[str, tuple[str, DataFrame]] = field(default_factory=dict)
    path: str = ""


def determine_file_path(base_path: str, filename: str) -> str:
    """Find the unique CSV or JSONL file matching *filename* in *base_path*.

    Args:
        base_path: Directory to search in.
        filename: Stem name (no extension) to match.

    Returns:
        Absolute path string to the matched file.

    Raises:
        FileNotFoundError: If no matching file exists.
        FileExistsError: If more than one matching file exists.
    """
    file_matches = [
        file
        for file in Path(base_path).glob(f"{filename}.*")
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
    """Register CLI flags and INI options for the pyspark-delta-caching plugin."""
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
            spark.sql(
                f"CREATE TABLE {table_name} USING DELTA LOCATION '{delta_target_path.as_posix()}'"
            )

            entries[filename] = (table_name, _df)
            print(f"successfully created delta table for {filename}")

        duration = round((datetime.now() - start).total_seconds(), 1)
        print(f"done with creating tables ({duration}s).")

        return _CachedTables(entries=entries, path=_pyspark_module_delta_path)

    return _prepare_tables_for_test


@pytest.fixture(scope="module")
def _delta_tables_cached(
    _prepare_tables_for_test, delta_tables_config
) -> _CachedTables:
    return _prepare_tables_for_test(delta_tables_config)


# --- Public fixtures ---


@pytest.fixture(scope="session")
def set_utc_timezone():
    """Set the process timezone to UTC for the duration of the test session."""
    os.environ["TZ"] = "UTC"
    time.tzset()


@pytest.fixture(scope="session")
def spark(set_utc_timezone, request):
    """Create a session-scoped SparkSession configured for local testing.

    Enables Delta Lake support when ``delta_jar`` is configured.  The session
    uses a randomly-named database so parallel test runs remain isolated in the
    Hive metastore.

    Yields:
        SparkSession ready for use in tests.
    """
    from pyspark.sql import SparkSession

    delta_jar = (
        request.config.getoption("--delta-jar")
        or request.config.getini("delta_jar")
        or None
    )
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

    spark_session = builder.getOrCreate()
    spark_session.sparkContext.setLogLevel("ERROR")
    spark_session.sql(f"create database if not exists {database_name}")
    spark_session.sql(f"use {database_name}")

    try:
        yield spark_session
    finally:
        spark_session.stop()


@pytest.fixture(scope="function")
def delta_tables(
    spark, _delta_tables_cached: _CachedTables, _pyspark_tmp_dir, tmp_path
) -> DeltaTablesResult:
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
        fqn = (
            f"{table.namespace}.{table.tableName}"
            if table.namespace
            else table.tableName
        )
        spark.sql(f"DROP TABLE IF EXISTS {fqn}")

    result_tables: dict[str, DataFrame] = {}
    for filename, (table_name, df) in _delta_tables_cached.entries.items():
        table_path = dest / table_name
        spark.sql(
            f"CREATE TABLE {table_name} USING DELTA LOCATION '{table_path.as_posix()}'"
        )
        result_tables[filename] = df

    return DeltaTablesResult(tables=result_tables, path=dest.as_posix())


@pytest.fixture(scope="function")
def drop_hive_objects(spark):
    """Drop all Hive tables in the current database.

    Useful as an explicit teardown step in tests that create their own tables
    outside of the ``delta_tables`` fixture.
    """
    tables = spark.sql("SHOW TABLES").collect()
    for table in tables:
        fqn = (
            f"{table.namespace}.{table.tableName}"
            if table.namespace
            else table.tableName
        )
        spark.sql(f"DROP TABLE IF EXISTS {fqn}")

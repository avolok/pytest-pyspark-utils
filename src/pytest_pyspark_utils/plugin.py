"""pytest plugin providing PySpark fixtures with Delta Lake table caching.

Configuration (pytest.ini / pyproject.toml / CLI):
    delta_jar: Maven coordinates for Delta Lake JAR.
    spark_app_name: Spark application name (default: pytest-pyspark).
    delta_cache_dir: Cache directory name (default: _delta_cache).

Usage:
    Define a module-scoped ``delta_tables_config`` fixture returning
    ``dict[str, TableConfig]``, then use ``delta_tables`` in your tests.
"""

from pytest_pyspark_utils.fixtures import (  # noqa: F401
    _delta_tables_cached,
    _prepare_tables_for_test,
    _pyspark_module_delta_path,
    _pyspark_tmp_dir,
    delta_tables,
    drop_hive_objects,
    set_utc_timezone,
    spark,
)
from pytest_pyspark_utils.models import DeltaTablesResult, TableConfig  # noqa: F401


def pytest_addoption(parser):
    """Register CLI flags and INI options for the pyspark-delta-caching plugin."""
    group = parser.getgroup("pyspark-delta-caching")
    group.addoption(
        "--delta-jar",
        action="store",
        dest="delta_jar",
        default=None,
        help=("Delta Lake Maven coordinates for spark.jars.packages, " "e.g. io.delta:delta-spark_2.13:4.0.1"),
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

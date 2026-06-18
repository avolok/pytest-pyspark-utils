"""Data models for pytest-pyspark-utils."""

from dataclasses import dataclass, field

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType


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

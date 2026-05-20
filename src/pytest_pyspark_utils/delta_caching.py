"""
Delta Lake caching layer for pytest-pyspark-delta-caching.

Converts CSV/JSONL source files to Delta format and caches them on disk.
Cache validity is determined by an MD5 hash of the source file content
and schema. A cache hit skips re-conversion; a miss cleans the old cache
and re-generates it.
"""

import hashlib
import logging
import shutil
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

logger = logging.getLogger(__name__)


class DeltaCaching:
    """Manages on-disk Delta Lake caching for a single CSV or JSONL source file.

    On first use (or when the source file changes) the data is converted to
    Delta format and stored under ``cache_base_dir / dataset_name``.  Subsequent
    calls with the same source file and schema reuse the cached Delta data.

    Args:
        source_path: Absolute or relative path to the CSV or JSONL source file.
        cache_base_dir: Directory under which per-dataset Delta caches are stored.
        spark: Active SparkSession.
        schema: Optional Spark schema.  If omitted, schema is inferred from the file.
        partition_by: Column names used for Delta partitioning or liquid clustering.
        liquid_clustering: Write using Delta liquid clustering instead of Hive
            partitioning.  Requires *schema* to be provided.
        debug: Emit extra debug log messages.
    """

    def __init__(
        self,
        source_path: str,
        cache_base_dir: Path,
        spark: SparkSession,
        schema: StructType | None = None,
        partition_by: list[str] | None = None,
        liquid_clustering: bool = False,
        debug: bool = False,
    ) -> None:
        self.source_path = Path(source_path)
        self.spark = spark
        self.schema = schema
        self.partition_by = partition_by
        self.liquid_clustering = liquid_clustering
        self.debug = debug
        self.dataset = self.source_path.stem
        self.cached_path = cache_base_dir / self.dataset

        if self.debug:
            logger.debug("cache_base_dir=%s", cache_base_dir)
            logger.debug("dataset=%s", self.dataset)
            logger.debug("cached_path=%s", self.cached_path)

    @property
    def hash_source(self) -> str:
        """MD5 hex digest of the source file content combined with the schema JSON.

        Returns ``"-2"`` when the source file does not exist so that a missing
        file never falsely matches a populated cache (which uses ``"-1"`` as its
        sentinel for a missing hash file).
        """
        if not self.source_path.exists():
            return "-2"
        content = self.source_path.read_text().encode("UTF-16")
        schema_content = self.schema.json().encode("UTF-16") if self.schema else b""
        return hashlib.md5(content + schema_content).hexdigest()

    @property
    def hash_cache(self) -> str:
        """MD5 hex digest stored alongside the cached Delta table.

        Returns ``"-1"`` when no hash file exists (cache is absent or corrupted).
        """
        hash_file = self.cached_path / "_source_data_hash"
        if hash_file.exists():
            return hash_file.read_text()
        else:
            return "-1"

    def probe_cache(self) -> bool:
        """Return ``True`` if the cached Delta table is up-to-date with the source."""
        return self.hash_source == self.hash_cache

    def cache(self) -> DataFrame:
        """Ensure the Delta cache is valid and return a DataFrame over the source.

        If the source hash matches the stored hash the existing cache is reused.
        Otherwise the cache directory is cleaned, the source is converted to Delta,
        and the hash file is written.

        Returns:
            DataFrame read from the source file (not from the Delta cache).
        """
        if self.probe_cache():
            if self.debug:
                logger.debug("%s: skipping, cached data is up to date", self.dataset)
            return self.read()
        else:
            if self.debug:
                logger.debug("%s: refreshing the cache", self.dataset)

        self.clean_cache()
        df = self.write_delta()
        self.write_cache_hash()
        self.remove_crc_files()

        return df

    def remove_crc_files(self) -> None:
        """Delete all Hadoop CRC sidecar files from the cache directory."""
        for crc_file in Path(self.cached_path).glob("**/*.crc"):
            crc_file.unlink()

    def write_cache_hash(self) -> None:
        """Write the current source hash into the cache directory."""
        self.cached_path.joinpath("_source_data_hash").write_text(self.hash_source)

    def read(self) -> DataFrame:
        """Read the source file into a Spark DataFrame.

        Dispatches to :meth:`read_csv` or :meth:`read_jsonl` based on the file
        extension.

        Raises:
            ValueError: For file extensions other than ``.csv`` or ``.jsonl``.
        """
        if self.source_path.suffix == ".csv":
            return self.read_csv()
        elif self.source_path.suffix == ".jsonl":
            return self.read_jsonl()
        else:
            raise ValueError(f"Unsupported file format: {self.source_path.suffix}")

    def read_jsonl(self) -> DataFrame:
        """Read a JSONL file into a Spark DataFrame.

        Uses the provided schema when available; otherwise infers the schema.
        """
        jsonl_path = self.source_path.as_posix()

        if self.schema:
            return self.spark.read.schema(self.schema).json(jsonl_path)
        else:
            return self.spark.read.option("inferSchema", "true").json(jsonl_path)

    def read_csv(self) -> DataFrame:
        """Read a CSV file into a Spark DataFrame.

        Expects a header row.  Uses the provided schema when available;
        otherwise infers the schema.  Empty strings are treated as ``null``.
        """
        csv_path = self.source_path.as_posix()

        if self.schema:
            return (
                self.spark.read.options(header=True)
                .option("nullValue", "null")
                .schema(self.schema)
                .csv(csv_path)
            )
        else:
            return (
                self.spark.read.options(header=True, inferSchema=True)
                .option("nullValue", "null")
                .csv(csv_path)
            )

    def write_delta(self) -> DataFrame:
        """Convert the source file to Delta and write it to the cache directory.

        Three write modes are supported (in priority order):

        1. **Liquid clustering** — when ``liquid_clustering=True``, creates the table
           via DDL and saves with ``saveAsTable``.  Requires *schema*.
        2. **Partitioned** — when ``partition_by`` is set, writes a Hive-partitioned
           Delta table.
        3. **Plain** — unpartitioned Delta table saved directly to ``cached_path``.

        Returns:
            The source DataFrame (same object returned by :meth:`read`).

        Raises:
            ValueError: When ``liquid_clustering=True`` but no schema is provided.
        """
        if self.liquid_clustering and self.schema is None:
            raise ValueError(
                "liquid_clustering=True requires an explicit schema to be provided"
            )

        reader = self.read()
        delta_location = self.cached_path.as_posix()
        df_writer = reader.repartition(1).write.format("delta").mode("overwrite")

        if self.liquid_clustering:
            ddl = self.construct_table_ddl()
            self.spark.sql(ddl)
            df_writer.saveAsTable(self.dataset)
        elif self.partition_by:
            df_writer.partitionBy(self.partition_by).save(delta_location)
        else:
            df_writer.save(delta_location)

        return reader

    def clean_cache(self) -> None:
        """Remove the cached Delta directory if it exists."""
        if self.cached_path.exists():
            shutil.rmtree(self.cached_path)

    def construct_table_ddl(self) -> str:
        """Build a ``CREATE TABLE … USING DELTA`` DDL statement for liquid clustering.

        Uses ``self.schema`` to enumerate columns and ``self.partition_by`` for the
        ``CLUSTER BY`` clause.

        Returns:
            A DDL string ready to pass to ``spark.sql()``.
        """
        delta_location = self.cached_path.as_posix()
        columns = [
            f"{field.name} {field.dataType.simpleString()}"
            for field in self.schema.fields
        ]
        columns_str = ",\n ".join(columns)

        if self.partition_by:
            cluster_str = f"""CLUSTER BY ({", ".join(self.partition_by)})"""
        else:
            cluster_str = ""

        return f"""
            CREATE TABLE {self.dataset}({columns_str})
            USING DELTA LOCATION '{delta_location}'
            {cluster_str}
        """

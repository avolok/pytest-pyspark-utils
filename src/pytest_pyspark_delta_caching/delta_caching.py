import hashlib
import shutil
from pathlib import Path
from typing import Optional, List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType


class DeltaCaching:
    def __init__(
        self,
        source_path: str,
        cache_base_dir: Path,
        spark: SparkSession,
        schema: Optional[StructType] = None,
        partition_by: Optional[List[str]] = None,
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
            print(f"{cache_base_dir=}")
            print(f"{self.dataset=}")
            print(f"{self.cached_path=}")

    @property
    def hash_source(self):
        csv_source_content = self.source_path.read_text().encode("UTF-16")
        if self.schema:
            schema_content = self.schema.json().encode("UTF-16")
        else:
            schema_content = b""

        if self.source_path.exists():
            return hashlib.md5(csv_source_content + schema_content).hexdigest()
        else:
            return "-2"

    @property
    def hash_cache(self):
        hash_file = self.cached_path / "_source_data_hash"
        if hash_file.exists():
            return hash_file.read_text()
        else:
            return "-1"

    def probe_cache(self) -> bool:
        return self.hash_source == self.hash_cache

    def cache(self) -> DataFrame:
        if self.probe_cache():
            if self.debug:
                print(f"{self.dataset}: skipping, as cached data is up to date")
            return self.read()
        else:
            if self.debug:
                print(f"{self.dataset}: refreshing the cache")

        self.clean_cache()
        df = self.write_delta()
        self.write_cache_hash()
        self.remove_crc_files()

        return df

    def remove_crc_files(self) -> None:
        for crc_file in Path(self.cached_path).glob("**/*.crc"):
            crc_file.unlink()

    def write_cache_hash(self) -> None:
        self.cached_path.joinpath("_source_data_hash").write_text(self.hash_source)

    def read(self) -> DataFrame:
        if self.source_path.suffix == ".csv":
            return self.read_csv()
        elif self.source_path.suffix == ".jsonl":
            return self.read_jsonl()
        else:
            raise Exception(f"Unsupported file format: {self.source_path.suffix}")

    def read_jsonl(self) -> DataFrame:
        jsonl_path = self.source_path.as_posix()

        if self.schema:
            return self.spark.read.schema(self.schema).json(jsonl_path)
        else:
            return self.spark.read.options(header=True, inferSchema=True).json(
                jsonl_path
            )

    def read_csv(self) -> DataFrame:
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
        if self.cached_path.exists():
            shutil.rmtree(self.cached_path)

    def construct_table_ddl(self) -> str:
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

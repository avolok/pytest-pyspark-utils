import hashlib
import shutil
from pathlib import Path
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType


class DeltaCaching:
    def __init__(
        self,
        source_path: str,
        schema_module_path: str,
        spark: SparkSession,
        schema: Optional[StructType] = None,
        partition_by: Optional[str] = None,
        liquid_clustering: bool = False,
        debug: bool = False,
    ) -> None:
        """Initialize the class and setup the paths and defaults"""

        self.source_path = Path(source_path)
        self.schema_module_path = Path(schema_module_path)
        self.spark = spark
        self.schema = schema
        self.partition_by = partition_by
        self.liquid_clustering = liquid_clustering
        self.debug = debug
        _tests_category_path = self.source_path.parent.parent
        _tests_root_path = _tests_category_path.parent
        self.category = _tests_category_path.name
        self.dataset = self.source_path.stem
        _tests_cached_path = _tests_root_path / "_delta_cache" / self.category / self.dataset
        if "tests" not in _tests_root_path.parts:
            raise Exception("This class is only intended to be used from within the tests folder")

        if self.debug:
            print(f"{_tests_root_path=}")
            print(f"{_tests_category_path=}")
            print(f"{_tests_cached_path=}")
            print(f"{self.category=}")
            print(f"{self.dataset=}")

        self.cached_path = _tests_cached_path

    @property
    def hash_source(self):
        """Hash the source data file, so we can compare it to the cached data"""

        # to calculate a combined hash of the input csv file and the schema.py
        # if any of them changed, the cache will be invalidated and recalculated
        csv_source_content = self.source_path.read_text().encode("UTF-16")
        if self.schema_module_path.exists():
            schema_module_content = self.schema_module_path.read_text().encode("UTF-16")
        else:
            schema_module_content = b""

        if self.source_path.exists() :
            return hashlib.md5(csv_source_content + schema_module_content).hexdigest()
        else:
            return "-2"

    @property
    def hash_cache(self):
        """Hash the cached data file, so we can compare it to the source data"""
        if Path(self.cached_path).joinpath("_source_data_hash").exists():
            return self.cached_path.joinpath("_source_data_hash").read_text()
        else:
            return "-1"

    def probe_cache(self) -> bool:
        """Compare the hash of the source data to the hash of the cached data"""
        return self.hash_source == self.hash_cache

    def cache(self) -> Path:
        """Cache the source data as a delta table"""
        if self.probe_cache():
            if self.debug:
                print(
                    f"{self.category}.{self.dataset}: skipping, as cached data is up to date",
                )
            return self.read()
        else:
            if self.debug:
                print(f"{self.category}.{self.dataset}: refreshing the cache")

        self.clean_cache()
        df = self.write_delta()
        self.write_cache_hash()
        self.remove_crc_files()

        return df

    def remove_crc_files(self) -> None:
        """
        Remove the crc files that are created by the delta caching process
        As they are not needed for the tests
        """

        for crc_file in Path(self.cached_path).glob("**/*.crc"):
            crc_file.unlink()

    def write_cache_hash(self) -> None:
        """Write the hash of the source data to the cache"""

        self.cached_path.joinpath("_source_data_hash").write_text(self.hash_source)
        
    def read(self) -> DataFrame:
        """Read the cached data as a spark dataframe"""

        if self.source_path.suffix == ".csv":
            return self.read_csv()
        elif self.source_path.suffix == ".jsonl":
            return self.read_jsonl()
        else:            
            raise Exception(f"Unsupported file format: {self.source_path.suffix}")
        
    def read_jsonl(self) -> DataFrame:
        """Read the source data as a spark dataframe"""

        jsonl_path = self.source_path.as_posix()

        if self.schema:
            reader = (
                self.spark.read                
                .schema(self.schema)
                .json(jsonl_path)
            )
        else:
            reader = (
                self.spark.read
                .options(header=True, inferSchema=True)
                .json(jsonl_path)
            )

        return reader

    def read_csv(self) -> DataFrame:
        """Read the source data as a spark dataframe"""

        csv_path = self.source_path.as_posix()

        if self.schema:
            reader = (
                self.spark.read
                .options(header=True)
                .option("nullValue", "null")
                #.option("quote", '"') # sdt2 only
                #.option("quoteAll", "true") # for all versions
                #.option("multiline", "true") # to be fixed in upcoming US
                .schema(self.schema)
                .csv(csv_path)
            )
        else:
            reader = (
                self.spark.read
                .options(header=True, inferSchema=True)
                .option("nullValue", "null")
                #.option("quote", '"') # sdt2 only
                #.option("quoteAll", "true") # for all versions
                #.option("multiline", "true")  # to be fixed in upcoming US
                .csv(csv_path)
            )

        return reader

    def write_delta(self) -> DataFrame:
        """Write the source data as a delta table"""

        reader = self.read()
        delta_location = self.cached_path.as_posix()
        df_writer = reader.repartition(1).write.format("delta").mode("overwrite")
        
        # Uncomment this part to generate json files instead of the current format (csv with specific options)
        # json output to generate new files
        # json_spark_path = Path("./task-interpreter/tests/sdt2_task_interpreter/input_json_spark" ) / (self.cached_path.name)
        # Path("./task-interpreter/tests/sdt2_task_interpreter/input_jsonl" ).mkdir(parents=True, exist_ok=True)
        # jsonl_path = Path("./task-interpreter/tests/sdt2_task_interpreter/input_jsonl" )
        # (
        #     reader
        #     .repartition(1)
        #     .write
        #     .format("json")
        #     .options(header=True, nullValue="null", quote='"', quoteAll="true", timestampFormat="yyyy-MM-dd'T'HH:mm:ss.SSSSSS")
        #     .mode("overwrite")
        #     .save(json_spark_path.as_posix())
        # )
        
        # # search for the generated json file and print its content for debugging
        # for json_file in json_spark_path.glob("*.json"):
        #     print(f"Generated JSON file: {json_file}")
            
        #     # copy json file to the cached path for debugging
        #     (jsonl_path / (self.cached_path.name + ".jsonl")).write_text(json_file.read_text())
        
        

        if self.liquid_clustering:
            print(f"Using liquid clustering for {self.dataset}")
            ddl = self.construct_table_ddl()
            self.spark.sql(ddl)
            df_writer.saveAsTable(self.dataset)        

        elif not self.liquid_clustering and self.partition_by:
            print(f"Using partitioning for {self.dataset}")
            df_writer.partitionBy(self.partition_by).save(delta_location)
        else:
            df_writer.save(delta_location)

        return reader

    def clean_cache(self) -> None:
        """Remove the cached data"""

        if self.cached_path.exists():
            shutil.rmtree(self.cached_path)


    def construct_table_ddl(self) -> str:
        """Construct the DDL for the cached data"""

        delta_location = self.cached_path.as_posix()
        columns = [f"{field.name} {field.dataType.simpleString()}" for field in self.schema.fields]
        columns_str = ",\n ".join(columns)

        if self.partition_by:
            cluster_str = f"""CLUSTER BY ({", ".join(self.partition_by)})"""
        else:
            cluster_str = ""

        ddl = f"""
            CREATE TABLE {self.dataset}({columns_str}) 
            USING DELTA LOCATION '{delta_location}'
            {cluster_str}
        """

        return ddl

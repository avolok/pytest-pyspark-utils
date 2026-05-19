pytest_plugins = 'pytester'

import pytest
import os
import shutil
from pathlib import Path
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from typing import Generator, List, Optional
from tests.delta_caching import DeltaCaching

THIS_DIR = os.path.dirname(os.path.abspath(__file__))


@pytest.fixture(scope="session")
def disposable_tmp_dir(tmpdir_factory):
    """Create a temporary directory that is deleted after the test"""

    # An extra precaution to make sure that temp directory is empty
    base_path = Path(tmpdir_factory.getbasetemp())

    base_path_delta = tmpdir_factory.mktemp("delta")

    try:
        print(f"\nCreating temporary location: {str(base_path_delta)}")
        yield Path(base_path_delta.strpath).as_posix()
    finally:
        shutil.rmtree(str(base_path_delta))
        print(f"\nRemoved {str(base_path_delta)}")


@pytest.fixture(scope="module")
def temp_delta_path(disposable_tmp_dir, request):
    """Returns a path where the delta tables going to be stored for the test"""
    test_delta_tables_path = (Path(disposable_tmp_dir) / Path(request.node.name).stem).as_posix()
    yield test_delta_tables_path


@pytest.fixture(scope="function")
def disposable_tmp_dir_per_function(tmp_path):
    """Create a temporary directory that is deleted after the test"""

    temp_path = tmp_path

    try:
        print(f"\nCreating temporary location: {str(temp_path)}")
        yield Path(temp_path).as_posix()
    finally:
        shutil.rmtree(str(temp_path))
        print(f"\nRemoved {str(temp_path)}")


@pytest.fixture(scope="module")
def get_test_paths():
    """Returns the paths to the input and expected output files for a test"""

    def _get_test_paths(test_file_location: Path):
        if test_file_location.as_posix().startswith("/dbfs"):
            test_file_location = Path(test_file_location.as_posix().replace("/dbfs", ""))

        test_group_location = test_file_location.parent

        test_input_path = (test_group_location / "input").as_posix()
        test_expected_output_path = (test_group_location / "expected_output").as_posix()

        return test_input_path, test_expected_output_path

    yield _get_test_paths


@pytest.fixture(scope="module")
def prepare_dataset_cached(spark, temp_delta_path):
    """
    Prepare a dataset from a csv file and save it as a Delta table
    This fixture to be used locally and on test agents
    """

    def _prepare_dataset(
        file_name: str,
        schema: Optional[StructType] = None,
        table_name: Optional[str] = None,
        partition_by: Optional[List[str]] = None,
        liquid_clustering: bool = False,
    ) -> DataFrame:
        temporary_dir_base = Path(temp_delta_path)

        if table_name:
            delta_target_path = temporary_dir_base / table_name
        else:
            delta_target_path = temporary_dir_base / Path(file_name).stem

        delta_caching = DeltaCaching(
            source_path=file_name,
            schema_module_path=f"{THIS_DIR}/schemas.py",
            spark=spark,
            schema=schema,
            partition_by=partition_by,
            liquid_clustering=liquid_clustering,
            debug=False,
        )

        _df = delta_caching.cache()
        delta_cache_path = delta_caching.cached_path

        shutil.copytree(delta_cache_path, delta_target_path)

        if table_name:
            spark.sql(f"DROP TABLE IF EXISTS {table_name}")
            spark.sql(f"CREATE TABLE {table_name} USING DELTA LOCATION '{delta_target_path.as_posix()}'")
            print(f"successfully created delta table for {table_name}")

        return _df

    yield _prepare_dataset


@pytest.fixture(scope="module")
def prepare_dataset(spark, temp_delta_path):
    """
    Prepare a dataset from a csv file and save it as a Delta table
    This fixture is indended to be used on Databricks only
    """

    def _prepare_dataset(
        file_name: str,
        schema: StructType = None,
        table_name: str = None,
        partition_by: List[str] = None,
    ) -> DataFrame:
        temporary_dir_base = Path(temp_delta_path)

        if table_name:
            delta_path = temporary_dir_base / table_name
        else:
            delta_path = temporary_dir_base / Path(file_name).stem

        if schema:
            _df = spark.read.schema(schema).options(header=True).csv(file_name)
        else:
            _df = spark.read.options(header=True, inferSchema=True).csv(file_name)

        _df_writer = _df.write.format("delta").option("path", delta_path)

        if partition_by:
            _df_writer = _df_writer.partitionBy(*partition_by)

        if table_name:
            _df_writer.saveAsTable(name=table_name, mode="ignore")
        else:
            _df_writer.save(mode="overwrite")

        return _df

    yield _prepare_dataset


@pytest.fixture(scope="module")
def prepare_tables_for_test(prepare_dataset, prepare_dataset_cached, temp_delta_path):
    def _prepare_tables_for_test(files: dict):
        start = datetime.now()
        output = {}

        dataset_fixture = prepare_dataset_cached

        for filename, config in files.items():
            table_name = config.table_name or filename
            _df = dataset_fixture(
                file_name=determine_file_path(base_path=config.location, filename=filename),
                schema=config.schema,
                table_name=table_name,
                partition_by=config.partition_by,
                liquid_clustering=config.liquid_clustering,
            )
            output[filename] = _df

            print(f"successfully created delta table for {filename}")

        duration = round((datetime.now() - start).total_seconds(), 1)
        print(f"done with creating tables ({duration}s).")

        output["delta_tables_path"] = temp_delta_path

        return output

    return _prepare_tables_for_test

def determine_file_path(base_path: str, filename: str) -> str:
    """
    Determine the file path for a given filename within a specified base path.

    This function searches for files matching the given filename (with any extension) in the specified base path.
    It ensures that exactly one matching file is found, raising an error if no matches or multiple matches are found.

    Args:
        base_path (str): The directory path where the search for the file will be performed.
        filename (str): The base name of the file (without extension) to search for.

    Returns:
        str: The full path to the matching file.

    Raises:
        FileNotFoundError: If no file matching the given filename is found in the base path.
        FileExistsError: If multiple files matching the given filename are found in the base path.
    """
    # search for two acceptible extensions using glob:
    file_matches = [file for file in Path(base_path).glob(f"{filename}.*") if file.suffix in ['.jsonl', '.csv']]

    if not file_matches:
        raise FileNotFoundError(f"No file found for {filename} in {base_path}")
    elif len(file_matches) > 1:
        raise FileExistsError(
            f"Multiple files found for {filename} in {base_path}: {[file.name for file in file_matches]}"
            f"Please ensure there is only one file for {filename} in the directory."
        )
    else:
        return f"{base_path}/{file_matches[0].name}"


@pytest.fixture(scope="function")
def isolate_tables(temp_delta_path, disposable_tmp_dir):
    """Isolate the delta tables so they can be used in each test function separately"""

    def _isolated_table(fixture_name, tmp_path):
        """Isolate the tables for each test function"""

        start = datetime.now()
        delta_path = temp_delta_path
        disposable_tmp = disposable_tmp_dir

        source_location = delta_path
        destination_location = Path(disposable_tmp) / "isolated_tables" / tmp_path.name
        destination_location = destination_location.as_posix()

        shutil.copytree(Path(source_location), Path(destination_location), dirs_exist_ok=True)

        output = fixture_name
        output["isolated_delta_tables_path"] = destination_location
        os.environ["UNIT_TESTS_DELTA_PATH"] = destination_location  # to pass the isolated path to the conf.py

        duration = round((datetime.now() - start).total_seconds(), 1)
        print(f"Isolated delta tables by copying from {source_location} to {destination_location} " f"({duration}s).")

        return output

    yield _isolated_table


@pytest.fixture(scope="function")
def remove_mrec_columns():
    """Remove system columns from a dataframe"""

    def _remove_mrec_columns(df: DataFrame):
        return df.drop(
            "m_rec_original_state_fact_id",
            "m_rec_inserted_utc",
            "m_rec_inserted_process_name",
            "m_rec_inserted_process_version",
            "m_rec_inserted_process_id",
            "m_rec_updated_utc",
            "m_rec_updated_process_name",
            "m_rec_updated_process_version",
            "m_rec_updated_process_id",
            # "root_error_code",
            "original_non_merged_states_id",  # this column is non-deterministic
            "original_merged_states_id",  # this column is non-deterministic
        )

    yield _remove_mrec_columns


@pytest.fixture(scope="function")
def clear_imports():
    """Removes tasker modules from sys.modules to avoid side effects between tests.
    As an example: configuration is imported in the main module and it is cached in sys.modules.
    """

    for key in list(sys.modules.keys()):
        if (
            key.endswith("tasker_conf")
            or key.endswith("tasker_execute_all_sequences")
            or key.startswith("delphi_lib")
            or key.startswith("tasker_lib")
        ):
            del sys.modules[key]


@pytest.fixture(scope="function")
def drop_hive_objects(spark: SparkSession) -> None:
    # get all tables
    tables = spark.sql("show tables").collect()

    # drop all tables
    for table in tables:
        if table.namespace:
            _sql = f"DROP TABLE IF EXISTS {table.namespace}.{table.tableName}"
        else:
            _sql = f"DROP TABLE IF EXISTS {table.tableName}"

        spark.sql(_sql)


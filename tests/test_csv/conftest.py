import json
from pathlib import Path

import pytest
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
import pytest
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Schemas for the sample tables, used in the sample_tables fixture
dataset1_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("department", StringType(), True),
])


# Fixture to create sample tables for testing, used in the test_sample_tables.py
@pytest.fixture(scope="module")
def sample_tables(get_test_paths, prepare_tables_for_test):
    test_input_path, test_expected_output_path = get_test_paths(Path(__file__))

    # each file maps to a tuple of (schema, location, table_name, partition_by, liquid_clustering)
    files = {
       
       "dataset1": (dataset1_schema, test_input_path, "dataset1", ["id"], False),
       
        
    }
    print("reading in files, needed for task interpreter tests")

    return prepare_tables_for_test(files)

# to create unique isolated tables for each test function, 
# we need to copy the cached tables to a new location for each test function, 
# and pass the new location to the tests via an environment variable
@pytest.fixture(scope="function")
def isolated_sample_tables(
    clear_imports, drop_hive_objects, isolate_tables, sample_tables, tmp_path
):
    return isolate_tables(sample_tables, tmp_path)
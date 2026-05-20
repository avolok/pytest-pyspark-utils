import pytest
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from pytest_pyspark_utils import TableConfig

dataset1_schema = StructType(
    [
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("department", StringType(), True),
    ]
)


@pytest.fixture(scope="module")
def delta_tables_config():
    return {
        "dataset1": TableConfig(
            source="input",
            schema=dataset1_schema,
            partition_by=["id"],
        ),
        "expected_dataset1": TableConfig(
            source="expected",
            schema=dataset1_schema,
            partition_by=["id"],
            liquid_clustering=True,
        ),
    }

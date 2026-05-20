import pytest
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType

from pytest_pyspark_utils import TableConfig

employees_schema = StructType(
    [
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("department", StringType(), True),
    ]
)

employees_stats_schema = StructType(
    [
        StructField("department", StringType(), True),
        StructField("employee_count", LongType(), True),
    ]
)


@pytest.fixture(scope="module")
def delta_tables_config():
    return {
        "employees": TableConfig(source="input", schema=employees_schema),
        "expected_employees_stats": TableConfig(
            source="expected", schema=employees_stats_schema
        ),
    }

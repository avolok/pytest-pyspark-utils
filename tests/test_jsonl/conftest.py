import pytest


from pytest_pyspark_utils import TableConfig
from tests.schema import employees_schema, employees_stats_schema


@pytest.fixture(scope="module")
def delta_tables_config():
    return {
        "employees": TableConfig(source="input", schema=employees_schema),
        "expected_departments_stats": TableConfig(
            source="expected", schema=employees_stats_schema
        ),
    }

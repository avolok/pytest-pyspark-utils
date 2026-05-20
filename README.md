# pytest-pyspark-utils

A pytest plugin that provides a reusable `spark` session fixture and automated Delta table caching for PySpark testing. Eliminates boilerplate Spark session setup and speeds up tests by caching CSV/JSONL-to-Delta conversions.

## Features

- Session-scoped `spark` fixture — one Spark session per test run, shared across all tests
- Optional Delta Lake support via configurable Maven JAR coordinates
- **Delta table caching** — CSV/JSONL files are converted to Delta once and cached between runs
- **Per-test isolation** — each test gets its own copy of the Delta tables via the `delta_tables` fixture
- PySpark version-agnostic — works with PySpark 3.x and 4.x
- Configurable via `pytest.ini`, `pyproject.toml`, or CLI flags

## Installation

Install the plugin with your chosen PySpark version:

```bash
# PySpark 4.x
pip install "pytest-pyspark-utils[pyspark4]"

# PySpark 3.x
pip install "pytest-pyspark-utils[pyspark3]"

# Or pin an exact version alongside the plugin
pip install pytest-pyspark-utils pyspark==4.0.2
```

## Usage of fixtures

### Spark fixture

Once installed, the `spark` fixture is automatically available in all your tests — no import or conftest wiring needed.

```python
from pyspark.sql import SparkSession

def test_something(spark: SparkSession):
    df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "value"])
    assert df.count() == 2
```

### Delta_tables fixture

The plugin can automatically convert your test data files (CSV or JSONL) into cached Delta tables and register them as Spark SQL tables. Each test gets an isolated copy.

#### 1. Organize your test data

```
tests/test_my_feature/
├── conftest.py
├── input/
│   ├── users.csv
│   └── orders.csv
├── expected/
│   └── results.csv
└── test_my_feature.py
```

#### 2. Define your table config in `conftest.py`

```python
# tests/test_my_feature/conftest.py
import pytest
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pytest_pyspark_utils import TableConfig

users_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
])

orders_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("amount", IntegerType(), True),
])

@pytest.fixture(scope="module")
def delta_tables_config():
    return {
        "users": TableConfig(
            source="input",
            schema=users_schema,
            partition_by=["id"],
        ),
        "orders": TableConfig(
            source="input",
            schema=orders_schema,
        ),
        "expected_results": TableConfig(
            source="expected",
            schema=orders_schema,
        ),
    }
```

#### 3. Use `delta_tables` in your tests

```python
# tests/test_my_feature/test_my_feature.py
def test_user_orders(spark, delta_tables):
    users = spark.table("users")
    orders = spark.table("orders")

    result = users.join(orders, users.id == orders.user_id)
    assert result.count() > 0

def test_another_scenario(spark, delta_tables):
    # Each test gets a fresh, isolated copy of all tables
    spark.sql("DELETE FROM users WHERE id = 1")
    assert spark.table("users").count() == 4  # won't affect other tests
```

### How `delta_tables` works

The fixture chain operates in two layers:

1. **Module-level caching** (runs once per test file): Reads CSV/JSONL files, converts them to Delta format, and caches the result in `<test_dir>/_delta_cache/`. On subsequent runs, if the source file and schema haven't changed, the cached Delta is reused instantly.

2. **Function-level isolation** (runs per test): Copies the cached Delta tables to a temporary directory, drops any existing Hive tables, and re-registers fresh tables pointing to the isolated copy.

This means the first run pays the conversion cost, but subsequent runs are fast — and every test is guaranteed a clean slate.

### `TableConfig` reference

```python
@dataclass
class TableConfig:
    source: str = "input"           # "input", "expected", or an absolute path
    schema: Optional[StructType] = None  # PySpark schema (recommended for consistency)
    table_name: Optional[str] = None     # Defaults to the dict key
    partition_by: Optional[List[str]] = None
    liquid_clustering: bool = False
```

| Field | Description |
|-------|-------------|
| `source` | Where to find the data file. `"input"` resolves to `<test_dir>/input/`, `"expected"` resolves to `<test_dir>/expected/`. Or pass an absolute path. |
| `schema` | PySpark `StructType`. If omitted, schema is inferred from the file. |
| `table_name` | The Spark SQL table name. Defaults to the dictionary key. |
| `partition_by` | List of columns to partition the Delta table by. |
| `liquid_clustering` | Use Delta liquid clustering instead of traditional partitioning. |

## Configuration

### With Delta Lake

Set the Delta JAR coordinates in `pyproject.toml`:

```toml
[tool.pytest.ini_options]
delta_jar = "io.delta:delta-spark_2.13:4.0.1"
spark_app_name = "my-project-tests"
delta_cache_dir = "_delta_cache"
```

Or in `pytest.ini`:

```ini
[pytest]
delta_jar = io.delta:delta-spark_2.13:4.0.1
spark_app_name = my-project-tests
```

Or pass it directly on the command line:

```bash
pytest --delta-jar=io.delta:delta-spark_2.13:4.0.1
```

When `delta_jar` is not set, the fixture starts a plain Spark session without Delta extensions.

### Delta JAR coordinates by PySpark version

| PySpark | Delta JAR coordinates                    |
|---------|------------------------------------------|
| 4.0.x   | `io.delta:delta-spark_2.13:4.0.1`        |
| 3.5.x   | `io.delta:delta-spark_2.12:3.2.0`        |
| 3.3.x   | `io.delta:delta-core_2.12:2.3.0`         |

### Available options

| Option          | `pytest.ini` key  | CLI flag          | Default          | Description                              |
|-----------------|-------------------|-------------------|------------------|------------------------------------------|
| Delta JAR       | `delta_jar`       | `--delta-jar`     | _(none)_         | Maven coordinates for Delta Lake JAR     |
| App name        | `spark_app_name`  | —                 | `pytest-pyspark` | Spark application name                   |
| Cache dir       | `delta_cache_dir` | —                 | `_delta_cache`   | Directory name for cached Delta tables   |

## How it works

The plugin registers several fixtures via the pytest entry point:

| Fixture | Scope | Description |
|---------|-------|-------------|
| `spark` | session | PySpark session with optional Delta support |
| `delta_tables` | function | Isolated Delta tables, registered as Spark SQL tables |
| `prepare_tables_for_test` | module | Lower-level helper for custom table preparation |
| `drop_hive_objects` | function | Drops all Spark SQL tables (cleanup utility) |

## License

[GNU GPL v3.0+](LICENSE)

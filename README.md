# pytest-pyspark-delta-caching

A pytest plugin that provides a reusable `spark` session fixture for PySpark and Delta Lake testing. Eliminates the need to copy-paste Spark session setup across projects.

## Features

- Session-scoped `spark` fixture — one Spark session per test run, shared across all tests
- Optional Delta Lake support via configurable Maven JAR coordinates
- PySpark version-agnostic — works with PySpark 3.x and 4.x
- Performance-tuned defaults (disabled UI, minimized shuffle partitions, UTC timezone)
- Configurable via `pytest.ini`, `pyproject.toml`, or CLI flags

## Installation

Install the plugin with your chosen PySpark version:

```bash
# PySpark 4.x
pip install "pytest-pyspark-delta-caching[pyspark4]"

# PySpark 3.x
pip install "pytest-pyspark-delta-caching[pyspark3]"

# Or pin an exact version alongside the plugin
pip install pytest-pyspark-delta-caching pyspark==4.0.2
```

## Usage

Once installed, the `spark` fixture is automatically available in all your tests — no import or conftest wiring needed.

```python
from pyspark.sql import SparkSession

def test_something(spark: SparkSession):
    df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "value"])
    assert df.count() == 2
```

## Configuration

### With Delta Lake

Set the Delta JAR coordinates in `pyproject.toml`:

```toml
[tool.pytest.ini_options]
delta_jar = "io.delta:delta-spark_2.13:4.0.1"
spark_app_name = "my-project-tests"
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

## How it works

The plugin registers a `session`-scoped `spark` fixture that:

1. Creates a `local[*]` Spark session with performance-tuned settings
2. Configures Delta extensions if `delta_jar` is provided
3. Creates and activates a randomly-named temporary database to isolate tests
4. Stops the session and cleans up after the test run

## License

[GNU GPL v3.0+](LICENSE)

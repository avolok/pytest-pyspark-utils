# AI Agent Instructions — pytest-pyspark-utils

A pytest plugin providing reusable PySpark fixtures with Delta Lake table caching.

## Commands

All commands use **uv** — never use `pip`, `python`, or bare `pytest` directly.

```bash
# Install dependencies
uv sync --group dev

# Run tests
uv run pytest tests/

# Run tests with coverage
uv run pytest --cache-clear --cov=pytest_pyspark_utils tests/

# Lint & format
uv run ruff check --fix .
uv run ruff format .

# Pre-commit hooks
uv run pre-commit run --all-files
```

## Architecture

Source lives in `src/pytest_pyspark_utils/`:

| Module | Role |
|---|---|
| `plugin.py` | Pytest entry point — CLI options, INI config registration |
| `fixtures.py` | All fixtures: `spark` (session-scoped), `delta_tables` (function-scoped) |
| `models.py` | `TableConfig`, `DeltaTablesResult` data classes |
| `delta_caching.py` | CSV/JSONL → Delta conversion with MD5-based cache invalidation |
| `utils.py` | Helpers: `determine_delta_jar()`, `determine_file_path()` |

## Conventions

- **Style**: Ruff, line length 120, auto-fix enabled
- **Commits**: [Conventional Commits](https://www.conventionalcommits.org/) (`fix:`, `feat:`, `refactor:`)
- **Tests**: Test data in `input/`+`expected/` subdirs; use `chispa.assert_df_equality()` for DataFrame assertions
- **Fixtures**: Define `delta_tables_config` (module-scoped) to configure test tables via `TableConfig`
- **Versioning**: Automated via python-semantic-release on main branch

## Testing patterns

```python
# Define tables in conftest.py
@pytest.fixture(scope="module")
def delta_tables_config():
    return [
        TableConfig(source_dir="input", table_name="employees", schema=my_schema),
    ]

# Use in tests
def test_example(spark, delta_tables):
    df = spark.read.format("delta").load(delta_tables.dataframes["employees"])
    # assert with chispa
```

## Key details

- PySpark 3.x and 4.x compatible — `determine_delta_jar()` in `utils.py` maps versions to Delta JARs
- Each test gets an isolated copy of Delta tables (function-scoped)
- `uv.lock` is committed — always use `uv sync`, never `uv pip install`
- See [README.md](README.md) for full configuration reference and usage examples

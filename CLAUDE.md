# CLAUDE.md — pytest-pyspark-utils

## Commands

All commands MUST use `uv`. Never use bare `pip`, `python`, or `pytest`.

```bash
uv sync --group dev          # Install deps
uv run pytest tests/         # Run tests
uv run pytest --cache-clear --cov=pytest_pyspark_utils tests/  # Tests + coverage
uv run ruff check --fix .    # Lint
uv run ruff format .         # Format
uv run pre-commit run --all-files  # Pre-commit hooks
```

## Project

Pytest plugin with PySpark fixtures and Delta Lake table caching. Source in `src/pytest_pyspark_utils/`.

- **Style**: Ruff, 120 char lines
- **Commits**: Conventional Commits (`fix:`, `feat:`, `refactor:`)
- **Tests**: `chispa.assert_df_equality()` for DataFrame comparisons; test data in `input/`+`expected/` subdirs
- **Versioning**: python-semantic-release on main; `uv.lock` is committed

See [AGENTS.md](AGENTS.md) for full architecture and testing patterns.

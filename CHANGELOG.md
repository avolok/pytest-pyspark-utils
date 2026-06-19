# CHANGELOG

<!-- version list -->

## v1.3.1 (2026-06-19)

### Bug Fixes

- Rename 'tables' to 'dataframes' in delta_tables fixture
  ([#13](https://github.com/avolok/pytest-pyspark-utils/pull/13),
  [`7488ded`](https://github.com/avolok/pytest-pyspark-utils/commit/7488dedfa6fbf96661c20d80e81b9623956884bc))

### Refactoring

- Rename 'tables' to 'dataframes' in DeltaTablesResult and related functions
  ([#12](https://github.com/avolok/pytest-pyspark-utils/pull/12),
  [`9378946`](https://github.com/avolok/pytest-pyspark-utils/commit/937894605f5f974e4490e0e910d861e61d2475b1))


## v1.3.0 (2026-06-19)

### Bug Fixes

- Move Spark SQL warehouse into session tmp directory
  ([#11](https://github.com/avolok/pytest-pyspark-utils/pull/11),
  [`f5fd971`](https://github.com/avolok/pytest-pyspark-utils/commit/f5fd97115ec09e9f9fb9ff8af560fb02efab7b51))

### Features

- Add spark_driver_memory configuration option for Spark session
  ([#11](https://github.com/avolok/pytest-pyspark-utils/pull/11),
  [`f5fd971`](https://github.com/avolok/pytest-pyspark-utils/commit/f5fd97115ec09e9f9fb9ff8af560fb02efab7b51))

- Make spark.driver.memory configurable
  ([#11](https://github.com/avolok/pytest-pyspark-utils/pull/11),
  [`f5fd971`](https://github.com/avolok/pytest-pyspark-utils/commit/f5fd97115ec09e9f9fb9ff8af560fb02efab7b51))


## v1.2.0 (2026-06-18)

### Features

- Auto-detect Delta Lake JAR from installed PySpark version
  ([#10](https://github.com/avolok/pytest-pyspark-utils/pull/10),
  [`2dad138`](https://github.com/avolok/pytest-pyspark-utils/commit/2dad1381dfa88b1d914c77bcc83772169c980c39))

- Set UNIT_TEST_TMP_DIR environment variable in delta_tables fixture
  ([#10](https://github.com/avolok/pytest-pyspark-utils/pull/10),
  [`2dad138`](https://github.com/avolok/pytest-pyspark-utils/commit/2dad1381dfa88b1d914c77bcc83772169c980c39))


## v1.1.0 (2026-06-18)

### Features

- Auto-detect Delta Lake JAR from installed PySpark version
  ([#9](https://github.com/avolok/pytest-pyspark-utils/pull/9),
  [`c765911`](https://github.com/avolok/pytest-pyspark-utils/commit/c7659117cd6c63e0eac618dc6e328fbe71098d5f))

### Refactoring

- Split plugin.py into smaller modules ([#8](https://github.com/avolok/pytest-pyspark-utils/pull/8),
  [`c259ef4`](https://github.com/avolok/pytest-pyspark-utils/commit/c259ef4f5c976ba7df9987e1a6fc13699ecfcb25))


## v1.0.3 (2026-05-20)

### Bug Fixes

- Update readme file reference in pyproject.toml from README.rst to README.md
  ([#7](https://github.com/avolok/pytest-pyspark-utils/pull/7),
  [`6c3ec95`](https://github.com/avolok/pytest-pyspark-utils/commit/6c3ec9562b70a48e745c234546725159b42de9bf))


## v1.0.2 (2026-05-20)

### Bug Fixes

- Add deploy step to CD pipeline for publishing package distributions to PyPI
  ([#6](https://github.com/avolok/pytest-pyspark-utils/pull/6),
  [`aa31dc9`](https://github.com/avolok/pytest-pyspark-utils/commit/aa31dc90020869daa53f3eb155cdb620e9082185))


## v1.0.1 (2026-05-20)

### Bug Fixes

- Update GitHub token secret reference in CD workflow
  ([#5](https://github.com/avolok/pytest-pyspark-utils/pull/5),
  [`ddb94a9`](https://github.com/avolok/pytest-pyspark-utils/commit/ddb94a9eb737e56b1159718d2f3405d76474d43b))


## v1.0.0 (2026-05-20)

- Initial Release

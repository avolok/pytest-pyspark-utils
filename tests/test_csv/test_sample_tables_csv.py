import os

from sample_pyspark_app.stats import count_employees_by_department
from chispa import assert_df_equality


def test_tables(spark, delta_tables):
    """Test that the sample tables fixture works"""

    # arrange
    input_df = spark.table("employees")
    expected_df = spark.table("expected_departments_stats")

    # act
    result_df = count_employees_by_department(input_df)

    # assert
    assert_df_equality(result_df, expected_df, ignore_row_order=True, ignore_nullable=True)


def test_unit_test_tmp_dir_env_var(delta_tables):
    """Test that delta_tables fixture sets UNIT_TEST_TMP_DIR to the isolated tables path"""
    assert os.environ.get("UNIT_TEST_TMP_DIR") == delta_tables.path

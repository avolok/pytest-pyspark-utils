from sample_pyspark_app.stats import count_employees_by_department
from chispa import assert_df_equality


def test_tables(spark, delta_tables):
    """Test that the sample tables fixture works"""

    # arrange
    input_df = spark.table("employees")
    expected_df = spark.table("expected_employees_stats")

    # act
    result_df = count_employees_by_department(input_df)

    # assert
    assert_df_equality(
        result_df, expected_df, ignore_row_order=True, ignore_nullable=True
    )

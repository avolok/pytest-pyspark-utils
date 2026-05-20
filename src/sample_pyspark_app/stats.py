from pyspark.sql import DataFrame


def count_employees_by_department(input_df: DataFrame) -> DataFrame:
    """Counts the number of employees in each department.

    Args:
        input_df (DataFrame): A DataFrame containing employee data with at least a "department" column.
    Returns:
        DataFrame: A DataFrame with two columns: "department" and "employee_count", where "employee_count"
        is the number of employees in each department.
    """

    return (
        input_df.groupBy("department")
        .count()
        .withColumnRenamed("count", "employee_count")
    )

from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType

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

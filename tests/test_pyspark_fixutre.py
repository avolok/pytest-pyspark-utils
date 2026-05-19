from pyspark.sql import SparkSession


def test_pyspark_fixture(spark: SparkSession):
    """Test that the Spark Session fixture works"""
    assert spark is not None
    assert spark.version is not None


def test_tables(spark, sample_tables):
    """Test that the sample tables fixture works"""
    
    assert sample_tables is not None
    assert len(sample_tables) > 0
    assert spark.table("dataset1").count() == 5
def test_tables(spark, delta_tables):
    """Test that the sample tables fixture works"""

    assert delta_tables is not None
    assert len(delta_tables) > 0
    assert spark.table("dataset1").count() == 5

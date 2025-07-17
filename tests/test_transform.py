import pytest
from pyspark.sql import SparkSession
from etl.transform import clean_claims


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local").appName("test").getOrCreate()


def test_clean_claims_basic(spark):
    data = [("open", 5000), ("rejected", 8000), ("closed", -3000), ("open", 15000)]
    df = spark.createDataFrame(data, ["status", "amount"])

    result = clean_claims(df).collect()

    assert len(result) == 3  # Filtered out 1 rejected claim
    assert result[1]["amount_clean"] == 0  # Change negative amount to 0
    assert result[2]["is_large_claim"] == True

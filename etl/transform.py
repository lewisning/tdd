from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def clean_claims(df: DataFrame) -> DataFrame:
    """
    Clean claims DataFrame.
    - Filter rejected claims
    - Clean amount values (set negative amounts to 0)
    - Add a new column labels indicating if the claim is large (amount > 10000)
    """
    return df.filter(F.col("status") != "rejected") \
             .withColumn("amount_clean",
                         F.when(F.col("amount") < 0, 0).otherwise(F.col("amount"))) \
             .withColumn("is_large_claim", F.col("amount_clean") > 10000)

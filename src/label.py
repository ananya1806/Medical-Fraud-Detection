from pyspark.sql.functions import when, col

def add_fraud_label(df, leie):
    # Get only provider IDs from LEIE
    leie_ids = leie.select("provider_id").distinct()

    # Join with main dataframe
    df = df.join(
        leie_ids.withColumn("fraud_flag", col("provider_id")),
        on="provider_id",
        how="left"
    )

    # Create label
    df = df.withColumn(
        "is_fraud",
        when(col("fraud_flag").isNotNull(), 1).otherwise(0)
    ).drop("fraud_flag")

    return df
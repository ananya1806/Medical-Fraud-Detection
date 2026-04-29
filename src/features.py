from pyspark.sql.functions import col, when, log1p


def build_features(df):

    df = df.fillna({
        "total_payments": 0,
        "Tot_Clms": 0,
        "Tot_Benes": 0,
        "Tot_Drug_Cst": 0,
        "unique_drugs": 0,
        "total_drug_claims": 0,
        "recipient_interactions": 0
    })

    df = df.withColumn(
        "claims_per_drug",
        col("total_drug_claims") / (col("unique_drugs") + 1)
    )

    df = df.withColumn(
        "avg_cost_per_claim",
        when(col("Tot_Clms") != 0,
             col("Tot_Drug_Cst") / col("Tot_Clms")
        ).otherwise(0)
    )

    df = df.withColumn(
        "payment_per_claim",
        when(col("Tot_Clms") != 0,
             col("total_payments") / col("Tot_Clms")
        ).otherwise(0)
    )

    df = df.withColumn(
        "claims_per_beneficiary",
        when(col("Tot_Benes") != 0,
             col("Tot_Clms") / col("Tot_Benes")
        ).otherwise(0)
    )

    df = df.withColumn(
        "log_payments",
        log1p(col("total_payments"))
    )

    df = df.withColumn(
        "high_payment_flag",
        when(col("total_payments") > 10000, 1).otherwise(0)
    )

    df = df.withColumn(
        "high_cost_flag",
        when(col("avg_cost_per_claim") > 500, 1).otherwise(0)
    )

    return df
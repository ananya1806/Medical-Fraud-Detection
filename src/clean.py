from pyspark.sql.functions import col, trim, lpad, regexp_replace


def clean_prescribers(df):
    df = df.withColumnRenamed("PRSCRBR_NPI", "provider_id")

    df = df.withColumn(
        "provider_id",
        lpad(trim(col("provider_id").cast("string")), 10, "0")
    )

    return df


def clean_payments(df):
    df = df.withColumnRenamed("Covered_Recipient_NPI", "provider_id")

    df = df.withColumn("provider_id", col("provider_id").cast("string"))
    df = df.withColumn("provider_id", regexp_replace(col("provider_id"), "\\.0$", ""))
    df = df.withColumn("provider_id", trim(col("provider_id")))

    df = df.withColumn(
        "provider_id",
        lpad(col("provider_id"), 10, "0")
    )

    df = df.withColumnRenamed(
        "Total_Amount_of_Payment_USDollars",
        "payment_amount"
    )

    df = df.withColumn("payment_amount", col("payment_amount").cast("double"))

    df = df.filter(col("provider_id").isNotNull())

    return df


def clean_leie(df):
    if "EXCLNPI" in df.columns:
        df = df.withColumnRenamed("EXCLNPI", "provider_id")
    elif "NPI" in df.columns:
        df = df.withColumnRenamed("NPI", "provider_id")

    df = df.withColumn(
        "provider_id",
        lpad(trim(col("provider_id").cast("string")), 10, "0")
    )

    return df


def clean_drug_data(df):
    df = df.withColumnRenamed("Prscrbr_NPI", "provider_id")

    df = df.withColumn(
        "provider_id",
        lpad(trim(col("provider_id").cast("string")), 10, "0")
    )

    return df


def clean_recipient_data(df):
    df = df.withColumnRenamed("Covered_Recipient_NPI", "provider_id")

    df = df.withColumn(
        "provider_id",
        lpad(trim(col("provider_id").cast("string")), 10, "0")
    )

    return df
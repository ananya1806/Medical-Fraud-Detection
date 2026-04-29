from pyspark.sql import SparkSession
import os


# -----------------------------------
# VALIDATION HELPER
# -----------------------------------
def validate_columns(df, required_cols, name):
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"{name} missing columns: {missing}")
    return df


def check_file(path):
    if not os.path.exists(path):
        raise FileNotFoundError(f"File not found: {path}")


# -----------------------------------
# 1. PRESCRIBERS
# -----------------------------------
def load_prescribers(spark):
    path = "data/raw/prescribers_2023.csv"
    check_file(path)

    df = spark.read.csv(path, header=True, inferSchema=True)

    validate_columns(df, ["PRSCRBR_NPI"], "prescribers")

    return df


# -----------------------------------
# 2. PAYMENTS
# -----------------------------------
def load_payments(spark):
    path = "data/raw/payments_2023.csv"
    check_file(path)

    df = spark.read.csv(
        path,
        header=True,
        inferSchema=True,
        multiLine=True,
        escape='"'
    )

    validate_columns(df, ["Covered_Recipient_NPI"], "payments")

    return df


# -----------------------------------
# 3. LEIE
# -----------------------------------
def load_leie(spark):
    path = "data/raw/leie.csv"
    check_file(path)

    df = spark.read.csv(path, header=True, inferSchema=True)

    # At least one must exist
    if not any(col in df.columns for col in ["NPI", "EXCLNPI"]):
        raise ValueError("LEIE missing NPI/EXCLNPI column")

    return df


# -----------------------------------
# 4. DRUG DATA
# -----------------------------------
def load_drug_data(spark):
    path = "data/raw/prescriber_drug.csv"
    check_file(path)

    df = spark.read.csv(path, header=True, inferSchema=True)

    validate_columns(df, ["Prscrbr_NPI"], "drug_data")

    return df


# -----------------------------------
# 5. RECIPIENT PROFILE
# -----------------------------------
def load_recipient_data(spark):
    path = "data/raw/recipient_profile.csv"
    check_file(path)

    df = spark.read.csv(path, header=True, inferSchema=True)

    validate_columns(df, ["Covered_Recipient_NPI"], "recipient_data")
    return df
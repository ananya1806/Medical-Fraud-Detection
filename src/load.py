import glob
import os
import re

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


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


def resolve_existing_path(*paths):
    for path in paths:
        if os.path.exists(path):
            return path
    raise FileNotFoundError(f"File not found in any of these locations: {paths}")


def resolve_matching_paths(*patterns):
    matches = []
    for pattern in patterns:
        matches.extend(sorted(glob.glob(pattern)))
    return sorted(dict.fromkeys(path for path in matches if os.path.exists(path)))


def resolve_yearly_dataset_paths(prefix, allow_legacy_current=False):
    patterns = [
        f"data/raw/{prefix}*.csv",
        f"data/{prefix}*.csv",
    ]
    matches = resolve_matching_paths(*patterns)

    exact_year_regex = re.compile(rf"^{re.escape(prefix)}_(20\d{{2}})\.csv$")
    legacy_current_regex = re.compile(rf"^{re.escape(prefix)}\.csv$")

    filtered = []
    for path in matches:
        filename = os.path.basename(path)
        if exact_year_regex.match(filename):
            filtered.append(path)
        elif allow_legacy_current and legacy_current_regex.match(filename):
            filtered.append(path)

    return sorted(dict.fromkeys(filtered))


def extract_year_from_path(path):
    match = re.search(r"(20\d{2})", os.path.basename(path))
    if match:
        return int(match.group(1))
    return None


def add_report_year(df, path, default_year=2023):
    year = extract_year_from_path(path) or default_year
    return df.withColumn("report_year", F.lit(year))


def load_yearly_csvs(spark, paths, reader_options=None, default_year=2023):
    if not paths:
        raise FileNotFoundError("No matching yearly CSV files were found.")

    reader_options = reader_options or {}
    dataframes = []
    for path in paths:
        df = spark.read.csv(path, header=True, inferSchema=True, **reader_options)
        dataframes.append(add_report_year(df, path, default_year=default_year))

    combined_df = dataframes[0]
    for df in dataframes[1:]:
        combined_df = combined_df.unionByName(df, allowMissingColumns=True)

    return combined_df


def normalize_prescriber_schema(df):
    rename_pairs = [
        ("Prscrbr_NPI", "PRSCRBR_NPI"),
        ("Prscrbr_Type_Src", "Prscrbr_Type_src"),
    ]

    for source_col, target_col in rename_pairs:
        if source_col in df.columns and target_col not in df.columns:
            df = df.withColumnRenamed(source_col, target_col)

    return df


# -----------------------------------
# 1. PRESCRIBERS
# -----------------------------------
def load_prescribers(spark):
    paths = resolve_yearly_dataset_paths("prescribers")
    if not paths:
        path = resolve_existing_path(
            "data/raw/prescribers_2023.csv",
            "data/prescribers_2023.csv"
        )
        paths = [path]

    df = normalize_prescriber_schema(load_yearly_csvs(spark, paths))

    validate_columns(df, ["PRSCRBR_NPI"], "prescribers")

    return df


# -----------------------------------
# 2. PAYMENTS
# -----------------------------------
def load_payments(spark):
    paths = resolve_yearly_dataset_paths("payments")
    if not paths:
        path = resolve_existing_path(
            "data/raw/payments_2023.csv",
            "data/payments_2023.csv"
        )
        paths = [path]

    df = load_yearly_csvs(spark, paths, reader_options={
        "multiLine": True,
        "escape": '"'
    })

    validate_columns(df, ["Covered_Recipient_NPI"], "payments")

    return df


# -----------------------------------
# 3. LEIE
# -----------------------------------
def load_leie(spark):
    path = resolve_existing_path(
        "data/raw/leie.csv",
        "data/leie.csv"
    )

    df = spark.read.csv(path, header=True, inferSchema=True)

    # At least one must exist
    if not any(col in df.columns for col in ["NPI", "EXCLNPI"]):
        raise ValueError("LEIE missing NPI/EXCLNPI column")

    return df


# -----------------------------------
# 4. DRUG DATA
# -----------------------------------
def load_drug_data(spark):
    paths = resolve_yearly_dataset_paths("prescriber_drug")
    if not paths:
        path = resolve_existing_path(
            "data/raw/prescriber_drug.csv",
            "data/prescriber_drug.csv",
            "data/prescriber_drug-002.csv"
        )
        paths = [path]

    df = load_yearly_csvs(spark, paths)

    validate_columns(df, ["Prscrbr_NPI"], "drug_data")

    return df


# -----------------------------------
# 5. RECIPIENT PROFILE
# -----------------------------------
def load_recipient_data(spark):
    paths = resolve_yearly_dataset_paths("recipient_profile", allow_legacy_current=True)
    if not paths:
        path = resolve_existing_path(
            "data/raw/recipient_profile.csv",
            "data/recipient_profile.csv"
        )
        paths = [path]

    df = load_yearly_csvs(spark, paths)

    validate_columns(df, ["Covered_Recipient_NPI"], "recipient_data")
    return df


def load_featured_data(spark):
    path = resolve_existing_path("data/processed/featured_data")
    return spark.read.parquet(path)


def has_multi_year_raw_data():
    years = set()
    for path in (
        resolve_yearly_dataset_paths("prescribers") +
        resolve_yearly_dataset_paths("payments") +
        resolve_yearly_dataset_paths("recipient_profile", allow_legacy_current=True) +
        resolve_yearly_dataset_paths("prescriber_drug")
    ):
        year = extract_year_from_path(path)
        if year is not None:
            years.add(year)
    return len(years) > 1


def load_optional_csv(spark, *paths):
    existing_path = next((path for path in paths if os.path.exists(path)), None)
    if existing_path is None:
        return None
    return spark.read.csv(existing_path, header=True, inferSchema=True)


def load_cia_labels(spark):
    return load_optional_csv(
        spark,
        "data/labels/cia.csv",
        "data/cia.csv"
    )


def load_doj_labels(spark):
    return load_optional_csv(
        spark,
        "data/labels/doj.csv",
        "data/doj.csv"
    )

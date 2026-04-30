from pyspark.sql import functions as F
from pyspark.sql.functions import coalesce, col, lit, to_date, when


def prepare_leie_exclusions(leie):
    raw_excldate = col("EXCLDATE")
    exclusion_date = coalesce(
        when(raw_excldate.rlike(r"^\d{8}$"), to_date(raw_excldate, "yyyyMMdd")),
        when(raw_excldate.rlike(r"^\d{1,2}/\d{1,2}/\d{4}$"), to_date(raw_excldate, "M/d/yyyy")),
        when(raw_excldate.rlike(r"^\d{4}-\d{2}-\d{2}$"), to_date(raw_excldate, "yyyy-MM-dd"))
    )

    return leie.select(
        "provider_id",
        exclusion_date.alias("exclusion_date")
    ).dropDuplicates(["provider_id"])


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


def add_temporal_fraud_label(df, leie, report_year=2023, horizon_years=2):
    leie_prepared = prepare_leie_exclusions(leie)

    labeled_df = df.join(leie_prepared, on="provider_id", how="left")

    if "report_year" in labeled_df.columns:
        report_year_col = col("report_year")
    else:
        report_year_col = lit(report_year)

    start_date = to_date(F.concat_ws("-", (report_year_col + lit(1)).cast("string"), lit("01"), lit("01")))
    end_date = to_date(F.concat_ws("-", (report_year_col + lit(horizon_years)).cast("string"), lit("12"), lit("31")))
    current_cutoff = to_date(F.concat_ws("-", report_year_col.cast("string"), lit("12"), lit("31")))

    labeled_df = labeled_df.withColumn(
        "excluded_on_or_before_report_year",
        when(
            col("exclusion_date").isNotNull() &
            (col("exclusion_date") <= current_cutoff),
            1
        ).otherwise(0)
    )

    labeled_df = labeled_df.withColumn(
        "is_future_fraud",
        when(
            col("exclusion_date").isNotNull() &
            (col("exclusion_date") >= start_date) &
            (col("exclusion_date") <= end_date),
            1
        ).otherwise(0)
    )

    labeled_df = labeled_df.withColumn(
        "is_fraud",
        col("is_future_fraud")
    )

    return labeled_df

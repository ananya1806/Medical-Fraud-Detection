import os

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from label import add_temporal_fraud_label


ID_COL = "provider_id"


def _parse_label_source_dates(df, date_column):
    raw_date = F.col(date_column)
    parsed_date = F.coalesce(
        F.when(raw_date.rlike(r"^\d{8}$"), F.to_date(raw_date, "yyyyMMdd")),
        F.when(raw_date.rlike(r"^\d{1,2}/\d{1,2}/\d{4}$"), F.to_date(raw_date, "M/d/yyyy")),
        F.when(raw_date.rlike(r"^\d{4}-\d{2}-\d{2}$"), F.to_date(raw_date, "yyyy-MM-dd"))
    )
    return df.withColumn(date_column, parsed_date)


def prepare_optional_label_source(df, flag_column, date_column=None):
    candidate_id_columns = ["provider_id", "NPI", "npi", "Provider_NPI", "provider_npi"]
    source_id_column = next((name for name in candidate_id_columns if name in df.columns), None)

    if source_id_column is None:
        raise ValueError(f"Optional label source for '{flag_column}' is missing an NPI/provider_id column.")

    prepared_df = df.withColumnRenamed(source_id_column, ID_COL)

    if date_column and date_column in prepared_df.columns:
        prepared_df = _parse_label_source_dates(prepared_df, date_column)
        prepared_df = prepared_df.select(ID_COL, F.col(date_column).alias(f"{flag_column}_date"))
    else:
        prepared_df = prepared_df.select(ID_COL)

    return prepared_df.dropDuplicates([ID_COL])


def add_optional_flag(df, source_df, flag_column, report_year=2023, horizon_years=2):
    if source_df is None:
        return df.withColumn(flag_column, F.lit(0))

    date_column = f"{flag_column}_date"
    joined_df = df.join(source_df, on=ID_COL, how="left")

    if date_column in source_df.columns:
        start_date = F.to_date(F.lit(f"{report_year + 1}-01-01"))
        end_date = F.to_date(F.lit(f"{report_year + horizon_years}-12-31"))
        return joined_df.withColumn(
            flag_column,
            F.when(
                F.col(date_column).isNotNull() &
                (F.col(date_column) >= start_date) &
                (F.col(date_column) <= end_date),
                1
            ).otherwise(0)
        )

    return joined_df.withColumn(
        flag_column,
        F.when(F.col(ID_COL).isNotNull() & F.col(flag_column).isNull(), F.lit(0)).otherwise(F.lit(0))
    ).withColumn(
        flag_column,
        F.when(F.col(date_column).isNotNull() if date_column in joined_df.columns else F.col(ID_COL).isNotNull(), 1).otherwise(0)
    )


def add_binary_optional_flag(df, source_df, flag_column):
    if source_df is None:
        return df.withColumn(flag_column, F.lit(0))

    source_flag_df = source_df.select(ID_COL).withColumn(flag_column, F.lit(1)).dropDuplicates([ID_COL])
    return (
        df.join(source_flag_df, on=ID_COL, how="left")
        .withColumn(flag_column, F.coalesce(F.col(flag_column), F.lit(0)))
    )


def add_behavioral_risk_flags(df):
    flagged_df = df
    numeric_features = [
        "Opioid_Prscrbr_Rate",
        "Opioid_LA_Prscrbr_Rate",
        "payment_per_claim",
        "avg_cost_per_claim",
        "claims_per_beneficiary",
        "total_payments",
    ]

    for feature in numeric_features:
        if feature in flagged_df.columns:
            flagged_df = flagged_df.withColumn(
                feature,
                F.when(F.isnan(F.col(feature)) | F.col(feature).isNull(), F.lit(0.0))
                .otherwise(F.col(feature).cast("double"))
            )

    peer_window = Window.partitionBy("Prscrbr_Type", "Prscrbr_State_Abrvtn").orderBy(F.col("Opioid_Prscrbr_Rate"))
    payment_window = Window.partitionBy("Prscrbr_Type", "Prscrbr_State_Abrvtn").orderBy(F.col("payment_per_claim"))

    flagged_df = flagged_df.withColumn(
        "opioid_peer_percentile",
        F.percent_rank().over(peer_window)
    )
    flagged_df = flagged_df.withColumn(
        "payment_peer_percentile",
        F.percent_rank().over(payment_window)
    )

    flagged_df = flagged_df.withColumn(
        "opioid_outlier_flag",
        F.when(
            (F.col("Opioid_Prscrbr_Rate") > 0) &
            (F.col("opioid_peer_percentile") >= 0.99),
            1
        ).otherwise(0)
    )

    flagged_df = flagged_df.withColumn(
        "payment_outlier_flag",
        F.when(
            (F.col("payment_per_claim") > 0) &
            (F.col("payment_peer_percentile") >= 0.99),
            1
        ).otherwise(0)
    )

    flagged_df = flagged_df.withColumn(
        "behavioral_risk_flag",
        F.when(
            (F.col("opioid_outlier_flag") == 1) |
            (F.col("payment_outlier_flag") == 1) |
            (F.col("high_payment_flag") == 1) |
            (F.col("high_cost_flag") == 1),
            1
        ).otherwise(0)
    )

    return flagged_df


def add_multi_signal_labels(
    df,
    leie_df,
    cia_df=None,
    doj_df=None,
    report_year=2023,
    horizon_years=2
):
    labeled_df = add_temporal_fraud_label(
        df,
        leie_df,
        report_year=report_year,
        horizon_years=horizon_years
    )

    labeled_df = add_binary_optional_flag(labeled_df, cia_df, "cia_flag")
    labeled_df = add_binary_optional_flag(labeled_df, doj_df, "doj_flag")
    labeled_df = add_behavioral_risk_flags(labeled_df)

    labeled_df = labeled_df.withColumn(
        "high_confidence_fraud_flag",
        F.when(F.col("is_future_fraud") == 1, 1).otherwise(0)
    )
    labeled_df = labeled_df.withColumn(
        "enforcement_risk_flag",
        F.when((F.col("cia_flag") == 1) | (F.col("doj_flag") == 1), 1).otherwise(0)
    )
    labeled_df = labeled_df.withColumn(
        "multi_signal_label_strength",
        F.col("high_confidence_fraud_flag") +
        F.col("enforcement_risk_flag") +
        F.col("behavioral_risk_flag")
    )

    labeled_df = labeled_df.withColumn(
        "is_fraud",
        F.when(F.col("high_confidence_fraud_flag") == 1, 1).otherwise(0)
    )

    return labeled_df

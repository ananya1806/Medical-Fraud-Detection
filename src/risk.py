from pyspark.sql import functions as F
from functools import reduce


ID_COL = "provider_id"
LABEL_COL = "is_fraud"
PEER_TYPE_COL = "Prscrbr_Type"
PEER_STATE_COL = "Prscrbr_State_Abrvtn"
PEER_GROUP_COL = "peer_group"
MIN_PEER_GROUP_SIZE = 25

ANOMALY_FEATURES = [
    "total_payments",
    "payment_per_claim",
    "avg_cost_per_claim",
    "claims_per_beneficiary",
    "Opioid_Prscrbr_Rate",
    "Opioid_LA_Prscrbr_Rate",
    "total_drug_claims",
    "recipient_interactions",
    "Bene_Avg_Risk_Scre",
]


def _cap_score(column_name):
    return F.least(F.greatest(F.col(column_name), F.lit(0.0)), F.lit(1.0))


def _sanitize_numeric(df, column_name):
    sanitized = F.col(column_name).cast("double")
    return df.withColumn(
        column_name,
        F.when(F.isnan(sanitized) | sanitized.isNull(), F.lit(0.0))
        .when(sanitized == float("inf"), F.lit(0.0))
        .when(sanitized == float("-inf"), F.lit(0.0))
        .otherwise(sanitized)
    )


def build_anomaly_scores(df):
    available_features = [feature for feature in ANOMALY_FEATURES if feature in df.columns]

    if not available_features:
        raise ValueError("No anomaly-scoring features were found in the dataframe.")

    scored_df = df
    for feature in available_features:
        scored_df = _sanitize_numeric(scored_df, feature)

    scored_df = scored_df.withColumn(
        PEER_GROUP_COL,
        F.concat_ws(
            "::",
            F.coalesce(F.col(PEER_TYPE_COL), F.lit("UNKNOWN_TYPE")),
            F.coalesce(F.col(PEER_STATE_COL), F.lit("UNKNOWN_STATE"))
        )
    )

    peer_stats_exprs = [F.count("*").alias("peer_group_count")]
    global_stats_exprs = []
    for feature in available_features:
        peer_stats_exprs.extend([
            F.avg(feature).alias(f"{feature}_peer_mean"),
            F.stddev_pop(feature).alias(f"{feature}_peer_std"),
        ])
        global_stats_exprs.extend([
            F.avg(feature).alias(f"{feature}_global_mean"),
            F.stddev_pop(feature).alias(f"{feature}_global_std"),
        ])

    peer_stats = scored_df.groupBy(PEER_GROUP_COL).agg(*peer_stats_exprs)
    global_stats = scored_df.agg(*global_stats_exprs)

    scored_df = scored_df.join(peer_stats, on=PEER_GROUP_COL, how="left")
    scored_df = scored_df.crossJoin(global_stats)

    anomaly_components = []
    for feature in available_features:
        peer_mean_col = F.coalesce(F.col(f"{feature}_peer_mean"), F.lit(0.0))
        peer_std_col = F.when(
            F.col(f"{feature}_peer_std").isNull() | (F.col(f"{feature}_peer_std") == 0),
            F.lit(1.0)
        ).otherwise(F.col(f"{feature}_peer_std"))

        global_mean_col = F.coalesce(F.col(f"{feature}_global_mean"), F.lit(0.0))
        global_std_col = F.when(
            F.col(f"{feature}_global_std").isNull() | (F.col(f"{feature}_global_std") == 0),
            F.lit(1.0)
        ).otherwise(F.col(f"{feature}_global_std"))

        peer_score_col = f"{feature}_peer_score"
        global_score_col = f"{feature}_global_score"
        anomaly_component_col = f"{feature}_anomaly_component"

        scored_df = scored_df.withColumn(
            peer_score_col,
            F.when(
                F.col("peer_group_count") >= MIN_PEER_GROUP_SIZE,
                F.greatest((F.col(feature) - peer_mean_col) / (peer_std_col * 4.0), F.lit(0.0))
            ).otherwise(F.lit(0.0))
        )
        scored_df = scored_df.withColumn(
            global_score_col,
            F.greatest((F.col(feature) - global_mean_col) / (global_std_col * 4.0), F.lit(0.0))
        )
        scored_df = scored_df.withColumn(
            anomaly_component_col,
            F.greatest(_cap_score(peer_score_col), _cap_score(global_score_col))
        )
        anomaly_components.append(F.col(anomaly_component_col))

    anomaly_sum = reduce(lambda left, right: left + right, anomaly_components)
    scored_df = scored_df.withColumn(
        "anomaly_score",
        anomaly_sum / F.lit(float(len(anomaly_components)))
    )

    scored_df = scored_df.withColumn(
        "anomaly_flag",
        F.when(F.col("anomaly_score") >= 0.65, F.lit(1)).otherwise(F.lit(0))
    )

    return scored_df


def combine_hybrid_scores(df, supervised_col="supervised_score", anomaly_col="anomaly_score"):
    return (
        df.withColumn(supervised_col, _cap_score(supervised_col))
        .withColumn(anomaly_col, _cap_score(anomaly_col))
        .withColumn(
            "hybrid_risk_score",
            (F.col(supervised_col) * F.lit(0.6)) + (F.col(anomaly_col) * F.lit(0.4))
        )
        .withColumn(
            "hybrid_risk_band",
            F.when(F.col("hybrid_risk_score") >= 0.9, F.lit("critical"))
            .when(F.col("hybrid_risk_score") >= 0.75, F.lit("high"))
            .when(F.col("hybrid_risk_score") >= 0.55, F.lit("medium"))
            .otherwise(F.lit("low"))
        )
    )


def get_top_risk_providers(df, limit=25):
    selected_columns = [ID_COL, "hybrid_risk_score", "hybrid_risk_band", "anomaly_score"]
    if LABEL_COL in df.columns:
        selected_columns.append(LABEL_COL)
    if "supervised_score" in df.columns:
        selected_columns.append("supervised_score")
    if PEER_TYPE_COL in df.columns:
        selected_columns.append(PEER_TYPE_COL)
    if PEER_STATE_COL in df.columns:
        selected_columns.append(PEER_STATE_COL)

    return (
        df.select(*selected_columns)
        .orderBy(F.col("hybrid_risk_score").desc(), F.col("anomaly_score").desc())
        .limit(limit)
    )


def build_compact_risk_table(df):
    compact_columns = [
        ID_COL,
        "hybrid_risk_score",
        "hybrid_risk_band",
        "supervised_score",
        "anomaly_score",
        "is_fraud",
        "high_confidence_fraud_flag",
        "behavioral_risk_flag",
        "opioid_outlier_flag",
        "payment_outlier_flag",
        "Prscrbr_Type",
        "Prscrbr_State_Abrvtn",
        "total_payments",
        "payment_per_claim",
        "avg_cost_per_claim",
        "claims_per_beneficiary",
        "Opioid_Prscrbr_Rate",
        "recipient_interactions",
        "unique_drugs",
    ]

    existing_columns = [name for name in compact_columns if name in df.columns]
    return df.select(*existing_columns)

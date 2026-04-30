import json
import os
from pathlib import Path
from time import perf_counter

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from pyspark.storagelevel import StorageLevel

from load import (
    load_prescribers, load_payments, load_leie,
    load_drug_data, load_recipient_data, load_featured_data,
    load_cia_labels, load_doj_labels, has_multi_year_raw_data
)

from clean import (
    clean_prescribers, clean_payments, clean_leie,
    clean_drug_data, clean_recipient_data
)

from join import (
    aggregate_payments, aggregate_drug_data,
    aggregate_recipient_data, join_data
)

from features import build_features
from risk import (
    build_anomaly_scores,
    build_compact_risk_table,
    combine_hybrid_scores,
    get_top_risk_providers,
)
from signals import add_multi_signal_labels
from train import train_and_evaluate


SAMPLE_FRACTION = 1.0
FEATURED_DATA_PATH = "data/processed/featured_data"
HYBRID_RISK_OUTPUT_PATH = "data/processed/hybrid_risk_scores"
COMPACT_RISK_OUTPUT_PATH = "data/processed/compact_risk_scores"
TOP_RISK_OUTPUT_PATH = "data/processed/top_risk_providers"
MODEL_OUTPUT_PATH = "data/artifacts/best_model"
METRICS_OUTPUT_PATH = "data/artifacts/model_metrics.json"
TIMINGS_OUTPUT_PATH = "data/artifacts/stage_timings.json"
REPORT_YEAR = 2023
FRAUD_HORIZON_YEARS = 2


def print_model_metrics(results):
    print("\nSelected model:", results["best_model_name"])
    print("Training rows:", results["train_rows"])
    print("Balanced training rows:", results["balanced_train_rows"])
    print("Test rows:", results["test_rows"])
    print("Feature columns:", ", ".join(results["feature_columns"]))
    print(
        "Training rebalance:"
        f" positives={results['balance_summary']['positive_count']},"
        f" negatives={results['balance_summary']['negative_count']},"
        f" negative_sampling_fraction={results['balance_summary']['sampling_fraction']:.4f}"
    )

    print("\nModel comparison:")
    for model_name, metrics in results["metrics_by_model"].items():
        print(
            f"- {model_name}: "
            f"AUC-PR={metrics['auc_pr']:.4f}, "
            f"AUC-ROC={metrics['auc_roc']:.4f}, "
            f"Precision={metrics['precision']:.4f}, "
            f"Recall={metrics['recall']:.4f}, "
            f"F1={metrics['f1']:.4f}, "
            f"Accuracy={metrics['accuracy']:.4f}"
        )


def print_hybrid_summary(hybrid_df):
    print("\nHybrid risk distribution:")
    hybrid_df.groupBy("hybrid_risk_band").count().orderBy("hybrid_risk_band").show()

    print("Top risk providers:")
    get_top_risk_providers(hybrid_df).show(25, truncate=False)


def ensure_parent_dir(path):
    Path(path).parent.mkdir(parents=True, exist_ok=True)


def save_metrics_artifact(results, output_path):
    ensure_parent_dir(output_path)

    payload = {
        "best_model_name": results["best_model_name"],
        "train_rows": results["train_rows"],
        "balanced_train_rows": results["balanced_train_rows"],
        "test_rows": results["test_rows"],
        "feature_columns": results["feature_columns"],
        "balance_summary": results["balance_summary"],
        "metrics_by_model": results["metrics_by_model"],
    }

    with open(output_path, "w", encoding="utf-8") as metrics_file:
        json.dump(payload, metrics_file, indent=2, sort_keys=True)


def save_json_artifact(payload, output_path):
    ensure_parent_dir(output_path)
    with open(output_path, "w", encoding="utf-8") as artifact_file:
        json.dump(payload, artifact_file, indent=2, sort_keys=True)


def save_model_artifact(results, output_path):
    ensure_parent_dir(output_path)
    results["best_model"].write().overwrite().save(output_path)


def save_risk_artifacts(hybrid_df):
    hybrid_df.write.mode("overwrite").parquet(HYBRID_RISK_OUTPUT_PATH)
    persisted_hybrid_df = hybrid_df.sparkSession.read.parquet(HYBRID_RISK_OUTPUT_PATH)

    print_hybrid_summary(persisted_hybrid_df)

    compact_risk_df = build_compact_risk_table(persisted_hybrid_df).coalesce(1)
    compact_risk_df.write.mode("overwrite").parquet(COMPACT_RISK_OUTPUT_PATH)

    top_risk_df = get_top_risk_providers(persisted_hybrid_df, limit=100).coalesce(1)
    top_risk_df.write.mode("overwrite").option("header", True).csv(TOP_RISK_OUTPUT_PATH)


def print_label_summary(final_df):
    print("\nFraud Distribution:")
    final_df.groupBy("is_fraud").count().show()

    if "excluded_on_or_before_report_year" in final_df.columns:
        print("Prior exclusions removed from prediction cohort:")
        final_df.groupBy("excluded_on_or_before_report_year").count().show()

    signal_columns = [
        "high_confidence_fraud_flag",
        "cia_flag",
        "doj_flag",
        "opioid_outlier_flag",
        "payment_outlier_flag",
        "behavioral_risk_flag",
        "enforcement_risk_flag",
    ]

    existing_signal_columns = [name for name in signal_columns if name in final_df.columns]
    if existing_signal_columns:
        print("Signal coverage:")
        final_df.select(
            *[F.sum(col(name)).alias(name) for name in existing_signal_columns]
        ).show(truncate=False)


def build_featured_dataset(spark):
    prescribers = load_prescribers(spark)
    payments = load_payments(spark)
    drug = load_drug_data(spark)
    recipient = load_recipient_data(spark)

    prescribers = clean_prescribers(prescribers)
    payments = clean_payments(payments)
    drug = clean_drug_data(drug)
    recipient = clean_recipient_data(recipient)

    payments_agg = aggregate_payments(payments)
    drug_agg = aggregate_drug_data(drug)
    recipient_agg = aggregate_recipient_data(recipient)

    joined_df = join_data(
        prescribers,
        payments_agg,
        drug_agg,
        recipient_agg
    )

    if SAMPLE_FRACTION < 1.0:
        joined_df = joined_df.sample(
            withReplacement=False,
            fraction=SAMPLE_FRACTION,
            seed=42
        )

    return build_features(joined_df)


def main():
    stage_timings = {}
    overall_start = perf_counter()
    spark = (
        SparkSession.builder
        .appName("FraudDetection")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.default.parallelism", "8")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )

    load_start = perf_counter()
    leie = load_leie(spark)
    leie = clean_leie(leie)
    cia_labels = load_cia_labels(spark)
    doj_labels = load_doj_labels(spark)
    stage_timings["load_label_sources_seconds"] = round(perf_counter() - load_start, 3)

    feature_start = perf_counter()
    use_processed_featured = os.path.exists(FEATURED_DATA_PATH) and not has_multi_year_raw_data()
    if use_processed_featured:
        print(f"Loading processed featured data from {FEATURED_DATA_PATH}")
        featured_df = load_featured_data(spark)
    else:
        print("Building featured dataset from raw data.")
        featured_df = build_featured_dataset(spark)
        featured_df = featured_df.persist(StorageLevel.DISK_ONLY)

    featured_rows = featured_df.count()

    if not use_processed_featured:
        print(f"Saving processed featured data to {FEATURED_DATA_PATH}")
        featured_df.write.mode("overwrite").parquet(FEATURED_DATA_PATH)
        featured_df.unpersist()
        featured_df = load_featured_data(spark)

    stage_timings["feature_dataset_seconds"] = round(perf_counter() - feature_start, 3)
    stage_timings["feature_dataset_rows"] = featured_rows

    print("Feature dataset ready.")

    # LABEL
    label_start = perf_counter()
    final_df = add_multi_signal_labels(
        featured_df,
        leie,
        cia_df=cia_labels,
        doj_df=doj_labels,
        report_year=REPORT_YEAR,
        horizon_years=FRAUD_HORIZON_YEARS
    )
    final_df = final_df.filter(col("excluded_on_or_before_report_year") == 0)
    final_rows = final_df.count()
    stage_timings["labeling_seconds"] = round(perf_counter() - label_start, 3)
    stage_timings["labeled_rows"] = final_rows

    print_label_summary(final_df)

    train_start = perf_counter()
    modeling_results = train_and_evaluate(final_df)
    stage_timings["model_training_seconds"] = round(perf_counter() - train_start, 3)
    print_model_metrics(modeling_results)
    save_model_artifact(modeling_results, MODEL_OUTPUT_PATH)
    save_metrics_artifact(modeling_results, METRICS_OUTPUT_PATH)

    risk_start = perf_counter()
    anomaly_df = build_anomaly_scores(modeling_results["full_scored_df"])
    hybrid_df = combine_hybrid_scores(anomaly_df)
    save_risk_artifacts(hybrid_df)
    stage_timings["risk_scoring_seconds"] = round(perf_counter() - risk_start, 3)

    stage_timings["total_pipeline_seconds"] = round(perf_counter() - overall_start, 3)
    stage_timings["used_processed_featured_data"] = use_processed_featured
    save_json_artifact(stage_timings, TIMINGS_OUTPUT_PATH)

    print(f"Saved best model to {MODEL_OUTPUT_PATH}")
    print(f"Saved model metrics to {METRICS_OUTPUT_PATH}")
    print(f"Saved stage timings to {TIMINGS_OUTPUT_PATH}")
    print(f"Saved hybrid risk scores to {HYBRID_RISK_OUTPUT_PATH}")
    print(f"Saved compact risk scores to {COMPACT_RISK_OUTPUT_PATH}")
    print(f"Saved top risk providers to {TOP_RISK_OUTPUT_PATH}")

    print("\nPipeline completed successfully")

    spark.stop()


if __name__ == "__main__":
    main()

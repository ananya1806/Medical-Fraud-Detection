from pyspark.sql import SparkSession

from load import (
    load_prescribers, load_payments, load_leie,
    load_drug_data, load_recipient_data
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
from label import add_fraud_label


def main():
    spark = SparkSession.builder.appName("FraudDetection").getOrCreate()

    # LOAD
    prescribers = load_prescribers(spark)
    payments = load_payments(spark)
    leie = load_leie(spark)
    drug = load_drug_data(spark)
    recipient = load_recipient_data(spark)

    # CLEAN
    prescribers = clean_prescribers(prescribers)
    payments = clean_payments(payments)
    leie = clean_leie(leie)
    drug = clean_drug_data(drug)
    recipient = clean_recipient_data(recipient)

    # AGGREGATE
    payments_agg = aggregate_payments(payments)
    drug_agg = aggregate_drug_data(drug)
    recipient_agg = aggregate_recipient_data(recipient)

    # JOIN
    joined_df = join_data(
        prescribers,
        payments_agg,
        drug_agg,
        recipient_agg
    )

    # 🔥 OPTIONAL (prevents memory crash)
    joined_df = joined_df.sample(0.2)

    # FEATURES
    featured_df = build_features(joined_df)

    # SAFE PREVIEW
    print("Feature preview:")
    featured_df.select(
        "provider_id",
        "total_payments",
        "unique_drugs",
        "recipient_interactions"
    ).limit(10).show()

    # LABEL
    final_df = add_fraud_label(featured_df, leie)

    print("Fraud Distribution:")
    final_df.groupBy("is_fraud").count().show()

    print("Pipeline completed successfully")

    spark.stop()


if __name__ == "__main__":
    main()
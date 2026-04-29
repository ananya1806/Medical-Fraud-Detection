from pyspark.sql.functions import sum, countDistinct, count


def aggregate_payments(df):
    return df.groupBy("provider_id").agg(
        sum("payment_amount").alias("total_payments")
    )


def aggregate_drug_data(df):
    return df.groupBy("provider_id").agg(
        countDistinct("Brnd_Name").alias("unique_drugs"),
        sum("Tot_Clms").alias("total_drug_claims")
    )


def aggregate_recipient_data(df):
    return df.groupBy("provider_id").agg(
        count("*").alias("recipient_interactions")
    )


def join_data(prescribers, payments_agg, drug_agg=None, recipient_agg=None):

    df = prescribers.join(payments_agg, "provider_id", "left")

    if drug_agg is not None:
        df = df.join(drug_agg, "provider_id", "left")

    if recipient_agg is not None:
        df = df.join(recipient_agg, "provider_id", "left")

    df = df.fillna({
        "total_payments": 0,
        "unique_drugs": 0,
        "total_drug_claims": 0,
        "recipient_interactions": 0
    })

    return df
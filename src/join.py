from pyspark.sql.functions import sum, countDistinct, count


def _group_keys(df):
    return ["provider_id", "report_year"] if "report_year" in df.columns else ["provider_id"]


def _join_keys(df):
    return ["provider_id", "report_year"] if "report_year" in df.columns else ["provider_id"]


def aggregate_payments(df):
    return df.groupBy(*_group_keys(df)).agg(
        sum("payment_amount").alias("total_payments")
    )


def aggregate_drug_data(df):
    return df.groupBy(*_group_keys(df)).agg(
        countDistinct("Brnd_Name").alias("unique_drugs"),
        sum("Tot_Clms").alias("total_drug_claims")
    )


def aggregate_recipient_data(df):
    return df.groupBy(*_group_keys(df)).agg(
        count("*").alias("recipient_interactions")
    )


def join_data(prescribers, payments_agg, drug_agg=None, recipient_agg=None):
    join_keys = _join_keys(prescribers)

    df = prescribers.join(payments_agg, join_keys, "left")

    if drug_agg is not None:
        df = df.join(drug_agg, join_keys, "left")

    if recipient_agg is not None:
        df = df.join(recipient_agg, join_keys, "left")

    df = df.fillna({
        "total_payments": 0,
        "unique_drugs": 0,
        "total_drug_claims": 0,
        "recipient_interactions": 0
    })

    return df

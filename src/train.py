from pyspark.ml import Pipeline
from pyspark.ml.classification import (
    GBTClassifier,
    LogisticRegression,
    RandomForestClassifier,
)
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, NumericType


LABEL_COL = "is_fraud"
ID_COL = "provider_id"
FEATURES_COL = "features"
SCALED_FEATURES_COL = "scaled_features"
WEIGHT_COL = "class_weight"
PREDICTION_COL = "prediction"
PROBABILITY_COL = "probability"
RAW_PREDICTION_COL = "rawPrediction"
DEFAULT_POSITIVE_RATIO = 0.25
EXCLUDED_FEATURE_COLUMNS = {
    LABEL_COL,
    ID_COL,
    "is_future_fraud",
    "exclusion_date",
    "excluded_on_or_before_report_year",
    "high_confidence_fraud_flag",
    "cia_flag",
    "doj_flag",
    "enforcement_risk_flag",
    "behavioral_risk_flag",
    "multi_signal_label_strength",
    "opioid_outlier_flag",
    "payment_outlier_flag",
    "opioid_peer_percentile",
    "payment_peer_percentile",
    "anomaly_flag",
    "anomaly_score",
    "supervised_score",
    "hybrid_risk_score",
}


def get_numeric_feature_columns(df, label_col=LABEL_COL, id_col=ID_COL):
    feature_columns = []

    for field in df.schema.fields:
        if field.name in EXCLUDED_FEATURE_COLUMNS or field.name in {label_col, id_col}:
            continue

        if isinstance(field.dataType, NumericType) or isinstance(field.dataType, BooleanType):
            feature_columns.append(field.name)

    if not feature_columns:
        raise ValueError("No numeric feature columns available for model training.")

    return sorted(feature_columns)


def prepare_modeling_data(df, label_col=LABEL_COL):
    if label_col not in df.columns:
        raise ValueError(f"Label column '{label_col}' not found in dataframe.")

    feature_columns = get_numeric_feature_columns(df, label_col=label_col)

    selected_columns = [ID_COL]
    for feature_col in feature_columns:
        sanitized_col = F.col(feature_col).cast("double")
        sanitized_col = (
            F.when(F.isnan(sanitized_col) | sanitized_col.isNull(), F.lit(0.0))
            .when(sanitized_col == float("inf"), F.lit(0.0))
            .when(sanitized_col == float("-inf"), F.lit(0.0))
            .otherwise(sanitized_col)
            .alias(feature_col)
        )
        selected_columns.append(sanitized_col)

    modeling_df = df.select(
        *selected_columns,
        F.col(label_col).cast("double").alias(label_col)
    ).filter(F.col(ID_COL).isNotNull())

    class_counts = {
        row[label_col]: row["count"]
        for row in modeling_df.groupBy(label_col).count().collect()
    }

    positive_count = int(class_counts.get(1.0, 0))
    negative_count = int(class_counts.get(0.0, 0))

    if positive_count == 0 or negative_count == 0:
        raise ValueError(
            "Training data must contain both fraud and non-fraud examples."
        )

    positive_weight = negative_count / positive_count

    modeling_df = modeling_df.withColumn(
        WEIGHT_COL,
        F.when(F.col(label_col) == 1.0, F.lit(float(positive_weight))).otherwise(F.lit(1.0))
    )

    return modeling_df, feature_columns


def split_train_test(df, label_col=LABEL_COL, train_ratio=0.8, seed=42, max_attempts=5):
    for attempt in range(max_attempts):
        current_seed = seed + attempt
        train_df, test_df = df.randomSplit([train_ratio, 1 - train_ratio], seed=current_seed)

        train_counts = {
            row[label_col]: row["count"]
            for row in train_df.groupBy(label_col).count().collect()
        }
        test_counts = {
            row[label_col]: row["count"]
            for row in test_df.groupBy(label_col).count().collect()
        }

        if (
            train_counts.get(0.0, 0) > 0
            and train_counts.get(1.0, 0) > 0
            and test_counts.get(0.0, 0) > 0
            and test_counts.get(1.0, 0) > 0
        ):
            return train_df, test_df

    raise ValueError(
        "Unable to create a train/test split containing both classes in each partition."
    )


def rebalance_training_data(
    df,
    label_col=LABEL_COL,
    target_positive_ratio=DEFAULT_POSITIVE_RATIO,
    seed=42
):
    positive_df = df.filter(F.col(label_col) == 1.0)
    negative_df = df.filter(F.col(label_col) == 0.0)

    positive_count = positive_df.count()
    negative_count = negative_df.count()

    if positive_count == 0 or negative_count == 0:
        return df, {
            "positive_count": positive_count,
            "negative_count": negative_count,
            "sampling_fraction": 1.0,
            "balanced_rows": df.count(),
        }

    desired_negative_count = int(positive_count * ((1 - target_positive_ratio) / target_positive_ratio))
    desired_negative_count = max(desired_negative_count, positive_count)

    sampling_fraction = min(1.0, desired_negative_count / negative_count)
    sampled_negative_df = negative_df.sample(
        withReplacement=False,
        fraction=sampling_fraction,
        seed=seed
    )

    balanced_df = positive_df.unionByName(sampled_negative_df)

    return balanced_df, {
        "positive_count": positive_count,
        "negative_count": negative_count,
        "sampling_fraction": sampling_fraction,
        "balanced_rows": balanced_df.count(),
    }


def compute_confusion_metrics(predictions, label_col=LABEL_COL, prediction_col=PREDICTION_COL):
    metrics_row = predictions.agg(
        F.sum(
            F.when((F.col(label_col) == 1.0) & (F.col(prediction_col) == 1.0), 1).otherwise(0)
        ).alias("tp"),
        F.sum(
            F.when((F.col(label_col) == 0.0) & (F.col(prediction_col) == 0.0), 1).otherwise(0)
        ).alias("tn"),
        F.sum(
            F.when((F.col(label_col) == 0.0) & (F.col(prediction_col) == 1.0), 1).otherwise(0)
        ).alias("fp"),
        F.sum(
            F.when((F.col(label_col) == 1.0) & (F.col(prediction_col) == 0.0), 1).otherwise(0)
        ).alias("fn")
    ).collect()[0]

    tp = int(metrics_row["tp"] or 0)
    tn = int(metrics_row["tn"] or 0)
    fp = int(metrics_row["fp"] or 0)
    fn = int(metrics_row["fn"] or 0)

    total = tp + tn + fp + fn
    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / (tp + fn) if (tp + fn) else 0.0
    accuracy = (tp + tn) / total if total else 0.0
    f1 = (
        2 * precision * recall / (precision + recall)
        if (precision + recall)
        else 0.0
    )

    return {
        "tp": tp,
        "tn": tn,
        "fp": fp,
        "fn": fn,
        "accuracy": accuracy,
        "precision": precision,
        "recall": recall,
        "f1": f1,
    }


def evaluate_predictions(predictions, label_col=LABEL_COL):
    scored_df = predictions.withColumn(
        "positive_probability",
        vector_to_array(F.col(PROBABILITY_COL))[1]
    )

    roc_evaluator = BinaryClassificationEvaluator(
        labelCol=label_col,
        rawPredictionCol=RAW_PREDICTION_COL,
        metricName="areaUnderROC"
    )
    pr_evaluator = BinaryClassificationEvaluator(
        labelCol=label_col,
        rawPredictionCol=RAW_PREDICTION_COL,
        metricName="areaUnderPR"
    )

    metrics = compute_confusion_metrics(scored_df, label_col=label_col)
    metrics["auc_roc"] = roc_evaluator.evaluate(scored_df)
    metrics["auc_pr"] = pr_evaluator.evaluate(scored_df)

    return metrics


def add_supervised_scores(predictions):
    return predictions.withColumn(
        "supervised_score",
        vector_to_array(F.col(PROBABILITY_COL))[1]
    )


def train_candidate_models(train_df, feature_columns, label_col=LABEL_COL):
    assembler = VectorAssembler(
        inputCols=feature_columns,
        outputCol=FEATURES_COL,
        handleInvalid="keep"
    )

    scaler = StandardScaler(
        inputCol=FEATURES_COL,
        outputCol=SCALED_FEATURES_COL,
        withStd=True,
        withMean=False
    )

    logistic_regression = LogisticRegression(
        labelCol=label_col,
        featuresCol=SCALED_FEATURES_COL,
        weightCol=WEIGHT_COL,
        maxIter=100,
        regParam=0.01,
        elasticNetParam=0.0
    )

    random_forest = RandomForestClassifier(
        labelCol=label_col,
        featuresCol=FEATURES_COL,
        weightCol=WEIGHT_COL,
        numTrees=100,
        maxDepth=8,
        seed=42
    )

    gbt_classifier = GBTClassifier(
        labelCol=label_col,
        featuresCol=FEATURES_COL,
        maxIter=80,
        maxDepth=6,
        maxBins=64,
        stepSize=0.1,
        seed=42
    )

    candidates = {
        "logistic_regression": Pipeline(stages=[assembler, scaler, logistic_regression]),
        "random_forest": Pipeline(stages=[assembler, random_forest]),
        "gbt_classifier": Pipeline(stages=[assembler, gbt_classifier]),
    }

    return {
        name: pipeline.fit(train_df)
        for name, pipeline in candidates.items()
    }


def select_best_model(models, test_df, label_col=LABEL_COL):
    evaluation_results = {}
    best_model_name = None
    best_model = None
    best_predictions = None
    best_score = float("-inf")

    for model_name, model in models.items():
        predictions = model.transform(test_df)
        metrics = evaluate_predictions(predictions, label_col=label_col)
        evaluation_results[model_name] = metrics

        score = metrics["auc_pr"]
        if score > best_score:
            best_score = score
            best_model_name = model_name
            best_model = model
            best_predictions = predictions

    return best_model_name, best_model, best_predictions, evaluation_results


def train_and_evaluate(df, label_col=LABEL_COL):
    modeling_df, feature_columns = prepare_modeling_data(df, label_col=label_col)
    train_df, test_df = split_train_test(modeling_df, label_col=label_col)
    balanced_train_df, balance_summary = rebalance_training_data(train_df, label_col=label_col)

    models = train_candidate_models(balanced_train_df, feature_columns, label_col=label_col)
    best_model_name, best_model, best_predictions, evaluation_results = select_best_model(
        models,
        test_df,
        label_col=label_col
    )
    full_supervised_scores = (
        add_supervised_scores(best_model.transform(modeling_df))
        .select(ID_COL, "supervised_score")
    )
    full_scored_df = df.join(full_supervised_scores, on=ID_COL, how="inner")

    return {
        "feature_columns": feature_columns,
        "train_rows": train_df.count(),
        "balanced_train_rows": balance_summary["balanced_rows"],
        "test_rows": test_df.count(),
        "balance_summary": balance_summary,
        "best_model_name": best_model_name,
        "best_model": best_model,
        "best_predictions": best_predictions,
        "full_scored_df": full_scored_df,
        "metrics_by_model": evaluation_results,
    }

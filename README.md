# Medical-Fraud-Detection

PySpark pipeline for provider-level medical fraud detection using public healthcare payment, prescribing, recipient, and exclusion datasets.

## Current pipeline

1. Load raw source files from `data/raw/`
2. Clean and standardize provider identifiers
3. Aggregate payment, drug, and recipient activity at the provider level
4. Engineer fraud-related numeric features
5. Label providers using the LEIE exclusion list
6. Train and evaluate Spark ML classification models

## ML stage

The project now includes a modeling pipeline in [src/train.py] that:

- Selects numeric features automatically
- Applies class weighting to address fraud class imbalance
- Splits data into train and test sets
- Trains both Logistic Regression and Random Forest models
- Compares models using AUC-PR, AUC-ROC, precision, recall, F1, and accuracy
- Selects the best model using AUC-PR, which is better suited for imbalanced fraud detection

Run the full workflow from [src/main.py](/Users/dishashanbhag/Documents/Medical-Fraud-Detection/src/main.py).

## Recommended data expansion

The next strongest upgrade is to move from a static exclusion match to a temporal fraud-risk target:

- Use `EXCLDATE` from LEIE to label providers who are excluded after the reporting year
- Add more CMS years so the model can learn trend and spike behavior over time
- Keep current-year excluded providers out of the prediction cohort to avoid label leakage

The current code now supports this temporal-label direction for the 2023 reporting year.

## Multi-signal labels

The pipeline now supports multiple fraud and risk signals:

- `high_confidence_fraud_flag`: future LEIE exclusion signal
- `cia_flag`: optional OIG CIA/entity label file
- `doj_flag`: optional DOJ/entity label file
- `opioid_outlier_flag`: peer-group opioid outlier
- `payment_outlier_flag`: peer-group payment outlier
- `behavioral_risk_flag`: combined behavioral heuristic

Optional external label files can be added at:

- `data/labels/cia.csv`
- `data/labels/doj.csv`

Each optional file should include an NPI/provider identifier column such as `provider_id` or `NPI`.

## Multi-year input support

The raw-data loaders now support multiple yearly files when they follow names like:

- `data/prescribers_2022.csv`, `data/prescribers_2023.csv`
- `data/payments_2022.csv`, `data/payments_2023.csv`
- `data/prescriber_drug_2022.csv`, `data/prescriber_drug_2023.csv`
- `data/recipient_profile_2022.csv`, `data/recipient_profile_2023.csv`

If multiple years are present, the pipeline preserves `report_year` and rebuilds features from raw data instead of reusing the single-year processed parquet snapshot.

## Runtime artifact

Each run now saves stage timings to:

- `data/artifacts/stage_timings.json`

This can be used in the presentation to compare ingestion, feature generation, labeling, model training, and risk scoring runtime.

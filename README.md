# 🏥 Medical-Fraud-Detection
## Overview

This project builds a scalable **data processing pipeline using PySpark** to detect potential healthcare fraud.
It integrates multiple CMS datasets, cleans inconsistencies, performs joins, and engineers meaningful features for downstream machine learning.

---

## Project Structure

```
Big Data Project/
│
├── data/
│   ├── raw/              # Original datasets (CMS)
│   └── processed/        # Cleaned + feature-engineered data
│
├── src/
│   ├── load.py           # Data loading functions
│   ├── clean.py          # Data cleaning & standardization
│   ├── join.py           # Aggregations and joins
│   ├── features.py       # Feature engineering
│   ├── label.py          # Fraud labeling (LEIE)
│   └── main.py           # Pipeline execution
│
├── venv/
└── README.md
```

---

## Datasets Used

All datasets are sourced from CMS (Centers for Medicare & Medicaid Services):

* **Prescribers Data (2023)**
* **Open Payments Data (2023)**
* **LEIE (Exclusion List)**
* **Prescriber Drug Data**
* **Covered Recipient Profile**

---

## Pipeline Steps

### 1. Data Loading

Datasets are loaded using PySpark:

```python
spark.read.csv(..., header=True, inferSchema=True)
```

Validation ensures required columns exist before processing.

---

### 2. Data Cleaning

Key cleaning steps:

* Standardized **NPI (provider_id)** across all datasets
* Removed `.0` artifacts from numeric NPIs
* Trimmed whitespace and handled null values
* Ensured consistent **10-digit formatting using `lpad`**

This step is critical to ensure successful joins.

---

### 3. Data Aggregation

#### Payments

* Total payments per provider:

```python
sum(payment_amount)
```

#### Drug Data

* Unique drugs prescribed
* Total drug claims

#### Recipient Data

* Number of interactions per provider

---

### 4. Data Joining

All datasets are joined on:

```text
provider_id (NPI)
```

Join strategy:

* Left joins to preserve all providers
* Missing values filled with 0

---

### 5. Feature Engineering

The following features were created:

#### Financial Features

* `total_payments`
* `log_payments`
* `payment_per_claim`

#### Behavioral Features

* `unique_drugs`
* `total_drug_claims`
* `recipient_interactions`

#### Derived Metrics

* `avg_cost_per_claim`
* `claims_per_drug`
* `claims_per_beneficiary`

#### Flags

* `high_payment_flag`
* `high_cost_flag`

---

### 6. Handling Big Data Constraints

Due to dataset size:

* Sampling is applied after joins
* Expensive operations like full `.describe()` are avoided
* Spark transformations are optimized to prevent memory overflow

---

### 7. Saving Processed Data

Final feature-engineered dataset is saved in **Parquet format**:

```python
featured_df.write.mode("overwrite").parquet("data/processed/featured_data")
```

Why Parquet?

* Efficient storage
* Faster reads
* Preserves schema

---

## Output

Processed dataset stored in:

```
data/processed/featured_data/
```

Contains:

* Cleaned provider-level data
* Engineered features ready for modeling

---

## 8. ML stage

The project now includes a modeling pipeline in [src/train.py] that:

- Selects numeric features automatically
- Applies class weighting to address fraud class imbalance
- Splits data into train and test sets
- Trains both Logistic Regression and Random Forest models
- Compares models using AUC-PR, AUC-ROC, precision, recall, F1, and accuracy
- Selects the best model using AUC-PR, which is better suited for imbalanced fraud detection

Run the full workflow from [src/main.py](/Users/dishashanbhag/Documents/Medical-Fraud-Detection/src/main.py).

## 9. Recommended data expansion

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

## How to Run

### 1. Activate environment

```bash
source venv/bin/activate
```

### 2. Ensure Java compatibility (Java 17 recommended)

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
```

### 3. Run pipeline

```bash
venv/bin/python src/main.py
```
## Presentation

A concise overview of the project, including pipeline design, modeling approach, and results, is available here:

[Project Presentation]((https://canva.link/qkjb2cj9ntdx4ix))

---

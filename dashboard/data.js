window.__DASHBOARD_DATA__ = {
  "bestModelName": "gbt_classifier",
  "metrics": {
    "balance_summary": {
      "balanced_rows": 2030,
      "negative_count": 2170450,
      "positive_count": 498,
      "sampling_fraction": 0.0006883365200764819
    },
    "balanced_train_rows": 2030,
    "best_model_name": "gbt_classifier",
    "feature_columns": [
      "Antbtc_Tot_Benes",
      "Antbtc_Tot_Clms",
      "Antbtc_Tot_Drug_Cst",
      "Antpsyct_GE65_Tot_Benes",
      "Antpsyct_GE65_Tot_Clms",
      "Antpsyct_GE65_Tot_Drug_Cst",
      "Bene_Age_65_74_Cnt",
      "Bene_Age_75_84_Cnt",
      "Bene_Age_GT_84_Cnt",
      "Bene_Age_LT_65_Cnt",
      "Bene_Avg_Age",
      "Bene_Avg_Risk_Scre",
      "Bene_Dual_Cnt",
      "Bene_Feml_Cnt",
      "Bene_Male_Cnt",
      "Bene_Ndual_Cnt",
      "Bene_Race_Api_Cnt",
      "Bene_Race_Black_Cnt",
      "Bene_Race_Hspnc_Cnt",
      "Bene_Race_Natind_Cnt",
      "Bene_Race_Othr_Cnt",
      "Bene_Race_Wht_Cnt",
      "Brnd_Tot_Clms",
      "Brnd_Tot_Drug_Cst",
      "GE65_Tot_30day_Fills",
      "GE65_Tot_Benes",
      "GE65_Tot_Clms",
      "GE65_Tot_Day_Suply",
      "GE65_Tot_Drug_Cst",
      "Gnrc_Tot_Clms",
      "Gnrc_Tot_Drug_Cst",
      "LIS_Drug_Cst",
      "LIS_Tot_Clms",
      "MAPD_Tot_Clms",
      "MAPD_Tot_Drug_Cst",
      "NonLIS_Drug_Cst",
      "NonLIS_Tot_Clms",
      "Opioid_LA_Prscrbr_Rate",
      "Opioid_LA_Tot_Benes",
      "Opioid_LA_Tot_Clms",
      "Opioid_LA_Tot_Drug_Cst",
      "Opioid_LA_Tot_Suply",
      "Opioid_Prscrbr_Rate",
      "Opioid_Tot_Benes",
      "Opioid_Tot_Clms",
      "Opioid_Tot_Drug_Cst",
      "Opioid_Tot_Suply",
      "Othr_Tot_Clms",
      "Othr_Tot_Drug_Cst",
      "PDP_Tot_Clms",
      "PDP_Tot_Drug_Cst",
      "Prscrbr_RUCA",
      "Prscrbr_State_FIPS",
      "Prscrbr_zip5",
      "Tot_30day_Fills",
      "Tot_Benes",
      "Tot_Clms",
      "Tot_Day_Suply",
      "Tot_Drug_Cst",
      "avg_cost_per_claim",
      "claims_per_beneficiary",
      "claims_per_drug",
      "high_cost_flag",
      "high_payment_flag",
      "log_payments",
      "payment_per_claim",
      "recipient_interactions",
      "report_year",
      "total_drug_claims",
      "total_payments",
      "unique_drugs"
    ],
    "metrics_by_model": {
      "gbt_classifier": {
        "accuracy": 0.8917910998590208,
        "auc_pr": 0.0006653377846345922,
        "auc_roc": 0.7464601706403124,
        "f1": 0.0013283604966024625,
        "fn": 64,
        "fp": 58577,
        "precision": 0.0006653473454346936,
        "recall": 0.3786407766990291,
        "tn": 483244,
        "tp": 39
      },
      "logistic_regression": {
        "accuracy": 0.006039592267550431,
        "auc_pr": 0.00044703260534492256,
        "auc_roc": 0.631785247458306,
        "f1": 0.00037487032641555303,
        "fn": 2,
        "fp": 538649,
        "precision": 0.00018747099767981438,
        "recall": 0.9805825242718447,
        "tn": 3172,
        "tp": 101
      },
      "random_forest": {
        "accuracy": 0.011898347369741883,
        "auc_pr": 0.0002848001537585431,
        "auc_roc": 0.6076812474323597,
        "f1": 0.00037709220837891415,
        "fn": 2,
        "fp": 535474,
        "precision": 0.00018858236474816786,
        "recall": 0.9805825242718447,
        "tn": 6347,
        "tp": 101
      }
    },
    "test_rows": 541924,
    "train_rows": 2170948
  },
  "timings": {
    "feature_dataset_rows": 2712974,
    "feature_dataset_seconds": 94.439,
    "labeled_rows": 2712872,
    "labeling_seconds": 1.062,
    "load_label_sources_seconds": 2.53,
    "model_training_seconds": 347.763,
    "risk_scoring_seconds": 100.683,
    "total_pipeline_seconds": 553.091,
    "used_processed_featured_data": false
  },
  "confusionMatrix": {
    "tp": 39,
    "tn": 483244,
    "fp": 58577,
    "fn": 64
  },
  "hybridSummary": {
    "bands": [
      {
        "band": "high",
        "count": 197
      },
      {
        "band": "low",
        "count": 5070518
      },
      {
        "band": "medium",
        "count": 116965
      }
    ]
  },
  "topRiskRows": [
    {
      "provider_id": "1073578951",
      "hybrid_risk_score": "0.8355402008678665",
      "hybrid_risk_band": "high",
      "anomaly_score": "0.6330676047689533",
      "is_fraud": "0",
      "supervised_score": "0.9705219316004753",
      "Prscrbr_Type": "Infectious Disease",
      "Prscrbr_State_Abrvtn": "OH"
    },
    {
      "provider_id": "1770570657",
      "hybrid_risk_score": "0.8224746323572161",
      "hybrid_risk_band": "high",
      "anomaly_score": "0.6396847661667555",
      "is_fraud": "0",
      "supervised_score": "0.9443345431508565",
      "Prscrbr_Type": "Internal Medicine",
      "Prscrbr_State_Abrvtn": "VA"
    },
    {
      "provider_id": "1265461941",
      "hybrid_risk_score": "0.8191082606859995",
      "hybrid_risk_band": "high",
      "anomaly_score": "0.6724318337624576",
      "is_fraud": "0",
      "supervised_score": "0.9168925453016941",
      "Prscrbr_Type": "Neuropsychiatry",
      "Prscrbr_State_Abrvtn": "FL"
    },
    {
      "provider_id": "1003998816",
      "hybrid_risk_score": "0.8158635807582703",
      "hybrid_risk_band": "high",
      "anomaly_score": "0.5765185956289031",
      "is_fraud": "0",
      "supervised_score": "0.9754269041778483",
      "Prscrbr_Type": "Allergy/ Immunology",
      "Prscrbr_State_Abrvtn": "OK"
    },
    {
      "provider_id": "1003998816",
      "hybrid_risk_score": "0.8157336021621066",
      "hybrid_risk_band": "high",
      "anomaly_score": "0.5765185956289031",
      "is_fraud": "0",
      "supervised_score": "0.9752102731842422",
      "Prscrbr_Type": "Allergy/ Immunology",
      "Prscrbr_State_Abrvtn": "OK"
    },
    {
      "provider_id": "1902071368",
      "hybrid_risk_score": "0.8072829573089264",
      "hybrid_risk_band": "high",
      "anomaly_score": "0.5631490834751047",
      "is_fraud": "0",
      "supervised_score": "0.970038873198141",
      "Prscrbr_Type": "Hematology-Oncology",
      "Prscrbr_State_Abrvtn": "AL"
    },
    {
      "provider_id": "1902071368",
      "hybrid_risk_score": "0.8048284406089763",
      "hybrid_risk_band": "high",
      "anomaly_score": "0.5631490834751047",
      "is_fraud": "0",
      "supervised_score": "0.9659480120315574",
      "Prscrbr_Type": "Hematology-Oncology",
      "Prscrbr_State_Abrvtn": "AL"
    },
    {
      "provider_id": "1598797219",
      "hybrid_risk_score": "0.8044520432690163",
      "hybrid_risk_band": "high",
      "anomaly_score": "0.6836355963918013",
      "is_fraud": "0",
      "supervised_score": "0.8849963411871598",
      "Prscrbr_Type": "Hematology",
      "Prscrbr_State_Abrvtn": "FL"
    },
    {
      "provider_id": "1568451466",
      "hybrid_risk_score": "0.8009521027296637",
      "hybrid_risk_band": "high",
      "anomaly_score": "0.5203139384958271",
      "is_fraud": "0",
      "supervised_score": "0.988044212218888",
      "Prscrbr_Type": "Nephrology",
      "Prscrbr_State_Abrvtn": "SC"
    },
    {
      "provider_id": "1568451466",
      "hybrid_risk_score": "0.8006686787977343",
      "hybrid_risk_band": "high",
      "anomaly_score": "0.5203139384958271",
      "is_fraud": "0",
      "supervised_score": "0.9875718389990057",
      "Prscrbr_Type": "Nephrology",
      "Prscrbr_State_Abrvtn": "SC"
    },
    {
      "provider_id": "1528051729",
      "hybrid_risk_score": "0.7984106026781945",
      "hybrid_risk_band": "high",
      "anomaly_score": "0.5319361925982372",
      "is_fraud": "0",
      "supervised_score": "0.9760602093981662",
      "Prscrbr_Type": "Hematology",
      "Prscrbr_State_Abrvtn": "NC"
    },
    {
      "provider_id": "1043502206",
      "hybrid_risk_score": "0.7935852873921436",
      "hybrid_risk_band": "high",
      "anomaly_score": "0.5264158787176982",
      "is_fraud": "0",
      "supervised_score": "0.9716982265084404",
      "Prscrbr_Type": "Hematology-Oncology",
      "Prscrbr_State_Abrvtn": "KY"
    },
    {
      "provider_id": "1770570657",
      "hybrid_risk_score": "0.7930513847812237",
      "hybrid_risk_band": "high",
      "anomaly_score": "0.6396847661667555",
      "is_fraud": "0",
      "supervised_score": "0.8952957971908692",
      "Prscrbr_Type": "Internal Medicine",
      "Prscrbr_State_Abrvtn": "VA"
    },
    {
      "provider_id": "1255544680",
      "hybrid_risk_score": "0.7927141576846499",
      "hybrid_risk_band": "high",
      "anomaly_score": "0.6103326771411495",
      "is_fraud": "0",
      "supervised_score": "0.9143018113803171",
      "Prscrbr_Type": "Hematology-Oncology",
      "Prscrbr_State_Abrvtn": "MO"
    },
    {
      "provider_id": "1598797219",
      "hybrid_risk_score": "0.7910886126680914",
      "hybrid_risk_band": "high",
      "anomaly_score": "0.6836355963918013",
      "is_fraud": "0",
      "supervised_score": "0.8627239568522849",
      "Prscrbr_Type": "Hematology",
      "Prscrbr_State_Abrvtn": "FL"
    },
    {
      "provider_id": "1861536534",
      "hybrid_risk_score": "0.7905609617197877",
      "hybrid_risk_band": "high",
      "anomaly_score": "0.5178003989331545",
      "is_fraud": "0",
      "supervised_score": "0.9724013369108766",
      "Prscrbr_Type": "Orthopedic Surgery",
      "Prscrbr_State_Abrvtn": "FL"
    },
    {
      "provider_id": "1205807500",
      "hybrid_risk_score": "0.7902902313481031",
      "hybrid_risk_band": "high",
      "anomaly_score": "0.549843862014996",
      "is_fraud": "0",
      "supervised_score": "0.9505878109035076",
      "Prscrbr_Type": "Surgery",
      "Prscrbr_State_Abrvtn": "FL"
    },
    {
      "provider_id": "1861536534",
      "hybrid_risk_score": "0.790016893241061",
      "hybrid_risk_band": "high",
      "anomaly_score": "0.5178003989331545",
      "is_fraud": "0",
      "supervised_score": "0.9714945561129986",
      "Prscrbr_Type": "Orthopedic Surgery",
      "Prscrbr_State_Abrvtn": "FL"
    },
    {
      "provider_id": "1659596179",
      "hybrid_risk_score": "0.7889272071140512",
      "hybrid_risk_band": "high",
      "anomaly_score": "0.4910068009266927",
      "is_fraud": "0",
      "supervised_score": "0.9875408112389569",
      "Prscrbr_Type": "Hematology-Oncology",
      "Prscrbr_State_Abrvtn": "OR"
    },
    {
      "provider_id": "1568451466",
      "hybrid_risk_score": "0.7884034723488624",
      "hybrid_risk_band": "high",
      "anomaly_score": "0.48894236254382406",
      "is_fraud": "0",
      "supervised_score": "0.988044212218888",
      "Prscrbr_Type": "Nephrology",
      "Prscrbr_State_Abrvtn": "SC"
    },
    {
      "provider_id": "1568451466",
      "hybrid_risk_score": "0.7881200484169331",
      "hybrid_risk_band": "high",
      "anomaly_score": "0.48894236254382406",
      "is_fraud": "0",
      "supervised_score": "0.9875718389990057",
      "Prscrbr_Type": "Nephrology",
      "Prscrbr_State_Abrvtn": "SC"
    },
    {
      "provider_id": "1194768614",
      "hybrid_risk_score": "0.7877189369649029",
      "hybrid_risk_band": "high",
      "anomaly_score": "0.5375314505494273",
      "is_fraud": "0",
      "supervised_score": "0.9545105945752201",
      "Prscrbr_Type": "Neurology",
      "Prscrbr_State_Abrvtn": "MD"
    },
    {
      "provider_id": "1104876093",
      "hybrid_risk_score": "0.7876960524197657",
      "hybrid_risk_band": "high",
      "anomaly_score": "0.5075156572300499",
      "is_fraud": "0",
      "supervised_score": "0.9744829825462429",
      "Prscrbr_Type": "Infectious Disease",
      "Prscrbr_State_Abrvtn": "CA"
    },
    {
      "provider_id": "1174787352",
      "hybrid_risk_score": "0.7858374900212759",
      "hybrid_risk_band": "high",
      "anomaly_score": "0.49328812116625315",
      "is_fraud": "0",
      "supervised_score": "0.9808704025912911",
      "Prscrbr_Type": "Hematology",
      "Prscrbr_State_Abrvtn": "GA"
    },
    {
      "provider_id": "1003998816",
      "hybrid_risk_score": "0.7854101658054616",
      "hybrid_risk_band": "high",
      "anomaly_score": "0.5003850582468816",
      "is_fraud": "0",
      "supervised_score": "0.9754269041778483",
      "Prscrbr_Type": "Allergy/ Immunology",
      "Prscrbr_State_Abrvtn": "OK"
    }
  ]
};

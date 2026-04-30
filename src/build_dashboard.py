import csv
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
METRICS_PATH = ROOT / "data" / "artifacts" / "model_metrics.json"
TIMINGS_PATH = ROOT / "data" / "artifacts" / "stage_timings.json"
HYBRID_SUMMARY_PATH = ROOT / "data" / "artifacts" / "hybrid_summary.json"
TOP_RISK_DIR = ROOT / "data" / "processed" / "top_risk_providers"
DASHBOARD_DIR = ROOT / "dashboard"
DATA_JS_PATH = DASHBOARD_DIR / "data.js"


def load_json(path):
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def load_optional_json(path):
    if not path.exists():
        return None
    return load_json(path)


def load_top_risk_rows(limit=25):
    if not TOP_RISK_DIR.exists():
        return []

    csv_paths = sorted(
        path for path in TOP_RISK_DIR.glob("*.csv")
        if path.is_file() and not path.name.startswith(".")
    )
    if not csv_paths:
        return []

    rows = []
    with csv_paths[0].open("r", encoding="utf-8", errors="replace") as handle:
        reader = csv.DictReader(handle)
        for index, row in enumerate(reader):
            if index >= limit:
                break
            rows.append(row)
    return rows


def build_payload():
    metrics = load_json(METRICS_PATH)
    timings = load_json(TIMINGS_PATH)
    hybrid_summary = load_optional_json(HYBRID_SUMMARY_PATH)
    top_risk_rows = load_top_risk_rows(limit=25)

    best_model_name = metrics["best_model_name"]
    best_metrics = metrics["metrics_by_model"][best_model_name]

    return {
        "bestModelName": best_model_name,
        "metrics": metrics,
        "timings": timings,
        "confusionMatrix": {
            "tp": best_metrics["tp"],
            "tn": best_metrics["tn"],
            "fp": best_metrics["fp"],
            "fn": best_metrics["fn"],
        },
        "hybridSummary": hybrid_summary,
        "topRiskRows": top_risk_rows,
    }


def write_dashboard_data(payload):
    DASHBOARD_DIR.mkdir(parents=True, exist_ok=True)
    script = "window.__DASHBOARD_DATA__ = " + json.dumps(payload, indent=2) + ";\n"
    DATA_JS_PATH.write_text(script, encoding="utf-8")


def main():
    payload = build_payload()
    write_dashboard_data(payload)
    print(f"Wrote dashboard data to {DATA_JS_PATH}")


if __name__ == "__main__":
    main()

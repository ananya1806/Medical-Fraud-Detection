const { createElement: h, Fragment } = React;
const { createRoot } = ReactDOM;

const data = window.__DASHBOARD_DATA__ || {};
const metrics = data.metrics || {};
const metricsByModel = metrics.metrics_by_model || {};
const bestModelName = data.bestModelName || data.best_model_name || metrics.best_model_name;
const bestMetrics = metricsByModel[bestModelName] || {};
const timings = data.timings || {};
const confusion = data.confusionMatrix || {};
const topRiskRows = data.topRiskRows || [];
const hybridSummary = data.hybridSummary || null;

const modelMeta = {
  gbt_classifier: { label: "GBT", color: "#216aa1" },
  logistic_regression: { label: "Logistic Regression", color: "#2bb4b2" },
  random_forest: { label: "Random Forest", color: "#9fd8ff" },
};

function fmtPct(value) {
  return `${(Number(value || 0) * 100).toFixed(2)}%`;
}

function fmtFloat(value, digits = 3) {
  return Number(value || 0).toFixed(digits);
}

function fmtInt(value) {
  return Number(value || 0).toLocaleString();
}

function Nav(page) {
  return h("div", { className: "topbar" },
    h("div", { className: "brand" },
      h("div", { className: "brand-kicker" }, "Presentation Site"),
      h("div", { className: "brand-title" }, "Medical Fraud Detection Dashboard")
    ),
    h("div", { className: "nav" },
      h("a", { href: "./index.html", className: `nav-link ${page === "overview" ? "active" : ""}` }, "Overview"),
      h("a", { href: "./findings.html", className: `nav-link ${page === "findings" ? "active" : ""}` }, "Findings")
    )
  );
}

function Panel(props) {
  return h("section", { className: `panel ${props.className || ""}` },
    h("div", { className: "panel-inner" }, props.children)
  );
}

function Hero() {
  return h("section", { className: "hero" },
    h(Panel, null,
      h("div", { className: "eyebrow" }, "Medical Intelligence Dashboard"),
      h("h1", null, "Medical Fraud Detection", h("br"), "Hybrid Risk Platform"),
      h("p", { className: "copy" },
        "A multi-source healthcare analytics pipeline combining temporal labeling, provider behavior features, supervised learning, anomaly scoring, and final risk ranking."
      ),
      h("div", { className: "kpi-grid" },
        KPI("Best Model", prettifyModel(bestModelName)),
        KPI("AUC-ROC", fmtFloat(bestMetrics.auc_roc)),
        KPI("Recall", fmtPct(bestMetrics.recall)),
        KPI("Prediction Cohort", fmtInt(timings.labeled_rows))
      )
    ),
    h(Panel, { className: "accent-panel" },
      h("div", { className: "eyebrow" }, "Performance Snapshot"),
      h("div", { className: "accent-stat" }, fmtPct(bestMetrics.accuracy)),
      h("p", { className: "copy" },
        "The boosted-tree model leads the current benchmark on overall discrimination and serves as the core supervised component inside the final hybrid fraud-risk system."
      ),
      h("div", { className: "mini-note" },
        h("strong", null, "Confusion matrix highlights"),
        h("br"),
        `True positives: ${fmtInt(confusion.tp)} | False negatives: ${fmtInt(confusion.fn)}`,
        h("br"),
        `False positives: ${fmtInt(confusion.fp)} | True negatives: ${fmtInt(confusion.tn)}`
      )
    )
  );
}

function KPI(label, value) {
  return h("div", { className: "kpi" },
    h("span", { className: "kpi-label" }, label),
    h("div", { className: "kpi-value" }, value)
  );
}

function prettifyModel(name) {
  if (!name) return "N/A";
  const meta = modelMeta[name];
  return meta ? meta.label : name.replaceAll("_", " ");
}

function PipelineOverview() {
  const steps = [
    ["Collect Sources", "CMS prescribers, provider-drug data, Open Payments, recipient data, and LEIE exclusion files."],
    ["Clean & Normalize", "Standardize identifiers, align schemas across years, and preserve provider-level keys."],
    ["Engineer Features", "Generate cost, utilization, opioid, payment, and provider-behavior features."],
    ["Temporal Labeling", "Create future fraud targets from LEIE timing while excluding already flagged providers."],
    ["Train Models", "Benchmark Logistic Regression, Random Forest, and Gradient Boosted Trees in Spark MLlib."],
    ["Hybrid Ranking", "Combine supervised risk with anomaly signals to prioritize suspicious providers."]
  ];

  return h("section", { className: "panel", style: { marginBottom: "24px" } },
    h("div", { className: "panel-inner" },
      h("div", { className: "section-title" },
        h("h2", null, "Pipeline Overview"),
        h("span", null, "How the system was built")
      ),
      h("div", { className: "pipeline-grid" },
        ...steps.map((step, idx) =>
          h("div", { className: "pipeline-step", key: step[0] },
            h("div", { className: "step-num" }, String(idx + 1)),
            h("h3", null, step[0]),
            h("p", null, step[1])
          )
        )
      )
    )
  );
}

function OverviewPage() {
  return h("div", { className: "shell" },
    Nav("overview"),
    Hero(),
    h("section", { className: "section-grid" },
      h("section", { className: "panel" },
        h("div", { className: "panel-inner" },
          h("div", { className: "section-title" },
            h("h2", null, "Model Comparison"),
            h("span", null, "Core model benchmark")
          ),
          h(ModelComparisonChart)
        )
      ),
      h("section", { className: "panel" },
        h("div", { className: "panel-inner" },
          h("div", { className: "section-title" },
            h("h2", null, "Confusion Matrix"),
            h("span", null, "Best model classification breakdown")
          ),
          h(ConfusionMatrix)
        )
      )
    ),
    PipelineOverview(),
    h("section", { className: "section-grid" },
      h("section", { className: "panel" },
        h("div", { className: "panel-inner" },
          h("div", { className: "section-title" },
            h("h2", null, "Fraud Distribution"),
            h("span", null, "Train and test imbalance view")
          ),
          h(BalanceDonutCard)
        )
      ),
      h("section", { className: "panel" },
        h("div", { className: "panel-inner" },
          h("div", { className: "section-title" },
            h("h2", null, "Pipeline Runtime"),
            h("span", null, "Measured stage timing")
          ),
          h(RuntimeChart)
        )
      )
    )
  );
}

function FindingsPage() {
  return h("div", { className: "shell" },
    Nav("findings"),
    h("section", { className: "hero" },
      h(Panel, null,
        h("div", { className: "eyebrow" }, "Detection Findings"),
        h("h1", null, "Suspicious Provider", h("br"), "Review Workspace"),
        h("p", { className: "copy" },
          "A presentation-ready view of the final risk outputs, including top provider rankings, hybrid risk banding, and the strongest operational signals."
        ),
        h("div", { className: "kpi-grid" },
          KPI("Top Provider Score", topRiskRows.length ? fmtFloat(topRiskRows[0].hybrid_risk_score) : "N/A"),
          KPI("Test Positives", fmtInt((confusion.tp || 0) + (confusion.fn || 0))),
          KPI("True Positives", fmtInt(confusion.tp)),
          KPI("False Positives", fmtInt(confusion.fp))
        )
      ),
      h(Panel, { className: "accent-panel" },
        h("div", { className: "eyebrow" }, "Priority Review Set"),
        h("div", { className: "accent-stat" }, "Top 25"),
        h("p", { className: "copy" },
          "This view highlights the highest-priority provider records based on the hybrid scoring framework, combining supervised model output with anomaly-driven risk signals."
        ),
        h("div", { className: "mini-note" },
          "The ranked output is intended to support targeted review, triage, and downstream investigative prioritization."
        )
      )
    ),
    h("section", { className: "findings-grid" },
      h("div", { className: "summary-grid" },
        h("section", { className: "summary-card" },
          h("h3", null, "Hybrid Risk Summary"),
          hybridSummary ? h(HybridRiskChart) : h("div", { className: "pending-card" },
            h("p", { className: "copy" },
              "Hybrid band counts will render here automatically after the pipeline exports ",
              h("strong", null, "data/artifacts/hybrid_summary.json"),
              "."
            )
          ),
          h("div", { className: "footer" },
            hybridSummary
              ? "This chart shows how providers are distributed across low, medium, high, and critical risk bands."
              : "The React page is already wired for the chart; it will populate as soon as the summary artifact exists."
          )
        ),
        h("section", { className: "summary-card" },
          h("h3", null, "Best Model Signals"),
          h("div", { className: "kpi-grid", style: { gridTemplateColumns: "1fr 1fr", marginTop: 0 } },
            KPI("Precision", fmtPct(bestMetrics.precision)),
            KPI("F1", fmtFloat(bestMetrics.f1, 4)),
            KPI("AUC-PR", fmtFloat(bestMetrics.auc_pr, 4)),
            KPI("Recall", fmtPct(bestMetrics.recall))
          )
        )
      ),
      h("section", { className: "panel table-panel" },
        h("div", { className: "panel-inner" },
          h("div", { className: "section-title" },
            h("h2", null, "Top Risk Providers"),
            h("span", null, "Highest hybrid-risk rows")
          ),
          h(TopRiskTable)
        )
      )
    )
  );
}

function ModelComparisonChart() {
  const models = ["gbt_classifier", "logistic_regression", "random_forest"];
  const metricDefs = [
    ["auc_roc", "AUC-ROC", (v) => fmtFloat(v)],
    ["recall", "Recall", (v) => fmtPct(v)],
    ["accuracy", "Accuracy", (v) => fmtPct(v)],
    ["auc_pr", "AUC-PR", (v) => fmtFloat(v, 4)],
  ];
  const width = 760;
  const rowHeight = 82;
  const headerHeight = 42;
  const height = headerHeight + metricDefs.length * rowHeight + 18;

  const children = [];
  metricDefs.forEach(([key, label, formatter], metricIndex) => {
    const yBase = headerHeight + metricIndex * rowHeight;
    children.push(
      h("text", {
        key: `${key}-label`,
        x: 0,
        y: yBase + 16,
        fill: "#12324d",
        fontSize: 14,
        fontWeight: 700,
        letterSpacing: 1.6,
      }, label)
    );

    const maxValue = Math.max(...models.map((model) => Number(metricsByModel[model]?.[key] || 0)), 0.0001);
    models.forEach((model, idx) => {
      const meta = modelMeta[model];
      const value = Number(metricsByModel[model]?.[key] || 0);
      const barX = 160;
      const barY = yBase + 26 + idx * 16;
      const trackWidth = 430;
      const fillWidth = Math.max(4, (value / maxValue) * trackWidth);

      children.push(
        h(Fragment, { key: `${key}-${model}` },
          h("text", {
            x: barX,
            y: barY - 1,
            fill: "#6b8aa5",
            fontSize: 11,
            fontWeight: 700,
          }, meta.label),
          h("rect", {
            x: barX + 110,
            y: barY - 11,
            width: trackWidth,
            height: 10,
            rx: 10,
            fill: "rgba(74,163,224,0.10)",
          }),
          h("rect", {
            x: barX + 110,
            y: barY - 11,
            width: fillWidth,
            height: 10,
            rx: 10,
            fill: meta.color,
          }),
          h("text", {
            x: width - 18,
            y: barY - 1,
            fill: "#12324d",
            fontSize: 12,
            fontWeight: 700,
            textAnchor: "end",
          }, formatter(value))
        )
      );
    });
  });

  return h(Fragment, null,
    h("div", { className: "legend" },
      ...models.map((model) => h("div", { className: "legend-item", key: model },
        h("span", { className: "legend-swatch", style: { background: modelMeta[model].color } }),
        modelMeta[model].label
      ))
    ),
    h("div", { className: "chart-card svg-wrap" },
      h("svg", { viewBox: `0 0 ${width} ${height}`, role: "img", "aria-label": "Model comparison chart" }, children)
    ),
    h("div", { className: "footer" },
      "This chart compares the three supervised baselines side by side and shows why GBT is the strongest current production candidate."
    )
  );
}

function ConfusionMatrix() {
  const cells = [
    { label: "True Positive", value: confusion.tp, note: "Fraud correctly detected", bg: "linear-gradient(180deg, #1ea26d, #2bb47d)" },
    { label: "False Positive", value: confusion.fp, note: "Flagged but not labeled fraud", bg: "linear-gradient(180deg, #f2a14b, #f4b46f)" },
    { label: "False Negative", value: confusion.fn, note: "Missed fraud case", bg: "linear-gradient(180deg, #e27f5f, #ef9d7f)" },
    { label: "True Negative", value: confusion.tn, note: "Correctly cleared", bg: "linear-gradient(180deg, #216aa1, #4aa3e0)" },
  ];

  return h(Fragment, null,
    h("div", { className: "matrix-grid" },
      ...cells.map((cell) =>
        h("div", { className: "matrix-cell", key: cell.label, style: { background: cell.bg } },
          h("div", { className: "matrix-label" }, cell.label),
          h("div", { className: "matrix-value" }, fmtInt(cell.value)),
          h("div", { className: "matrix-note" }, cell.note)
        )
      )
    ),
    h("div", { className: "footer" },
      "The confusion matrix is a stronger presentation visual than raw accuracy alone because it shows the actual detection behavior on fraud and non-fraud cases."
    )
  );
}

function polarToCartesian(cx, cy, r, angleInDegrees) {
  const angleInRadians = ((angleInDegrees - 90) * Math.PI) / 180.0;
  return {
    x: cx + r * Math.cos(angleInRadians),
    y: cy + r * Math.sin(angleInRadians),
  };
}

function describeArc(cx, cy, r, startAngle, endAngle) {
  const start = polarToCartesian(cx, cy, r, endAngle);
  const end = polarToCartesian(cx, cy, r, startAngle);
  const largeArcFlag = endAngle - startAngle <= 180 ? "0" : "1";
  return ["M", start.x, start.y, "A", r, r, 0, largeArcFlag, 0, end.x, end.y].join(" ");
}

function BalanceDonutCard() {
  const trainPositive = Number(metrics.balance_summary?.positive_count || 0);
  const trainNegative = Number(metrics.balance_summary?.negative_count || 0);
  const trainShare = trainPositive / Math.max(1, trainPositive + trainNegative);
  const testPositive = Number((confusion.tp || 0) + (confusion.fn || 0));
  const testNegative = Number((confusion.tn || 0) + (confusion.fp || 0));
  const testShare = testPositive / Math.max(1, testPositive + testNegative);
  const width = 430;
  const height = 270;
  const cx = 120;
  const cy = 124;
  const r = 70;
  const stroke = 20;
  const fraudAngle = Math.max(3, trainShare * 360);

  return h(Fragment, null,
    h("div", { className: "viz-grid" },
      h("div", { className: "viz-card" },
        h("h3", null, "Dataset Imbalance"),
        h("div", { className: "svg-wrap" },
          h("svg", { viewBox: `0 0 ${width} ${height}`, role: "img", "aria-label": "Fraud imbalance donut chart" },
            h("circle", {
              cx, cy, r, fill: "none", stroke: "rgba(74,163,224,0.12)", strokeWidth: stroke,
            }),
            h("path", {
              d: describeArc(cx, cy, r, 0, fraudAngle),
              fill: "none",
              stroke: "#2bb4b2",
              strokeWidth: stroke,
              strokeLinecap: "round",
            }),
            h("text", { x: cx, y: cy - 2, fill: "#12324d", fontSize: 15, fontWeight: 700, textAnchor: "middle" }, "Train Fraud"),
            h("text", { x: cx, y: cy + 24, fill: "#123f68", fontSize: 24, fontWeight: 800, textAnchor: "middle" }, fmtPct(trainShare)),
            renderMetricBadge(250, 48, "Train positives", fmtInt(trainPositive), "#e8fcf8"),
            renderMetricBadge(250, 96, "Train negatives", fmtInt(trainNegative), "#edf8ff"),
            renderMetricBadge(250, 144, "Test positives", fmtInt(testPositive), "#e8fcf8"),
            renderMetricBadge(250, 192, "Test negatives", fmtInt(testNegative), "#edf8ff")
          )
        ),
        h("div", { className: "insight" },
          `Train fraud share: ${fmtPct(trainShare)}`,
          h("br"),
          `Test fraud share: ${fmtPct(testShare)}`
        )
      ),
      h("div", { className: "viz-card" },
        h("h3", null, "Pipeline Runtime"),
        h(RuntimeChartSvg),
        h("div", { className: "insight" },
          `End-to-end runtime: ${Number(timings.total_pipeline_seconds || 0).toFixed(1)}s`,
          h("br"),
          "Feature building and model training dominate the overall cost."
        )
      )
    )
  );
}

function renderMetricBadge(x, y, label, value, fill) {
  return h(Fragment, { key: `${label}-${y}` },
    h("rect", { x, y, width: 144, height: 34, rx: 12, fill }),
    h("text", { x: x + 12, y: y + 13, fill: "#6b8aa5", fontSize: 11, fontWeight: 700 }, label),
    h("text", { x: x + 12, y: y + 28, fill: "#12324d", fontSize: 16, fontWeight: 800 }, value)
  );
}

function RuntimeChart() {
  return h(RuntimeChartSvg);
}

function RuntimeChartSvg() {
  const rows = [
    ["Load", Number(timings.load_label_sources_seconds || 0)],
    ["Features", Number(timings.feature_dataset_seconds || 0)],
    ["Labeling", Number(timings.labeling_seconds || 0)],
    ["Training", Number(timings.model_training_seconds || 0)],
    ["Risk", Number(timings.risk_scoring_seconds || 0)],
  ];
  const width = 430;
  const height = 270;
  const chartBottom = 214;
  const chartTop = 34;
  const usableHeight = chartBottom - chartTop;
  const barWidth = 46;
  const gap = 28;
  const startX = 36;
  const maxValue = Math.max(...rows.map((r) => r[1]), 1);
  const colors = ["#9fd8ff", "#4aa3e0", "#2bb4b2", "#216aa1", "#81d0d3"];

  return h("div", { className: "svg-wrap chart-card" },
    h("svg", { viewBox: `0 0 ${width} ${height}`, role: "img", "aria-label": "Runtime chart" },
      h("line", { x1: 24, y1: chartBottom, x2: width - 18, y2: chartBottom, stroke: "rgba(74,163,224,0.18)", strokeWidth: 1.5 }),
      ...rows.flatMap((row, idx) => {
        const x = startX + idx * (barWidth + gap);
        const hgt = (row[1] / maxValue) * usableHeight;
        const y = chartBottom - hgt;
        return [
          h("rect", { key: `${row[0]}-bar`, x, y, width: barWidth, height: hgt, rx: 14, fill: colors[idx % colors.length] }),
          h("text", { key: `${row[0]}-value`, x: x + barWidth / 2, y: y - 10, fill: "#12324d", fontSize: 12, fontWeight: 700, textAnchor: "middle" }, `${row[1].toFixed(1)}s`),
          h("text", { key: `${row[0]}-label`, x: x + barWidth / 2, y: chartBottom + 22, fill: "#6b8aa5", fontSize: 12, fontWeight: 700, textAnchor: "middle" }, row[0]),
        ];
      })
    )
  );
}

function HybridRiskChart() {
  const rows = Array.isArray(hybridSummary?.bands) ? hybridSummary.bands : [];
  if (!rows.length) {
    return h("div", { className: "copy" }, "Hybrid summary artifact is empty.");
  }
  const width = 440;
  const height = 260;
  const chartBottom = 210;
  const chartTop = 34;
  const usableHeight = chartBottom - chartTop;
  const barWidth = 58;
  const gap = 28;
  const startX = 44;
  const maxValue = Math.max(...rows.map((r) => Number(r.count || 0)), 1);
  const colors = {
    critical: "#ef6b57",
    high: "#f2a14b",
    medium: "#4aa3e0",
    low: "#2bb4b2",
  };

  return h("div", { className: "svg-wrap" },
    h("svg", { viewBox: `0 0 ${width} ${height}`, role: "img", "aria-label": "Hybrid risk summary chart" },
      h("line", { x1: 26, y1: chartBottom, x2: width - 20, y2: chartBottom, stroke: "rgba(74,163,224,0.18)", strokeWidth: 1.5 }),
      ...rows.flatMap((row, idx) => {
        const x = startX + idx * (barWidth + gap);
        const count = Number(row.count || 0);
        const hgt = (count / maxValue) * usableHeight;
        const y = chartBottom - hgt;
        return [
          h("rect", {
            key: `${row.band}-bar`,
            x, y, width: barWidth, height: hgt, rx: 14, fill: colors[row.band] || "#4aa3e0"
          }),
          h("text", {
            key: `${row.band}-value`,
            x: x + barWidth / 2, y: y - 10, fill: "#12324d", fontSize: 12, fontWeight: 700, textAnchor: "middle"
          }, fmtInt(count)),
          h("text", {
            key: `${row.band}-label`,
            x: x + barWidth / 2, y: chartBottom + 22, fill: "#6b8aa5", fontSize: 12, fontWeight: 700, textAnchor: "middle"
          }, row.band)
        ];
      })
    )
  );
}

function TopRiskTable() {
  return h(Fragment, null,
    h("table", null,
      h("thead", null,
        h("tr", null,
          h("th", null, "Provider"),
          h("th", null, "Type"),
          h("th", null, "State"),
          h("th", null, "Supervised"),
          h("th", null, "Anomaly"),
          h("th", null, "Hybrid")
        )
      ),
      h("tbody", null,
        ...topRiskRows.map((row, idx) =>
          h("tr", { key: `${row.provider_id}-${idx}` },
            h("td", { className: "provider" }, row.provider_id || ""),
            h("td", null, row.Prscrbr_Type || ""),
            h("td", null, row.Prscrbr_State_Abrvtn || ""),
            h("td", null, fmtFloat(row.supervised_score)),
            h("td", null, fmtFloat(row.anomaly_score)),
            h("td", null, h("span", { className: "score-pill" }, fmtFloat(row.hybrid_risk_score)))
          )
        )
      )
    ),
    h("div", { className: "footer" },
      "These providers are the highest-ranked rows in the saved top-risk export and can be used directly in your project presentation."
    )
  );
}

function App() {
  const page = window.DASHBOARD_PAGE || "overview";
  return page === "findings" ? h(FindingsPage) : h(OverviewPage);
}

createRoot(document.getElementById("root")).render(h(App));

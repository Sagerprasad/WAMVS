import json
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

DRIFT_FILE = BASE_DIR / "workload" / "drift_workload.json"
NO_MV_FILE = BASE_DIR / "results" / "no_mv_baseline.json"
STATIC_FILE = BASE_DIR / "results" / "static_topk_baseline.json"
REWRITE_FILE = BASE_DIR / "workload" / "rewrite_benchmark.json"
MISSING_FILE = BASE_DIR / "workload" / "missing_candidate_benchmark.json"
Q08_FILE = BASE_DIR / "workload" / "q08_benchmark.json"
Q09_FILE = BASE_DIR / "workload" / "q09_benchmark.json"
DEEPSEA_FILE = BASE_DIR / "workload" / "deepsea_baseline_selection.json"
ADAPTIVE_FILE = BASE_DIR / "workload" / "adaptive_experiment_replacement_final.json"

OUTPUT_FILE = BASE_DIR / "results" / "controlled_deepsea_vs_wamvs.json"

with open(DRIFT_FILE) as f:
    drift = json.load(f)

with open(NO_MV_FILE) as f:
    no_mv_raw = json.load(f)

with open(STATIC_FILE) as f:
    static_raw = json.load(f)

with open(DEEPSEA_FILE) as f:
    deepsea = json.load(f)

with open(ADAPTIVE_FILE) as f:
    adaptive = json.load(f)

# ------------------------------------------------------------
# Normalize query benchmark data.
# ------------------------------------------------------------

def normalize_query_list(data):
    if isinstance(data, list):
        return {x["query_id"]: x for x in data}
    if isinstance(data, dict):
        if "results" in data and isinstance(data["results"], list):
            return {x["query_id"]: x for x in data["results"]}
        return data
    return {}

no_mv = normalize_query_list(no_mv_raw)
static = normalize_query_list(static_raw)

# Existing rewrite benchmark
with open(REWRITE_FILE) as f:
    rewrite = normalize_query_list(json.load(f))

with open(MISSING_FILE) as f:
    missing = normalize_query_list(json.load(f))

with open(Q08_FILE) as f:
    q08 = normalize_query_list(json.load(f))

with open(Q09_FILE) as f:
    q09 = normalize_query_list(json.load(f))

# ------------------------------------------------------------
# Build measured execution-cost lookup.
# ------------------------------------------------------------

mv_cost = {}
base_cost = {}

for source in [rewrite, missing, q08, q09]:
    for qid, record in source.items():
        if not isinstance(record, dict):
            continue

        if "baseline_median_ms" in record:
            base_cost[qid] = float(record["baseline_median_ms"])

        if "mv_median_ms" in record:
            mv_cost[qid] = float(record["mv_median_ms"])

# Fall back to No-MV measurements for base costs.
for qid, record in no_mv.items():
    if isinstance(record, dict):
        value = (
            record.get("median_ms")
            or record.get("execution_time_ms")
            or record.get("runtime_ms")
        )
        if value is not None and qid not in base_cost:
            base_cost[qid] = float(value)

# ------------------------------------------------------------
# Query -> candidate mapping.
# ------------------------------------------------------------

query_to_mv = {
    "q01": "MV_001",
    "q02": "MV_002",
    "q03": "MV_003",
    "q04": "MV_001",
    "q05": "MV_004",
    "q06": "MV_005",
    "q07": "MV_003",
    "q08": "MV_006",
    "q09": "MV_003",
    "q10": "MV_007",
}

# Fixed DeepSea-style selection.
deepsea_selected = [
    x["candidate_id"]
    for x in deepsea["selected_views"]
]

# Static Top-K selection used in the existing controlled comparison.
static_selected = ["MV_003", "MV_001", "MV_002", "MV_004"]

# Adaptive selection by interval.
adaptive_by_interval = {
    int(x["interval"]): set(x["selected_views"])
    for x in adaptive
}

# ------------------------------------------------------------
# Calculate workload-weighted costs.
#
# A query is assumed to use its MV if that candidate is present
# in the corresponding strategy's selected set.
# Otherwise its measured No-MV cost is used.
#
# This is the same controlled-cost methodology used for the
# previous Static-vs-WAMVS comparison.
# ------------------------------------------------------------

def strategy_cost(query_id, selected):
    if query_to_mv.get(query_id) in selected:
        return mv_cost.get(query_id, base_cost.get(query_id, 0.0))
    return base_cost.get(query_id, 0.0)


strategies = {
    "No-MV": lambda qid, interval: base_cost.get(qid, 0.0),
    "Static Top-K": lambda qid, interval: strategy_cost(qid, static_selected),
    "DeepSea-style": lambda qid, interval: strategy_cost(qid, deepsea_selected),
    "WAMVS": lambda qid, interval: strategy_cost(
        qid,
        adaptive_by_interval.get(interval, set())
    ),
}

totals = {name: 0.0 for name in strategies}
interval_results = []

for interval_record in drift:
    interval = int(interval_record["interval"])
    phase = interval_record.get("phase", "unknown")
    counts = interval_record["query_counts"]

    row = {
        "interval": interval,
        "phase": phase,
        "query_counts": counts,
        "costs_ms": {},
    }

    for name, cost_function in strategies.items():
        total = 0.0

        for qid, frequency in counts.items():
            total += float(frequency) * cost_function(qid, interval)

        totals[name] = totals[name] + total
        row["costs_ms"][name] = round(total, 4)

    # Adaptive hit rate for the interval.
    total_frequency = sum(counts.values())

    if total_frequency:
        selected = adaptive_by_interval.get(interval, set())

        accelerated_frequency = sum(
            frequency
            for qid, frequency in counts.items()
            if query_to_mv.get(qid) in selected
        )

        row["wamvs_hit_rate_percent"] = round(
            100.0 * accelerated_frequency / total_frequency,
            2
        )
    else:
        row["wamvs_hit_rate_percent"] = 0.0

    interval_results.append(row)

no_mv_total = totals["No-MV"]

summary = {}

for name, total in totals.items():
    summary[name] = {
        "weighted_cost_ms": round(total, 4),
        "speedup_vs_no_mv": round(no_mv_total / total, 4)
        if total else None,
    }

deepsea_total = totals["DeepSea-style"]
wamvs_total = totals["WAMVS"]

summary["DeepSea-style"]["improvement_vs_no_mv_percent"] = round(
    100.0 * (no_mv_total - deepsea_total) / no_mv_total,
    2
)

summary["WAMVS"]["improvement_vs_no_mv_percent"] = round(
    100.0 * (no_mv_total - wamvs_total) / no_mv_total,
    2
)

summary["WAMVS"]["improvement_vs_deepsea_percent"] = round(
    100.0 * (deepsea_total - wamvs_total) / deepsea_total,
    2
)

result = {
    "experiment": "controlled_deepsea_vs_wamvs",
    "methodology": (
        "Workload-weighted modeled comparison using measured query "
        "execution costs and simulated nine-interval workload frequencies."
    ),
    "deepsea_policy": "fixed frequency-greedy selection",
    "deepsea_selected_views": deepsea_selected,
    "deepsea_storage_mb": deepsea["storage_used_mb"],
    "static_topk_selected_views": static_selected,
    "adaptive_intervals": 9,
    "summary": summary,
    "interval_results": interval_results,
}

with open(OUTPUT_FILE, "w") as f:
    json.dump(result, f, indent=2)

print("===== DEEPSEA vs WAMVS =====")
print()

for name, values in summary.items():
    print(
        f'{name:16s} '
        f'cost={values["weighted_cost_ms"]:.2f} ms | '
        f'speedup={values["speedup_vs_no_mv"]:.2f}x'
    )

print()
print(
    "DeepSea vs No-MV improvement:",
    f'{summary["DeepSea-style"]["improvement_vs_no_mv_percent"]:.2f}%'
)

print(
    "WAMVS vs No-MV improvement:",
    f'{summary["WAMVS"]["improvement_vs_no_mv_percent"]:.2f}%'
)

print(
    "WAMVS vs DeepSea improvement:",
    f'{summary["WAMVS"]["improvement_vs_deepsea_percent"]:.2f}%'
)

print()
print("DeepSea selected:", deepsea_selected)
print("DeepSea storage:", deepsea["storage_used_mb"], "MB")
print()
print(f"Saved: {OUTPUT_FILE}")

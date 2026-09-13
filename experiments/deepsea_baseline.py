import json
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
CANDIDATE_FILE = BASE_DIR / "workload" / "candidate_views.json"
METRICS_FILE = BASE_DIR / "workload" / "generic_materialization_metrics.json"
OUTPUT_FILE = BASE_DIR / "workload" / "deepsea_baseline_selection.json"

STORAGE_BUDGET_MB = 0.0200

with open(CANDIDATE_FILE) as f:
    candidates = json.load(f)

with open(METRICS_FILE) as f:
    metrics = json.load(f)

# Support both list- and dictionary-based metric files.
metric_by_id = {}

if isinstance(metrics, list):
    for item in metrics:
        candidate_id = item.get("candidate_id") or item.get("view_name")
        if candidate_id:
            metric_by_id[candidate_id] = item
elif isinstance(metrics, dict):
    for key, item in metrics.items():
        if isinstance(item, dict):
            candidate_id = item.get("candidate_id") or item.get("view_name") or key
            metric_by_id[candidate_id] = item

ranked = []

for candidate in candidates:
    candidate_id = candidate["candidate_id"]

    metric = metric_by_id.get(candidate_id, {})
    storage = float(metric.get("storage_mb", 0.0))
    frequency = float(candidate.get("frequency", 0))

    ranked.append({
        "candidate_id": candidate_id,
        "frequency": frequency,
        "storage_mb": storage,
        "tables": candidate["tables"],
        "group_by": candidate["group_by"],
        "source_queries": candidate.get("source_queries", [])
    })

# Simplified DeepSea-style policy:
# rank by workload frequency only.
ranked.sort(
    key=lambda x: (
        -x["frequency"],
        x["storage_mb"],
        x["candidate_id"]
    )
)

selected = []
used_storage = 0.0

for candidate in ranked:
    if used_storage + candidate["storage_mb"] <= STORAGE_BUDGET_MB:
        selected.append(candidate)
        used_storage += candidate["storage_mb"]

result = {
    "baseline": "simplified_deepsea_style",
    "selection_policy": "frequency_greedy",
    "adaptive": False,
    "storage_budget_mb": STORAGE_BUDGET_MB,
    "storage_used_mb": round(used_storage, 4),
    "selected_count": len(selected),
    "selected_views": selected,
    "ranking": ranked
}

with open(OUTPUT_FILE, "w") as f:
    json.dump(result, f, indent=2)

print("===== SIMPLIFIED DEEPSEA-STYLE BASELINE =====")
print(f"Storage budget : {STORAGE_BUDGET_MB:.4f} MB")
print(f"Storage used   : {used_storage:.4f} MB")
print(f"Selected views : {len(selected)}")
print()

for view in selected:
    print(
        f'{view["candidate_id"]}: '
        f'frequency={view["frequency"]:.0f}, '
        f'storage={view["storage_mb"]:.4f} MB, '
        f'queries={view["source_queries"]}'
    )

print()
print(f"Saved: {OUTPUT_FILE}")

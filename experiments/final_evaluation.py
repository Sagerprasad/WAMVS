from pathlib import Path
import json
import statistics

ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / "results"
WORKLOAD = ROOT / "workload"

def load(path):
    with open(path) as f:
        return json.load(f)

def as_query_dict(data):
    if isinstance(data, list):
        return {x["query_id"]: x for x in data}
    if isinstance(data, dict):
        if "results" in data and isinstance(data["results"], dict):
            return data["results"]
        return data
    return {}

no_mv = as_query_dict(load(RESULTS / "no_mv_baseline.json"))
static = as_query_dict(load(RESULTS / "static_topk_baseline.json"))

adaptive = load(WORKLOAD / "adaptive_experiment_replacement_final.json")

# Collect the best available MV benchmark for each query.
mv = {}

for filename in [
    "rewrite_benchmark.json",
    "missing_candidate_benchmark.json",
    "q08_benchmark.json",
    "q09_benchmark.json",
]:
    path = WORKLOAD / filename
    if not path.exists():
        continue

    data = load(path)

    if isinstance(data, list):
        items = data
    elif isinstance(data, dict) and "results" in data:
        items = list(data["results"].values())
    else:
        items = [data]

    for item in items:
        qid = item.get("query_id")
        if not qid:
            continue

        baseline = (
            item.get("baseline_median_ms")
            or item.get("no_mv_ms")
        )
        mv_time = (
            item.get("mv_median_ms")
            or item.get("static_topk_ms")
        )

        if baseline is not None and mv_time is not None:
            mv[qid] = {
                "mv_ms": mv_time,
                "speedup": baseline / mv_time if mv_time else None,
                "improvement_pct": (
                    (baseline - mv_time) / baseline * 100
                    if baseline else None
                ),
                "candidate_id": item.get("candidate_id"),
            }

print("=" * 80)
print("WAMVS FINAL EXPERIMENTAL EVALUATION")
print("=" * 80)

print("\nQUERY PERFORMANCE")
print("-" * 80)
print(
    f"{'Query':<8}"
    f"{'No-MV':>12}"
    f"{'Static':>12}"
    f"{'MV':>12}"
    f"{'MV Speedup':>14}"
    f"{'MV Improve':>14}"
)

comparison = []

for i in range(1, 11):
    qid = f"q{i:02d}"

    no_time = no_mv.get(qid, {}).get("median_ms")
    static_time = static.get(qid, {}).get("median_ms")
    mv_info = mv.get(qid, {})

    mv_time = mv_info.get("mv_ms")
    speedup = mv_info.get("speedup")
    improvement = mv_info.get("improvement_pct")

    print(
        f"{qid:<8}"
        f"{no_time if no_time is not None else 0:>12.2f}"
        f"{static_time if static_time is not None else 0:>12.2f}"
        f"{mv_time if mv_time is not None else 0:>12.2f}"
        f"{speedup if speedup is not None else 0:>14.2f}x"
        f"{improvement if improvement is not None else 0:>13.2f}%"
    )

    comparison.append({
        "query_id": qid,
        "no_mv_ms": no_time,
        "static_topk_ms": static_time,
        "mv_ms": mv_time,
        "mv_speedup": speedup,
        "mv_improvement_pct": improvement,
        "candidate_id": mv_info.get("candidate_id"),
    })

# Aggregate available MV measurements.
speedups = [
    x["mv_speedup"]
    for x in comparison
    if x["mv_speedup"] is not None
]

improvements = [
    x["mv_improvement_pct"]
    for x in comparison
    if x["mv_improvement_pct"] is not None
]

print("\n" + "=" * 80)
print("AGGREGATE MV PERFORMANCE")
print("=" * 80)

if speedups:
    print(f"Measured queries      : {len(speedups)}")
    print(f"Mean speedup          : {statistics.mean(speedups):.2f}x")
    print(f"Median speedup        : {statistics.median(speedups):.2f}x")

if improvements:
    print(f"Mean improvement      : {statistics.mean(improvements):.2f}%")
    print(f"Median improvement    : {statistics.median(improvements):.2f}%")

print("\n" + "=" * 80)
print("STATIC TOP-K")
print("=" * 80)

static_selected = load(
    RESULTS / "static_topk_baseline.json"
).get("selected_views", [])

print("Selected views:", static_selected)

static_storage = 0.0

metrics_path = WORKLOAD / "generic_materialization_metrics.json"

if metrics_path.exists():
    metrics = load(metrics_path)

    for item in metrics:
        if item.get("candidate_id") in static_selected:
            static_storage += item.get("storage_mb", 0.0)

print(f"Static storage: {static_storage:.4f} MB")

print("\n" + "=" * 80)
print("WAMVS ADAPTIVE TIMELINE")
print("=" * 80)

adaptive_rows = (
    adaptive
    if isinstance(adaptive, list)
    else adaptive.get("intervals", [])
)

for row in adaptive_rows:
    interval = row.get("interval")
    phase = row.get("phase", "")
    selected = row.get("selected_views", [])
    added = row.get("added_views", [])
    dropped = row.get("dropped_views", [])
    storage = row.get("storage_mb", 0)
    hit_rate = row.get("hit_rate_percent", 0)

    print(
        f"Interval {interval}: "
        f"{phase:<10} "
        f"selected={selected} "
        f"added={added} "
        f"dropped={dropped} "
        f"storage={storage:.4f} MB "
        f"hit-rate={hit_rate:.1f}%"
    )

# Build final JSON.
summary = {
    "query_comparison": comparison,
    "aggregate": {
        "measured_queries": len(speedups),
        "mean_speedup": statistics.mean(speedups) if speedups else None,
        "median_speedup": statistics.median(speedups) if speedups else None,
        "mean_improvement_pct": (
            statistics.mean(improvements) if improvements else None
        ),
        "median_improvement_pct": (
            statistics.median(improvements) if improvements else None
        ),
    },
    "static_topk": {
        "selected_views": static_selected,
        "storage_mb": static_storage,
    },
    "adaptive_timeline": adaptive_rows,
}

output = RESULTS / "final_evaluation_summary.json"
output.write_text(json.dumps(summary, indent=2))

print("\n" + "=" * 80)
print("FINAL EVALUATION COMPLETE")
print("=" * 80)
print(f"Output: {output}")

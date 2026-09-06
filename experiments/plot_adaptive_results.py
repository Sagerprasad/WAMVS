import json
from pathlib import Path
import matplotlib.pyplot as plt

ROOT = Path(__file__).resolve().parent.parent
RESULT = ROOT / "workload/adaptive_experiment_replacement_final.json"
OUT = ROOT / "results/figures"

with open(RESULT) as f:
    exp = json.load(f)

intervals = [r["interval"] for r in exp]

# ------------------------------------------------------------
# 1. Workload drift
# ------------------------------------------------------------

queries = sorted({
    q
    for r in exp
    for q in r.get("decayed_frequency", {})
})

plt.figure(figsize=(10, 6))

for q in queries:
    values = [
        r.get("decayed_frequency", {}).get(q, 0)
        for r in exp
    ]
    plt.plot(intervals, values, marker="o", label=q)

plt.xlabel("Workload Interval")
plt.ylabel("Decayed Frequency")
plt.title("Workload Drift Across Adaptive Intervals")
plt.xticks(intervals)
plt.grid(True, alpha=0.3)
plt.legend(ncol=2)
plt.tight_layout()
plt.savefig(OUT / "workload_drift.png", dpi=300)
plt.close()


# ------------------------------------------------------------
# 2. MV selection timeline
# ------------------------------------------------------------

mvs = sorted({
    mv
    for r in exp
    for mv in r.get("selected_views", [])
})

plt.figure(figsize=(10, 6))

for mv in mvs:
    values = [
        1 if mv in r.get("selected_views", []) else 0
        for r in exp
    ]
    plt.plot(
        intervals,
        values,
        marker="o",
        linewidth=2,
        label=mv
    )

plt.xlabel("Workload Interval")
plt.ylabel("Selected")
plt.title("Adaptive Materialized View Selection")
plt.yticks([0, 1], ["Not Selected", "Selected"])
plt.xticks(intervals)
plt.grid(True, alpha=0.3)
plt.legend()
plt.tight_layout()
plt.savefig(OUT / "mv_selection_timeline.png", dpi=300)
plt.close()


# ------------------------------------------------------------
# 3. Storage utilization
# ------------------------------------------------------------

budget = 0.0200

storage = [
    r.get("storage_mb", 0)
    for r in exp
]

plt.figure(figsize=(10, 6))

plt.plot(
    intervals,
    storage,
    marker="o",
    linewidth=2,
    label="Selected MV Storage"
)

plt.axhline(
    budget,
    linestyle="--",
    linewidth=2,
    label="Storage Budget"
)

plt.xlabel("Workload Interval")
plt.ylabel("Storage (MB)")
plt.title("Storage Budget Utilization")
plt.xticks(intervals)
plt.grid(True, alpha=0.3)
plt.legend()
plt.tight_layout()
plt.savefig(OUT / "storage_utilization.png", dpi=300)
plt.close()


# ------------------------------------------------------------
# 4. Adaptive workload benefit
# ------------------------------------------------------------

benchmarks = {}

files = [
    ("workload/rewrite_benchmark.json", "view"),
    ("workload/missing_candidate_benchmark.json", "candidate_id"),
    ("workload/q08_benchmark.json", "candidate_id"),
    ("workload/q09_benchmark.json", "candidate_id"),
]

for filename, view_key in files:

    path = ROOT / filename

    if not path.exists():
        continue

    with open(path) as f:
        data = json.load(f)

    rows = data if isinstance(data, list) else [data]

    for row in rows:

        correct = row.get(
            "result_match",
            row.get("correct", False)
        )

        if not correct:
            continue

        query = row["query_id"]
        view = row[view_key]

        saved = (
            float(row["baseline_median_ms"])
            - float(row["mv_median_ms"])
        )

        benchmarks[query] = {
            "view": view,
            "saved_ms": max(0.0, saved)
        }


benefits = []

for record in exp:

    frequencies = record.get("decayed_frequency", {})
    selected = set(record.get("selected_views", []))

    total = 0.0

    for query, measurement in benchmarks.items():

        if measurement["view"] not in selected:
            continue

        frequency = float(
            frequencies.get(query, 0)
        )

        total += (
            frequency
            * measurement["saved_ms"]
        )

    benefits.append(total)


plt.figure(figsize=(10, 6))

plt.plot(
    intervals,
    benefits,
    marker="o",
    linewidth=2
)

plt.xlabel("Workload Interval")
plt.ylabel("Workload-Weighted Benefit (ms)")
plt.title("Adaptive Workload Benefit")
plt.xticks(intervals)
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig(OUT / "adaptive_benefit.png", dpi=300)
plt.close()


# ------------------------------------------------------------
# Summary
# ------------------------------------------------------------

print("=" * 70)
print("WAMVS ADAPTIVE RESULT FIGURES")
print("=" * 70)

for p in sorted(OUT.glob("*.png")):
    print("Created:", p.relative_to(ROOT))

print()
print("Intervals :", len(exp))
print(f"Budget    : {budget:.4f} MB")
print(f"Final     : {storage[-1]:.4f} MB")
print(f"Benefit   : {benefits[-1]:.2f} ms")
print("=" * 70)

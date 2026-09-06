from pathlib import Path
import json

import matplotlib.pyplot as plt

ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / "results"
FIGURES = RESULTS / "figures"

FIGURES.mkdir(parents=True, exist_ok=True)

data = json.loads(
    (RESULTS / "controlled_static_vs_wamvs.json").read_text()
)

summary = data["summary"]
static = data["static_topk"]
wamvs = data["wamvs"]

# ============================================================
# 1. TOTAL WORKLOAD-WEIGHTED COST
# ============================================================

methods = ["No-MV", "Static Top-K", "WAMVS"]
costs = [
    summary["no_mv_total_ms"],
    summary["static_topk_total_ms"],
    summary["wamvs_total_ms"],
]

plt.figure(figsize=(12, 7))
plt.bar(methods, costs)
plt.ylabel("Workload-weighted execution cost (ms)")
plt.title("Workload-Weighted Execution Cost")
plt.grid(axis="y", alpha=0.25)
plt.tight_layout()
plt.savefig(
    FIGURES / "workload_weighted_cost.png",
    dpi=300,
    bbox_inches="tight",
)
plt.close()

# ============================================================
# 2. SPEEDUP
# ============================================================

speedups = [
    1.0,
    summary["static_speedup_vs_no_mv"],
    summary["wamvs_speedup_vs_no_mv"],
]

plt.figure(figsize=(12, 7))
plt.bar(methods, speedups)
plt.ylabel("Speedup relative to No-MV")
plt.title("Performance Improvement over No-MV")
plt.grid(axis="y", alpha=0.25)
plt.tight_layout()
plt.savefig(
    FIGURES / "overall_speedup.png",
    dpi=300,
    bbox_inches="tight",
)
plt.close()

# ============================================================
# 3. INTERVAL COST: STATIC VS WAMVS
# ============================================================

intervals = [x["interval"] for x in static]
static_cost = [x["static_topk_cost_ms"] for x in static]
wamvs_cost = [x["wamvs_cost_ms"] for x in wamvs]

plt.figure(figsize=(13, 7))
plt.plot(
    intervals,
    static_cost,
    marker="o",
    label="Static Top-K",
)
plt.plot(
    intervals,
    wamvs_cost,
    marker="o",
    label="WAMVS",
)

plt.axvline(
    3.5,
    linestyle="--",
    alpha=0.6,
    label="Drift begins",
)

plt.axvline(
    6.5,
    linestyle="--",
    alpha=0.6,
    label="Bursty phase begins",
)

plt.xlabel("Workload interval")
plt.ylabel("Workload-weighted cost (ms)")
plt.title("Static Top-K vs WAMVS Across Workload Drift")
plt.legend()
plt.grid(alpha=0.25)
plt.tight_layout()
plt.savefig(
    FIGURES / "interval_cost_comparison.png",
    dpi=300,
    bbox_inches="tight",
)
plt.close()

# ============================================================
# 4. ADAPTIVE HIT RATE
# ============================================================

hit_rates = [x["hit_rate_percent"] for x in wamvs]
phases = [x["phase"] for x in wamvs]

plt.figure(figsize=(13, 7))
plt.plot(
    intervals,
    hit_rates,
    marker="o",
)

plt.xlabel("Workload interval")
plt.ylabel("Query hit rate (%)")
plt.title("WAMVS Query Hit Rate Across Workload Drift")
plt.ylim(0, 105)
plt.grid(alpha=0.25)
plt.tight_layout()
plt.savefig(
    FIGURES / "adaptive_hit_rate.png",
    dpi=300,
    bbox_inches="tight",
)
plt.close()

print("=" * 70)
print("FINAL WAMVS FIGURES GENERATED")
print("=" * 70)

for name in [
    "workload_weighted_cost.png",
    "overall_speedup.png",
    "interval_cost_comparison.png",
    "adaptive_hit_rate.png",
]:
    path = FIGURES / name
    print(f"{name:<35} {path.stat().st_size / 1024:.1f} KB")

print(f"\nOutput directory: {FIGURES}")

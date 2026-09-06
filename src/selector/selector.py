from pathlib import Path
import json


# ============================================================
# WAMVS PERFORMANCE-AWARE VIEW SELECTOR
# ============================================================

CANDIDATE_FILE = Path("workload/candidate_views.json")
METRICS_FILE = Path("workload/generic_materialization_metrics.json")
FREQUENCY_FILE = Path("workload/adaptive_frequency.json")
OLD_BENCHMARK_FILE = Path("workload/rewrite_benchmark.json")
NEW_BENCHMARK_FILE = Path("workload/missing_candidate_benchmark.json")
Q09_BENCHMARK_FILE = Path("workload/q09_benchmark.json")
Q08_BENCHMARK_FILE = Path("workload/q08_benchmark.json")

OUTPUT_FILE = Path("workload/selected_views.json")

STORAGE_BUDGET_MB = 0.0200


# ------------------------------------------------------------
# Load JSON
# ------------------------------------------------------------

def load_json(path):
    with open(path) as f:
        return json.load(f)


# ------------------------------------------------------------
# Query performance measurements
# ------------------------------------------------------------

def load_query_benefits():

    benefits = {}

    # Existing benchmark
    old = load_json(OLD_BENCHMARK_FILE)

    for row in old:

        if row.get("result_match") is not True:
            continue

        query_id = row["query_id"]
        view_id = row["view"]

        baseline = row["baseline_median_ms"]
        mv = row["mv_median_ms"]

        benefits[query_id] = {
            "view": view_id,
            "baseline_ms": baseline,
            "mv_ms": mv,
            "saved_ms": max(0.0, baseline - mv),
        }

    # Q02, Q06, Q10 benchmark
    new = load_json(NEW_BENCHMARK_FILE)

    for row in new:

        if row.get("correct") is not True:
            continue

        query_id = row["query_id"]
        view_id = row["candidate_id"]

        baseline = row["baseline_median_ms"]
        mv = row["mv_median_ms"]

        benefits[query_id] = {
            "view": view_id,
            "baseline_ms": baseline,
            "mv_ms": mv,
            "saved_ms": max(0.0, baseline - mv),
        }

    # Q09 benchmark
    if Q09_BENCHMARK_FILE.exists():

        row = load_json(Q09_BENCHMARK_FILE)

        if row.get("correct") is True:

            query_id = row["query_id"]
            view_id = row["candidate_id"]

            baseline = row["baseline_median_ms"]
            mv = row["mv_median_ms"]

            benefits[query_id] = {
                "view": view_id,
                "baseline_ms": baseline,
                "mv_ms": mv,
                "saved_ms": max(
                    0.0,
                    baseline - mv
                ),
            }

    # Q08 benchmark
    if Q08_BENCHMARK_FILE.exists():

        row = load_json(Q08_BENCHMARK_FILE)

        if row.get("correct") is True:

            query_id = row["query_id"]
            view_id = row["candidate_id"]

            baseline = row["baseline_median_ms"]
            mv = row["mv_median_ms"]

            benefits[query_id] = {
                "view": view_id,
                "baseline_ms": baseline,
                "mv_ms": mv,
                "saved_ms": max(
                    0.0,
                    baseline - mv
                ),
            }


    return benefits


# ------------------------------------------------------------
# Workload frequencies
#
# Use the latest decayed frequency state.
# ------------------------------------------------------------

def load_latest_frequencies():

    data = load_json(FREQUENCY_FILE)

    # Expected format:
    # [
    #   {"interval": 1, "frequencies": {...}},
    #   ...
    # ]

    if isinstance(data, list):

        latest = data[-1]

        if "frequencies" in latest:
            return latest["frequencies"]

        if "scores" in latest:
            return latest["scores"]

    if isinstance(data, dict):

        if "frequencies" in data:
            value = data["frequencies"]

            if isinstance(value, list):
                return value[-1]

            return value

        if "scores" in data:
            value = data["scores"]

            if isinstance(value, list):
                return value[-1]

            return value

    raise ValueError(
        "Unsupported adaptive_frequency.json format"
    )


# ------------------------------------------------------------
# 0/1 Knapsack
#
# Storage is converted to integer micro-MB so that we can
# perform exact discrete selection without floating-point
# errors.
# ------------------------------------------------------------

def knapsack(candidates, budget_mb):

    SCALE = 1_000_000

    budget = int(round(budget_mb * SCALE))

    n = len(candidates)

    dp = [0.0] * (budget + 1)
    selected = [None] * (budget + 1)

    for i, candidate in enumerate(candidates):

        weight = int(
            round(candidate["storage_mb"] * SCALE)
        )

        value = candidate["benefit_ms"]

        if weight > budget:
            continue

        for b in range(budget, weight - 1, -1):

            candidate_value = (
                dp[b - weight] + value
            )

            if candidate_value > dp[b]:

                dp[b] = candidate_value

                previous = (
                    selected[b - weight]
                    if selected[b - weight] is not None
                    else []
                )

                selected[b] = (
                    previous + [i]
                )

    indexes = selected[budget] or []

    return [candidates[i] for i in indexes]


# ============================================================
# MAIN
# ============================================================

def main():

    candidates = load_json(CANDIDATE_FILE)
    metrics = load_json(METRICS_FILE)

    frequencies = load_latest_frequencies()
    query_benefits = load_query_benefits()

    metric_by_id = {
        m["candidate_id"]: m
        for m in metrics
    }

    scored = []

    for candidate in candidates:

        candidate_id = candidate["candidate_id"]

        metric = metric_by_id.get(candidate_id)

        if metric is None:
            print(
                f"WARNING: no materialization metrics "
                f"for {candidate_id}"
            )
            continue

        storage_mb = metric["storage_mb"]

        total_benefit = 0.0
        query_details = []

        for query_id in candidate["source_queries"]:

            measurement = query_benefits.get(query_id)

            if measurement is None:
                continue

            frequency_missing = query_id not in frequencies

            frequency = float(
                frequencies.get(query_id, 0)
            )

            saved_ms = measurement["saved_ms"]

            weighted_benefit = (
                frequency * saved_ms
            )

            total_benefit += weighted_benefit

            query_details.append({
                "query_id": query_id,
                "frequency": round(frequency, 4),
                "frequency_status": (
                    "unobserved"
                    if frequency_missing
                    else "observed"
                ),
                "saved_ms": round(saved_ms, 2),
                "weighted_benefit_ms": round(
                    weighted_benefit,
                    2
                )
            })

        if storage_mb > 0:
            utility = (
                total_benefit / storage_mb
            )
        else:
            utility = 0.0

        scored.append({
            **candidate,
            "storage_mb": storage_mb,
            "materialization_time_ms":
                metric.get("materialization_time_ms", 0),
            "benefit_ms": round(
                total_benefit,
                4
            ),
            "utility": round(
                utility,
                4
            ),
            "query_benefits": query_details
        })

    # --------------------------------------------------------
    # Sort for display
    # --------------------------------------------------------

    ranked = sorted(
        scored,
        key=lambda x: x["utility"],
        reverse=True
    )

    print("=" * 75)
    print("WAMVS PERFORMANCE-AWARE VIEW SELECTOR")
    print("=" * 75)

    print(
        f"\nStorage budget : "
        f"{STORAGE_BUDGET_MB:.4f} MB"
    )

    print(
        "\nObjective:"
    )

    print(
        "  Benefit = frequency × measured query time saved"
    )

    print(
        "  Utility = Benefit / storage"
    )

    print("\nCandidate ranking:")
    print("-" * 75)

    for rank, candidate in enumerate(
        ranked,
        start=1
    ):

        print(
            f"{rank:2}. "
            f"{candidate['candidate_id']:<7} | "
            f"benefit={candidate['benefit_ms']:>10.2f} ms | "
            f"storage={candidate['storage_mb']:>7.4f} MB | "
            f"utility={candidate['utility']:>10.2f}"
        )

    # --------------------------------------------------------
    # Exact budget selection
    # --------------------------------------------------------

    selected = knapsack(
        scored,
        STORAGE_BUDGET_MB
    )

    selected_ids = {
        c["candidate_id"]
        for c in selected
    }

    selected = sorted(
        selected,
        key=lambda x: x["utility"],
        reverse=True
    )

    total_storage = sum(
        c["storage_mb"]
        for c in selected
    )

    total_benefit = sum(
        c["benefit_ms"]
        for c in selected
    )

    print("\n" + "=" * 75)
    print("SELECTED MATERIALIZED VIEWS")
    print("=" * 75)

    for candidate in selected:

        print(
            f"\n{candidate['candidate_id']}"
        )

        print(
            f"  Queries     : "
            f"{', '.join(candidate['source_queries'])}"
        )

        print(
            f"  Benefit     : "
            f"{candidate['benefit_ms']:.2f} ms"
        )

        print(
            f"  Storage     : "
            f"{candidate['storage_mb']:.4f} MB"
        )

        print(
            f"  Utility     : "
            f"{candidate['utility']:.2f}"
        )

        print(
            f"  Build time  : "
            f"{candidate['materialization_time_ms']:.2f} ms"
        )

    print("\n" + "-" * 75)

    print(
        f"Views selected : "
        f"{len(selected)}/{len(scored)}"
    )

    print(
        f"Storage used   : "
        f"{total_storage:.4f} MB"
    )

    print(
        f"Storage budget : "
        f"{STORAGE_BUDGET_MB:.4f} MB"
    )

    print(
        f"Remaining      : "
        f"{STORAGE_BUDGET_MB - total_storage:.4f} MB"
    )

    print(
        f"Total benefit  : "
        f"{total_benefit:.2f} ms"
    )

    # --------------------------------------------------------
    # Output
    # --------------------------------------------------------

    output = []

    for candidate in selected:

        output.append({
            "candidate_id":
                candidate["candidate_id"],
            "source_queries":
                candidate["source_queries"],
            "tables":
                candidate["tables"],
            "group_by":
                candidate["group_by"],
            "measures":
                candidate["measures"],
            "storage_mb":
                candidate["storage_mb"],
            "materialization_time_ms":
                candidate["materialization_time_ms"],
            "benefit_ms":
                candidate["benefit_ms"],
            "utility":
                candidate["utility"],
            "query_benefits":
                candidate["query_benefits"]
        })

    OUTPUT_FILE.write_text(
        json.dumps(
            output,
            indent=2
        )
    )

    print(
        f"\nOutput         : "
        f"{OUTPUT_FILE}"
    )

    print("=" * 75)


if __name__ == "__main__":
    main()

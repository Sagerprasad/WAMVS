import json
from pathlib import Path
from itertools import combinations

BUDGET_MB = 0.0200
DRIFT_FILE = Path("workload/drift_workload.json")
CANDIDATE_FILE = Path("workload/candidate_views.json")
METRICS_FILE = Path("workload/generic_materialization_metrics.json")
ADAPTIVE_FILE = Path("workload/adaptive_frequency.json")

REWRITE_FILE = Path("workload/rewrite_benchmark.json")
MISSING_FILE = Path("workload/missing_candidate_benchmark.json")
Q08_FILE = Path("workload/q08_benchmark.json")
Q09_FILE = Path("workload/q09_benchmark.json")

OUTPUT_FILE = Path("results/ablation_study.json")


def load(path):
    with open(path) as f:
        return json.load(f)


def build_query_costs():
    costs = {}

    for row in load(REWRITE_FILE):
        if row.get("result_match") is True:
            costs[row["query_id"]] = {
                "baseline": float(row["baseline_median_ms"]),
                "mv": float(row["mv_median_ms"]),
                "view": row["view"],
            }

    for row in load(MISSING_FILE):
        if row.get("correct") is True:
            costs[row["query_id"]] = {
                "baseline": float(row["baseline_median_ms"]),
                "mv": float(row["mv_median_ms"]),
                "view": row["candidate_id"],
            }

    for path in [Q08_FILE, Q09_FILE]:
        if path.exists():
            row = load(path)
            if row.get("correct") is True:
                costs[row["query_id"]] = {
                    "baseline": float(row["baseline_median_ms"]),
                    "mv": float(row["mv_median_ms"]),
                    "view": row["candidate_id"],
                }

    for q in costs:
        costs[q]["saving"] = max(
            0.0,
            costs[q]["baseline"] - costs[q]["mv"]
        )

    return costs


def build_candidates():
    candidates = load(CANDIDATE_FILE)
    metrics = load(METRICS_FILE)

    metric_map = {
        m["candidate_id"]: m
        for m in metrics
    }

    result = {}

    for c in candidates:
        cid = c["candidate_id"]

        if cid not in metric_map:
            continue

        result[cid] = {
            "candidate_id": cid,
            "source_queries": c["source_queries"],
            "storage_mb": float(
                metric_map[cid]["storage_mb"]
            ),
        }

    return result


def score_candidates(candidates, costs, frequencies):
    scored = {}

    for cid, candidate in candidates.items():
        benefit = 0.0

        for qid in candidate["source_queries"]:
            if qid not in costs:
                continue

            frequency = float(
                frequencies.get(qid, 0.0)
            )

            benefit += (
                frequency *
                costs[qid]["saving"]
            )

        storage = candidate["storage_mb"]

        utility = (
            benefit / storage
            if storage > 0
            else 0.0
        )

        scored[cid] = {
            **candidate,
            "benefit": benefit,
            "utility": utility,
        }

    return scored


def exact_knapsack(scored):
    items = list(scored.values())

    best = []
    best_benefit = -1.0
    best_storage = 0.0

    for r in range(len(items) + 1):
        for combo in combinations(items, r):
            storage = sum(
                x["storage_mb"] for x in combo
            )

            if storage > BUDGET_MB + 1e-12:
                continue

            benefit = sum(
                x["benefit"] for x in combo
            )

            if benefit > best_benefit:
                best = list(combo)
                best_benefit = benefit
                best_storage = storage

    return [x["candidate_id"] for x in best], best_storage


def top_k_frequency(scored, k):
    ranked = sorted(
        scored.values(),
        key=lambda x: x["benefit"],
        reverse=True,
    )

    selected = ranked[:k]

    return (
        [x["candidate_id"] for x in selected],
        sum(x["storage_mb"] for x in selected),
    )


def initial_frequency(drift):
    return {
        qid: float(freq)
        for qid, freq in drift[0]["query_counts"].items()
    }


def run_adaptive_selection(
    drift,
    candidates,
    costs,
    decay=0.70,
):
    frequencies = {}
    selections = []

    for interval in drift:
        counts = interval["query_counts"]

        updated = {}

        all_queries = set(frequencies) | set(counts)

        for qid in all_queries:
            updated[qid] = (
                decay * frequencies.get(qid, 0.0)
                + float(counts.get(qid, 0))
            )

        frequencies = updated

        scored = score_candidates(
            candidates,
            costs,
            frequencies,
        )

        selected, storage = exact_knapsack(scored)

        selections.append({
            "interval": interval["interval"],
            "phase": interval["phase"],
            "selected": selected,
            "storage_mb": storage,
            "frequencies": dict(frequencies),
        })

    return selections


def run_static_selection(
    drift,
    candidates,
    costs,
):
    frequencies = initial_frequency(drift)

    scored = score_candidates(
        candidates,
        costs,
        frequencies,
    )

    selected, storage = exact_knapsack(scored)

    return [
        {
            "interval": interval["interval"],
            "phase": interval["phase"],
            "selected": selected,
            "storage_mb": storage,
        }
        for interval in drift
    ]


def run_frequency_only(
    drift,
    candidates,
    costs,
    decay=0.70,
):
    frequencies = {}
    selections = []

    for interval in drift:
        counts = interval["query_counts"]

        all_queries = set(frequencies) | set(counts)

        updated = {}

        for qid in all_queries:
            updated[qid] = (
                decay * frequencies.get(qid, 0.0)
                + float(counts.get(qid, 0))
            )

        frequencies = updated

        scored = score_candidates(
            candidates,
            costs,
            frequencies,
        )

        # Full WAMVS selects four views in every interval.
        # Keeping K=4 isolates the effect of removing
        # storage awareness rather than changing view count.
        selected, storage = top_k_frequency(
            scored,
            k=4,
        )

        selections.append({
            "interval": interval["interval"],
            "phase": interval["phase"],
            "selected": selected,
            "storage_mb": storage,
            "frequencies": dict(frequencies),
        })

    return selections


def serves_query(qid, selected, costs):
    if qid not in costs:
        return False

    required_view = costs[qid]["view"]

    return required_view in selected


def evaluate_variant(
    name,
    drift,
    selections,
    costs,
):
    total_no_mv = 0.0
    total_variant = 0.0
    total_queries = 0
    total_hits = 0

    interval_results = []

    for interval, selection in zip(
        drift,
        selections,
    ):
        no_mv = 0.0
        variant = 0.0
        hits = 0
        queries = 0

        for qid, frequency in interval[
            "query_counts"
        ].items():

            if qid not in costs:
                continue

            frequency = int(frequency)

            baseline = costs[qid]["baseline"]
            mv = costs[qid]["mv"]

            queries += frequency
            total_queries += frequency

            no_mv += frequency * baseline
            total_no_mv += frequency * baseline

            if serves_query(
                qid,
                selection["selected"],
                costs,
            ):
                variant += frequency * mv
                total_hits += frequency
                hits += frequency
            else:
                variant += frequency * baseline

            total_variant += (
                frequency * (
                    mv
                    if serves_query(
                        qid,
                        selection["selected"],
                        costs,
                    )
                    else baseline
                )
            )

        interval_results.append({
            "interval": interval["interval"],
            "phase": interval["phase"],
            "queries": queries,
            "selected_views": selection["selected"],
            "storage_mb": round(
                selection["storage_mb"],
                4,
            ),
            "no_mv_cost_ms": round(no_mv, 2),
            "variant_cost_ms": round(
                variant,
                2,
            ),
            "speedup_vs_no_mv": round(
                no_mv / variant
                if variant > 0
                else 0,
                3,
            ),
            "hit_rate_percent": round(
                hits / queries * 100
                if queries > 0
                else 0,
                2,
            ),
        })

    return {
        "name": name,
        "total_cost_ms": round(
            total_variant,
            2,
        ),
        "no_mv_cost_ms": round(
            total_no_mv,
            2,
        ),
        "speedup_vs_no_mv": round(
            total_no_mv / total_variant,
            3,
        ),
        "improvement_vs_no_mv_percent": round(
            (
                (total_no_mv - total_variant)
                / total_no_mv
                * 100
            ),
            2,
        ),
        "overall_hit_rate_percent": round(
            total_hits / total_queries * 100,
            2,
        ),
        "mean_storage_mb": round(
            sum(
                x["storage_mb"]
                for x in interval_results
            ) / len(interval_results),
            4,
        ),
        "intervals": interval_results,
    }


def main():
    print("=" * 80)
    print("WAMVS ABLATION STUDY")
    print("=" * 80)

    drift = load(DRIFT_FILE)
    candidates = build_candidates()
    costs = build_query_costs()

    print(f"Intervals: {len(drift)}")
    print(f"Candidates: {len(candidates)}")
    print(f"Measured queries: {len(costs)}")
    print(f"Storage budget: {BUDGET_MB:.4f} MB")
    print()

    # ------------------------------------------------------------
    # 1. Full WAMVS
    # ------------------------------------------------------------
    full_selection = run_adaptive_selection(
        drift,
        candidates,
        costs,
    )

    # ------------------------------------------------------------
    # 2. No workload adaptation
    # ------------------------------------------------------------
    static_selection = run_static_selection(
        drift,
        candidates,
        costs,
    )

    # ------------------------------------------------------------
    # 3. No storage awareness
    # ------------------------------------------------------------
    frequency_only_selection = run_frequency_only(
        drift,
        candidates,
        costs,
    )

    results = {
        "full_wamvs": evaluate_variant(
            "Full WAMVS",
            drift,
            full_selection,
            costs,
        ),
        "no_adaptation": evaluate_variant(
            "No workload adaptation",
            drift,
            static_selection,
            costs,
        ),
        "no_storage_awareness": evaluate_variant(
            "No storage awareness",
            drift,
            frequency_only_selection,
            costs,
        ),
    }

    # ------------------------------------------------------------
    # Print summary
    # ------------------------------------------------------------
    print("=" * 80)
    print("ABLATION SUMMARY")
    print("=" * 80)

    for key, result in results.items():
        print()
        print(result["name"])
        print("-" * 80)
        print(
            f"Total cost       : "
            f"{result['total_cost_ms']:.2f} ms"
        )
        print(
            f"Speedup          : "
            f"{result['speedup_vs_no_mv']:.2f}x"
        )
        print(
            f"Improvement      : "
            f"{result['improvement_vs_no_mv_percent']:.2f}%"
        )
        print(
            f"Hit rate         : "
            f"{result['overall_hit_rate_percent']:.2f}%"
        )
        print(
            f"Mean storage     : "
            f"{result['mean_storage_mb']:.4f} MB"
        )

    print()
    print("=" * 80)
    print("VIEW-SELECTION DIFFERENCES")
    print("=" * 80)

    for i in range(len(drift)):
        print(
            f"\nInterval {i + 1} "
            f"({drift[i]['phase']}):"
        )

        print(
            "  Full WAMVS       : "
            + ", ".join(
                full_selection[i]["selected"]
            )
        )

        print(
            "  No adaptation    : "
            + ", ".join(
                static_selection[i]["selected"]
            )
        )

        print(
            "  No storage-aware : "
            + ", ".join(
                frequency_only_selection[i]["selected"]
            )
        )

    output = {
        "methodology": {
            "description": (
                "Controlled ablation using the same "
                "nine-interval stable/drifting/bursty "
                "workload and measured median query costs."
            ),
            "budget_mb": BUDGET_MB,
            "decay_factor": 0.70,
            "frequency_only_top_k": 4,
            "variants": [
                "full_wamvs",
                "no_adaptation",
                "no_storage_awareness",
            ],
        },
        "results": results,
    }

    OUTPUT_FILE.parent.mkdir(exist_ok=True)

    with open(OUTPUT_FILE, "w") as f:
        json.dump(
            output,
            f,
            indent=2,
        )

    print()
    print("=" * 80)
    print(f"Saved: {OUTPUT_FILE}")
    print("=" * 80)


if __name__ == "__main__":
    main()

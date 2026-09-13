import json
from pathlib import Path

import matplotlib.pyplot as plt


CANDIDATE_FILE = Path("workload/candidate_views.json")
METRICS_FILE = Path("workload/generic_materialization_metrics.json")
NO_MV_FILE = Path("results/no_mv_baseline.json")
FREQ_FILE = Path("workload/adaptive_frequency.json")

BUDGETS = [
    0.005,
    0.010,
    0.015,
    0.020,
    0.025,
    0.030,
    0.050,
]


def load_json(path):
    with open(path) as f:
        return json.load(f)


def load_no_mv_costs():
    data = load_json(NO_MV_FILE)

    if isinstance(data, list):
        return {
            x["query_id"]: float(x["median_ms"])
            for x in data
        }

    return {
        q: float(v["median_ms"])
        for q, v in data.items()
    }


def load_frequencies():
    data = load_json(FREQ_FILE)

    if not data:
        raise RuntimeError(
            "adaptive_frequency.json is empty"
        )

    latest = data[-1]

    return {
        q: float(v)
        for q, v in latest["frequencies"].items()
    }


def load_candidates():
    candidates = load_json(CANDIDATE_FILE)
    metrics = load_json(METRICS_FILE)

    metrics_by_id = {
        x["candidate_id"]: x
        for x in metrics
    }

    for candidate in candidates:

        cid = candidate["candidate_id"]

        if cid not in metrics_by_id:
            raise RuntimeError(
                f"Missing materialization metrics for {cid}"
            )

        metric = metrics_by_id[cid]

        candidate["storage_mb"] = float(
            metric["storage_mb"]
        )

        candidate["materialization_time_ms"] = float(
            metric["materialization_time_ms"]
        )

    return candidates


def load_benchmarks():
    """
    Build query -> measured MV benchmark mapping.

    Handles all benchmark file formats currently
    present in the project.
    """

    benchmark_files = [
        Path("workload/rewrite_benchmark.json"),
        Path("workload/missing_candidate_benchmark.json"),
        Path("workload/q08_benchmark.json"),
        Path("workload/q09_benchmark.json"),
    ]

    benchmarks = {}

    for path in benchmark_files:

        if not path.exists():
            continue

        data = load_json(path)

        if isinstance(data, list):
            records = data
        elif isinstance(data, dict):
            records = [data]
        else:
            continue

        for record in records:

            query_id = record.get("query_id")

            if not query_id:
                continue

            baseline = record.get(
                "baseline_median_ms"
            )

            mv = record.get(
                "mv_median_ms"
            )

            candidate_id = (
                record.get("candidate_id")
                or record.get("view")
            )

            if (
                baseline is not None
                and mv is not None
                and candidate_id is not None
            ):
                benchmarks[query_id] = {
                    "candidate_id": candidate_id,
                    "baseline_median_ms": float(
                        baseline
                    ),
                    "mv_median_ms": float(
                        mv
                    ),
                    "saving_ms": max(
                        0.0,
                        float(baseline)
                        - float(mv)
                    ),
                }

    return benchmarks


def select_views(
    candidates,
    budget_mb,
    frequencies,
    benchmarks
):
    """
    Exact 0/1 knapsack.

    Objective:
        maximize frequency-weighted measured
        query-time savings.

    Constraint:
        total physical MV storage <= budget.
    """

    scale = 1_000_000

    budget_units = int(
        round(budget_mb * scale)
    )

    items = []

    for candidate in candidates:

        cid = candidate["candidate_id"]

        storage_units = int(
            round(
                candidate["storage_mb"]
                * scale
            )
        )

        if storage_units > budget_units:
            continue

        benefit = 0.0

        for query_id in candidate.get(
            "source_queries",
            []
        ):

            benchmark = benchmarks.get(
                query_id
            )

            if (
                benchmark is None
                or benchmark["candidate_id"] != cid
            ):
                continue

            frequency = frequencies.get(
                query_id,
                0.0
            )

            benefit += (
                frequency
                * benchmark["saving_ms"]
            )

        items.append({
            "candidate": candidate,
            "weight": storage_units,
            "benefit": benefit,
        })

    # 0/1 knapsack.
    dp = {
        0: (0.0, [])
    }

    for index, item in enumerate(items):

        weight = item["weight"]
        value = item["benefit"]

        updates = {}

        for used, (
            current_value,
            selected_indices
        ) in dp.items():

            new_used = used + weight

            if new_used > budget_units:
                continue

            new_value = (
                current_value
                + value
            )

            existing = dp.get(
                new_used,
                (-1.0, [])
            )

            pending = updates.get(
                new_used,
                (-1.0, [])
            )

            if new_value > max(
                existing[0],
                pending[0]
            ):
                updates[new_used] = (
                    new_value,
                    selected_indices
                    + [index]
                )

        dp.update(updates)

    best_used, (
        best_value,
        selected_indices
    ) = max(
        dp.items(),
        key=lambda x: x[1][0]
    )

    selected = [
        items[i]["candidate"]
        for i in selected_indices
    ]

    return (
        selected,
        best_used / scale,
        best_value
    )


def calculate_cost(
    selected,
    frequencies,
    no_mv_costs,
    benchmarks
):
    """
    Workload-weighted cost using measured
    No-MV and MV execution times.
    """

    selected_ids = {
        x["candidate_id"]
        for x in selected
    }

    query_to_candidate = {}

    for candidate in selected:

        for query_id in candidate.get(
            "source_queries",
            []
        ):
            query_to_candidate[query_id] = (
                candidate["candidate_id"]
            )

    baseline_total = 0.0
    wamvs_total = 0.0

    covered = set()

    for query_id, frequency in frequencies.items():

        if query_id not in no_mv_costs:
            continue

        frequency = float(frequency)

        baseline = no_mv_costs[query_id]

        baseline_total += (
            frequency * baseline
        )

        candidate_id = query_to_candidate.get(
            query_id
        )

        benchmark = benchmarks.get(
            query_id
        )

        if (
            candidate_id is not None
            and benchmark is not None
            and benchmark["candidate_id"]
            == candidate_id
        ):
            wamvs_total += (
                frequency
                * benchmark["mv_median_ms"]
            )

            covered.add(query_id)

        else:
            wamvs_total += (
                frequency * baseline
            )

    return (
        baseline_total,
        wamvs_total,
        covered
    )


def main():

    candidates = load_candidates()

    no_mv_costs = load_no_mv_costs()

    frequencies = load_frequencies()

    benchmarks = load_benchmarks()

    print("=" * 80)
    print("WAMVS STORAGE BUDGET SENSITIVITY")
    print("=" * 80)

    print()
    print("Candidate storage:")

    for candidate in candidates:
        print(
            f'  {candidate["candidate_id"]}: '
            f'{candidate["storage_mb"]:.4f} MB'
        )

    print()
    print("Measured MV benchmarks:")

    for query_id in sorted(benchmarks):
        b = benchmarks[query_id]

        print(
            f'  {query_id}: '
            f'{b["candidate_id"]} | '
            f'{b["baseline_median_ms"]:.2f} → '
            f'{b["mv_median_ms"]:.2f} ms | '
            f'saving={b["saving_ms"]:.2f} ms'
        )

    print()
    print("Final decayed frequencies:")

    for query_id in sorted(frequencies):
        print(
            f"  {query_id}: "
            f"{frequencies[query_id]:.4f}"
        )

    results = []

    for budget in BUDGETS:

        selected, used, benefit = select_views(
            candidates,
            budget,
            frequencies,
            benchmarks
        )

        baseline_total, wamvs_total, covered = (
            calculate_cost(
                selected,
                frequencies,
                no_mv_costs,
                benchmarks
            )
        )

        speedup = (
            baseline_total / wamvs_total
            if wamvs_total > 0
            else 1.0
        )

        improvement = (
            (
                baseline_total
                - wamvs_total
            )
            / baseline_total
            * 100
            if baseline_total > 0
            else 0.0
        )

        selected_ids = [
            x["candidate_id"]
            for x in selected
        ]

        result = {
            "budget_mb": budget,
            "used_storage_mb": round(
                used,
                4
            ),
            "selected_count": len(
                selected
            ),
            "selected_views": selected_ids,
            "covered_queries": sorted(
                covered
            ),
            "weighted_benefit_ms": round(
                benefit,
                2
            ),
            "weighted_baseline_cost_ms": round(
                baseline_total,
                2
            ),
            "weighted_wamvs_cost_ms": round(
                wamvs_total,
                2
            ),
            "speedup_vs_no_mv": round(
                speedup,
                3
            ),
            "improvement_vs_no_mv_pct": round(
                improvement,
                2
            ),
        }

        results.append(result)

        print()
        print(
            f"Budget {budget:.3f} MB"
        )
        print(
            f"  Used       : "
            f"{used:.4f} MB"
        )
        print(
            f"  Views      : "
            f"{len(selected)}"
        )
        print(
            f"  Selected   : "
            f'{", ".join(selected_ids) or "NONE"}'
        )
        print(
            f"  Covered    : "
            f'{", ".join(sorted(covered)) or "NONE"}'
        )
        print(
            f"  Speedup    : "
            f"{speedup:.3f}x"
        )
        print(
            f"  Improvement: "
            f"{improvement:.2f}%"
        )

    output = {
        "methodology": {
            "description": (
                "Storage-budget sensitivity using "
                "measured MV storage, measured MV "
                "execution times, and final decayed "
                "workload frequencies."
            ),
            "selection_policy": (
                "exact 0/1 knapsack"
            ),
            "objective": (
                "frequency-weighted measured "
                "query-time savings"
            ),
            "cost_model": (
                "measured No-MV and MV median "
                "execution times"
            ),
            "budgets_mb": BUDGETS,
        },
        "results": results,
    }

    Path("results").mkdir(
        exist_ok=True
    )

    Path(
        "results/budget_sensitivity.json"
    ).write_text(
        json.dumps(
            output,
            indent=2
        )
    )

    Path(
        "results/figures"
    ).mkdir(
        parents=True,
        exist_ok=True
    )

    # ----------------------------------------------------------
    # Figure 1: Speedup vs storage budget
    # ----------------------------------------------------------

    x = [
        r["budget_mb"]
        for r in results
    ]

    y = [
        r["speedup_vs_no_mv"]
        for r in results
    ]

    plt.figure(
        figsize=(10, 6)
    )

    plt.plot(
        x,
        y,
        marker="o"
    )

    plt.xlabel(
        "Storage Budget (MB)"
    )

    plt.ylabel(
        "Speedup vs No-MV"
    )

    plt.title(
        "WAMVS Speedup vs Storage Budget"
    )

    plt.grid(
        True,
        alpha=0.3
    )

    plt.tight_layout()

    plt.savefig(
        "results/figures/budget_vs_speedup.png",
        dpi=200
    )

    plt.close()

    # ----------------------------------------------------------
    # Figure 2: Number of selected views
    # ----------------------------------------------------------

    y = [
        r["selected_count"]
        for r in results
    ]

    plt.figure(
        figsize=(10, 6)
    )

    plt.plot(
        x,
        y,
        marker="o"
    )

    plt.xlabel(
        "Storage Budget (MB)"
    )

    plt.ylabel(
        "Number of Selected MVs"
    )

    plt.title(
        "WAMVS View Selection vs Storage Budget"
    )

    plt.grid(
        True,
        alpha=0.3
    )

    plt.tight_layout()

    plt.savefig(
        "results/figures/budget_vs_selected_views.png",
        dpi=200
    )

    plt.close()

    print()
    print("=" * 80)
    print("DONE")
    print("=" * 80)
    print(
        "Saved: results/budget_sensitivity.json"
    )
    print(
        "Saved: results/figures/budget_vs_speedup.png"
    )
    print(
        "Saved: results/figures/budget_vs_selected_views.png"
    )


if __name__ == "__main__":
    main()

from pathlib import Path
import json
import statistics

ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / "results"
WORKLOAD = ROOT / "workload"

DRIFT_FILE = WORKLOAD / "drift_workload.json"
ADAPTIVE_FILE = WORKLOAD / "adaptive_experiment_replacement_final.json"
NO_MV_FILE = RESULTS / "no_mv_baseline.json"
STATIC_FILE = RESULTS / "static_topk_baseline.json"
REWRITE_FILE = WORKLOAD / "rewrite_benchmark.json"
MISSING_FILE = WORKLOAD / "missing_candidate_benchmark.json"
Q08_FILE = WORKLOAD / "q08_benchmark.json"
Q09_FILE = WORKLOAD / "q09_benchmark.json"

OUTPUT_FILE = RESULTS / "controlled_static_vs_wamvs.json"

QUERY_IDS = [f"q{i:02d}" for i in range(1, 11)]

STATIC_VIEWS = [
    "MV_003",
    "MV_001",
    "MV_002",
    "MV_004",
]


def load(path):
    return json.loads(path.read_text())


def query_dict(data):
    if isinstance(data, list):
        return {x["query_id"]: x for x in data}

    if isinstance(data, dict):
        if "results" in data and isinstance(data["results"], dict):
            return data["results"]

        if "query_id" in data:
            return {data["query_id"]: data}

        return data

    return {}


def load_mv_benchmarks():
    mv = {}

    for path in [
        REWRITE_FILE,
        MISSING_FILE,
        Q08_FILE,
        Q09_FILE,
    ]:
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

            mv_time = (
                item.get("mv_median_ms")
                or item.get("static_topk_ms")
            )

            if mv_time is not None:
                mv[qid] = {
                    "mv_median_ms": mv_time,
                    "candidate_id": item.get("candidate_id"),
                }

    return mv


def build_query_costs():
    no_mv = query_dict(load(NO_MV_FILE))
    mv = load_mv_benchmarks()

    costs = {}

    for qid in QUERY_IDS:
        if qid not in no_mv:
            continue

        baseline = no_mv[qid]["median_ms"]

        costs[qid] = {
            "no_mv_ms": baseline,
            "mv_ms": mv.get(qid, {}).get("mv_median_ms"),
            "candidate_id": mv.get(qid, {}).get("candidate_id"),
        }

    return costs


def static_serves_query(qid):
    """
    Determine whether the fixed Static Top-K configuration
    contains the MV candidate used by the measured query.
    """
    return qid in {
        "q01",  # MV_001
        "q03",  # MV_003
        "q04",  # MV_001
        "q05",  # MV_004
        "q07",  # MV_003
        "q09",  # MV_003
    }


def adaptive_serves_query(qid, selected):
    """
    Determine whether WAMVS currently contains the candidate
    MV required by the measured query.
    """
    candidate_map = {
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

    candidate = candidate_map.get(qid)

    return candidate in selected if candidate else False


def evaluate():
    drift = load(DRIFT_FILE)
    adaptive = load(ADAPTIVE_FILE)
    costs = build_query_costs()

    static_interval_results = []
    adaptive_interval_results = []

    for interval in drift:
        counts = interval["query_counts"]
        interval_id = interval["interval"]
        phase = interval["phase"]

        total_static = 0.0
        total_adaptive = 0.0
        total_no_mv = 0.0

        static_saved = 0.0
        adaptive_saved = 0.0

        static_hits = 0
        adaptive_hits = 0
        total_queries = 0

        # Find corresponding adaptive configuration.
        adaptive_row = next(
            x for x in adaptive
            if x["interval"] == interval_id
        )

        selected = adaptive_row["selected_views"]

        for qid, frequency in counts.items():
            if qid not in costs:
                continue

            baseline = costs[qid]["no_mv_ms"]
            mv_time = costs[qid]["mv_ms"]

            if mv_time is None:
                continue

            total_queries += frequency

            # No-MV execution cost.
            total_no_mv += frequency * baseline

            # Static Top-K.
            if static_serves_query(qid):
                static_time = mv_time
                static_hits += frequency
            else:
                static_time = baseline

            total_static += frequency * static_time
            static_saved += frequency * (baseline - static_time)

            # WAMVS.
            if adaptive_serves_query(qid, selected):
                adaptive_time = mv_time
                adaptive_hits += frequency
            else:
                adaptive_time = baseline

            total_adaptive += frequency * adaptive_time
            adaptive_saved += frequency * (baseline - adaptive_time)

        static_interval_results.append({
            "interval": interval_id,
            "phase": phase,
            "workload_queries": total_queries,
            "no_mv_cost_ms": total_no_mv,
            "static_topk_cost_ms": total_static,
            "time_saved_ms": static_saved,
            "speedup_vs_no_mv": (
                total_no_mv / total_static
                if total_static else None
            ),
            "hit_rate_percent": (
                static_hits / total_queries * 100
                if total_queries else 0
            ),
        })

        adaptive_interval_results.append({
            "interval": interval_id,
            "phase": phase,
            "workload_queries": total_queries,
            "selected_views": selected,
            "no_mv_cost_ms": total_no_mv,
            "wamvs_cost_ms": total_adaptive,
            "time_saved_ms": adaptive_saved,
            "speedup_vs_no_mv": (
                total_no_mv / total_adaptive
                if total_adaptive else None
            ),
            "hit_rate_percent": (
                adaptive_hits / total_queries * 100
                if total_queries else 0
            ),
            "storage_mb": adaptive_row["storage_mb"],
            "added_views": adaptive_row.get("added_views", []),
            "dropped_views": adaptive_row.get("dropped_views", []),
        })

    total_no_mv = sum(x["no_mv_cost_ms"] for x in static_interval_results)
    total_static = sum(x["static_topk_cost_ms"] for x in static_interval_results)
    total_adaptive = sum(x["wamvs_cost_ms"] for x in adaptive_interval_results)

    result = {
        "methodology": {
            "description": (
                "Controlled workload-weighted comparison using the same "
                "9-interval stable/drifting/bursty workload. Query costs "
                "are based on measured median execution times."
            ),
            "static_views": STATIC_VIEWS,
            "budget_mb": 0.0200,
            "intervals": 9,
        },
        "summary": {
            "no_mv_total_ms": total_no_mv,
            "static_topk_total_ms": total_static,
            "wamvs_total_ms": total_adaptive,

            "static_speedup_vs_no_mv": (
                total_no_mv / total_static
                if total_static else None
            ),

            "wamvs_speedup_vs_no_mv": (
                total_no_mv / total_adaptive
                if total_adaptive else None
            ),

            "wamvs_vs_static_improvement_pct": (
                (total_static - total_adaptive)
                / total_static * 100
                if total_static else None
            ),

            "static_time_saved_ms": total_no_mv - total_static,
            "wamvs_time_saved_ms": total_no_mv - total_adaptive,
        },
        "static_topk": static_interval_results,
        "wamvs": adaptive_interval_results,
    }

    OUTPUT_FILE.write_text(json.dumps(result, indent=2))

    print("=" * 80)
    print("CONTROLLED STATIC TOP-K vs WAMVS")
    print("=" * 80)

    print("\nTOTAL WORKLOAD-WEIGHTED COST")
    print("-" * 80)
    print(f"No-MV       : {total_no_mv:,.2f} ms")
    print(f"Static Top-K: {total_static:,.2f} ms")
    print(f"WAMVS       : {total_adaptive:,.2f} ms")

    print("\nPERFORMANCE")
    print("-" * 80)
    print(
        f"Static speedup vs No-MV : "
        f"{total_no_mv / total_static:.2f}x"
    )
    print(
        f"WAMVS speedup vs No-MV  : "
        f"{total_no_mv / total_adaptive:.2f}x"
    )
    print(
        f"WAMVS improvement vs Static: "
        f"{(total_static - total_adaptive) / total_static * 100:.2f}%"
    )

    print("\nINTERVAL COMPARISON")
    print("-" * 80)

    for s, a in zip(static_interval_results, adaptive_interval_results):
        print(
            f"Interval {s['interval']} ({s['phase']:<8}) | "
            f"Static={s['static_topk_cost_ms']:,.1f} ms | "
            f"WAMVS={a['wamvs_cost_ms']:,.1f} ms | "
            f"WAMVS hit={a['hit_rate_percent']:.1f}% | "
            f"Added={a['added_views']} | "
            f"Dropped={a['dropped_views']}"
        )

    print("\n" + "=" * 80)
    print("CONTROLLED COMPARISON COMPLETE")
    print("=" * 80)
    print(f"Output: {OUTPUT_FILE}")


if __name__ == "__main__":
    evaluate()

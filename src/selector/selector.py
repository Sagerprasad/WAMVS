from pathlib import Path
import json


CANDIDATE_FILE = Path("workload/candidate_views.json")
METRICS_FILE = Path("workload/generic_materialization_metrics.json")
OUTPUT_FILE = Path("workload/selected_views.json")

# Initial storage budget.
#
# The actual MVs are tiny because our TPC-DS dataset is SF=1.
# We therefore use a small MB-scale budget for the prototype.
STORAGE_BUDGET_MB = 0.020


def load_json(path):
    return json.loads(path.read_text())


def calculate_utility(candidate, metrics):
    """
    Estimate the usefulness of an MV.

    Current prototype:
        benefit = query frequency
        cost    = actual storage

    Higher frequency + smaller storage = higher utility.

    Later we will replace this with a runtime-aware model
    using measured query speedup and maintenance cost.
    """

    frequency = candidate["frequency"]
    storage_mb = metrics["storage_mb"]

    if storage_mb <= 0:
        return 0

    return frequency / storage_mb


def main():

    candidates = load_json(CANDIDATE_FILE)
    metrics_list = load_json(METRICS_FILE)

    metrics_by_id = {
        m["candidate_id"]: m
        for m in metrics_list
        if m.get("status") == "success"
    }

    scored_candidates = []

    print("=" * 75)
    print("WAMVS BUDGET-AWARE VIEW SELECTOR")
    print("=" * 75)

    print(f"\nStorage budget : {STORAGE_BUDGET_MB:.4f} MB")

    # ---------------------------------------------------------
    # Calculate utility for every candidate
    # ---------------------------------------------------------
    for candidate in candidates:

        candidate_id = candidate["candidate_id"]

        if candidate_id not in metrics_by_id:
            print(
                f"\nSkipping {candidate_id}: "
                "no materialization metrics"
            )
            continue

        metrics = metrics_by_id[candidate_id]

        storage_mb = metrics["storage_mb"]

        utility = calculate_utility(
            candidate,
            metrics
        )

        scored_candidates.append({
            "candidate_id": candidate_id,
            "frequency": candidate["frequency"],
            "storage_mb": storage_mb,
            "utility": utility,
            "row_count": metrics["row_count"],
            "materialization_time_ms":
                metrics["materialization_time_ms"],
            "source_queries":
                candidate["source_queries"],
            "tables":
                candidate["tables"],
            "group_by":
                candidate["group_by"],
            "measures":
                candidate["measures"],
        })

    # ---------------------------------------------------------
    # Sort by utility density
    # ---------------------------------------------------------
    scored_candidates.sort(
        key=lambda x: x["utility"],
        reverse=True
    )

    print("\nCandidate ranking:")
    print("-" * 75)

    for rank, candidate in enumerate(
        scored_candidates,
        start=1
    ):

        print(
            f"{rank:2d}. "
            f"{candidate['candidate_id']:8s} | "
            f"freq={candidate['frequency']} | "
            f"storage="
            f"{candidate['storage_mb']:.4f} MB | "
            f"utility="
            f"{candidate['utility']:.2f}"
        )

    # ---------------------------------------------------------
    # Greedy budget selection
    # ---------------------------------------------------------
    selected = []
    used_storage = 0.0

    for candidate in scored_candidates:

        candidate_storage = candidate["storage_mb"]

        if (
            used_storage + candidate_storage
            <= STORAGE_BUDGET_MB
        ):

            selected.append(candidate)

            used_storage += candidate_storage

    # ---------------------------------------------------------
    # Write selected views
    # ---------------------------------------------------------
    OUTPUT_FILE.write_text(
        json.dumps(
            selected,
            indent=2
        )
    )

    # ---------------------------------------------------------
    # Report
    # ---------------------------------------------------------
    print("\n" + "=" * 75)
    print("SELECTED MATERIALIZED VIEWS")
    print("=" * 75)

    if not selected:

        print("No views selected.")

    else:

        for candidate in selected:

            print(
                f"\n{candidate['candidate_id']}"
            )

            print(
                f"  Queries     : "
                f"{', '.join(candidate['source_queries'])}"
            )

            print(
                f"  Frequency   : "
                f"{candidate['frequency']}"
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
        f"{len(selected)}/{len(scored_candidates)}"
    )

    print(
        f"Storage used   : "
        f"{used_storage:.4f} MB"
    )

    print(
        f"Storage budget : "
        f"{STORAGE_BUDGET_MB:.4f} MB"
    )

    print(
        f"Remaining      : "
        f"{STORAGE_BUDGET_MB - used_storage:.4f} MB"
    )

    print(
        f"\nOutput         : "
        f"{OUTPUT_FILE}"
    )

    print("=" * 75)


if __name__ == "__main__":
    main()

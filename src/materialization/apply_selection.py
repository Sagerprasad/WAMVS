from pathlib import Path
import json
import shutil


SELECTED_FILE = Path("workload/selected_views.json")
METRICS_FILE = Path("workload/generic_materialization_metrics.json")
DELTA_DIR = Path("data/tpcds/delta")


def load_json(path):
    return json.loads(path.read_text())


def main():

    selected = load_json(SELECTED_FILE)
    metrics = load_json(METRICS_FILE)

    selected_ids = {
        view["candidate_id"]
        for view in selected
    }

    all_ids = {
        metric["candidate_id"]
        for metric in metrics
        if metric.get("status") == "success"
    }

    print("=" * 75)
    print("WAMVS APPLY SELECTION")
    print("=" * 75)

    print(f"\nCandidate MVs : {len(all_ids)}")
    print(f"Selected MVs  : {len(selected_ids)}")

    # ---------------------------------------------------------
    # Keep selected views
    # ---------------------------------------------------------
    print("\nKEEP")
    print("-" * 75)

    kept_storage = 0.0

    for view in selected:

        candidate_id = view["candidate_id"]
        storage = view["storage_mb"]

        path = DELTA_DIR / candidate_id

        if path.exists():

            print(
                f"  {candidate_id} | "
                f"{storage:.4f} MB | "
                f"{', '.join(view['source_queries'])}"
            )

            kept_storage += storage

        else:

            print(
                f"  {candidate_id} | "
                "WARNING: Delta path missing"
            )

    # ---------------------------------------------------------
    # Drop unselected views
    # ---------------------------------------------------------
    dropped_storage = 0.0

    print("\nDROP")
    print("-" * 75)

    for candidate_id in sorted(
        all_ids - selected_ids
    ):

        path = DELTA_DIR / candidate_id

        metric = next(
            (
                m for m in metrics
                if m["candidate_id"] == candidate_id
            ),
            None
        )

        storage = (
            metric["storage_mb"]
            if metric
            else 0.0
        )

        if path.exists():

            shutil.rmtree(path)

            dropped_storage += storage

            print(
                f"  {candidate_id} | "
                f"{storage:.4f} MB"
            )

        else:

            print(
                f"  {candidate_id} | "
                "already absent"
            )

    # ---------------------------------------------------------
    # Verify filesystem state
    # ---------------------------------------------------------
    remaining = sorted(
        p.name
        for p in DELTA_DIR.iterdir()
        if p.is_dir()
        and p.name.startswith("MV_")
    )

    print("\n" + "=" * 75)
    print("FINAL MATERIALIZED VIEW STATE")
    print("=" * 75)

    for view_id in remaining:
        print(f"  {view_id}")

    print("-" * 75)

    print(
        f"Expected selected : "
        f"{sorted(selected_ids)}"
    )

    print(
        f"Actual remaining  : "
        f"{remaining}"
    )

    print(
        f"\nKept storage      : "
        f"{kept_storage:.4f} MB"
    )

    print(
        f"Dropped storage   : "
        f"{dropped_storage:.4f} MB"
    )

    if set(remaining) == selected_ids:

        print(
            "\nSTATUS: SUCCESS"
        )

        print(
            "Only selected materialized views remain."
        )

    else:

        print(
            "\nSTATUS: WARNING"
        )

        print(
            "Filesystem state does not match selection."
        )

    print("=" * 75)


if __name__ == "__main__":
    main()

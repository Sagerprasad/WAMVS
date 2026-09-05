from pathlib import Path
import json


FREQUENCY_FILE = Path(
    "workload/adaptive_frequency.json"
)

CANDIDATE_FILE = Path(
    "workload/candidate_views.json"
)

METRICS_FILE = Path(
    "workload/generic_materialization_metrics.json"
)

OUTPUT_FILE = Path(
    "workload/adaptive_selection.json"
)

STORAGE_BUDGET_MB = 0.0200


def load_json(path):
    return json.loads(
        path.read_text()
    )


def calculate_view_score(
    candidate,
    frequency_scores
):
    """
    Calculate the current workload value of an MV.

    An MV may serve multiple queries. Therefore its workload
    score is the sum of the decayed frequencies of all queries
    that the MV can answer.
    """

    score = 0.0

    for query in candidate["source_queries"]:

        score += frequency_scores.get(
            query,
            0.0
        )

    return score


def main():

    frequency_history = load_json(
        FREQUENCY_FILE
    )

    candidates = load_json(
        CANDIDATE_FILE
    )

    metrics = load_json(
        METRICS_FILE
    )

    metrics_by_id = {
        item["candidate_id"]: item
        for item in metrics
        if item.get("status") == "success"
    }

    previous_selected = set()

    history = []

    print("=" * 80)
    print("WAMVS ADAPTIVE MATERIALIZED VIEW SELECTOR")
    print("=" * 80)

    print(
        f"\nStorage budget : "
        f"{STORAGE_BUDGET_MB:.4f} MB"
    )

    # ---------------------------------------------------------
    # Process each workload interval
    # ---------------------------------------------------------

    for observation in frequency_history:

        interval = observation["interval"]
        phase = observation["phase"]
        frequency_scores = observation["scores"]

        scored = []

        for candidate in candidates:

            candidate_id = candidate["candidate_id"]

            if candidate_id not in metrics_by_id:
                continue

            storage = metrics_by_id[
                candidate_id
            ]["storage_mb"]

            if storage <= 0:
                continue

            score = calculate_view_score(
                candidate,
                frequency_scores
            )

            # Utility density:
            #
            # workload value per MB of storage.
            utility = score / storage

            scored.append({

                "candidate_id":
                    candidate_id,

                "score":
                    score,

                "storage_mb":
                    storage,

                "utility":
                    utility,

                "source_queries":
                    candidate["source_queries"],
            })

        # -----------------------------------------------------
        # Rank by adaptive utility
        # -----------------------------------------------------

        scored.sort(
            key=lambda x: x["utility"],
            reverse=True
        )

        # -----------------------------------------------------
        # Greedy budget selection
        # -----------------------------------------------------

        selected = []
        used_storage = 0.0

        for candidate in scored:

            storage = candidate["storage_mb"]

            if (
                used_storage + storage
                <= STORAGE_BUDGET_MB
            ):

                selected.append(
                    candidate
                )

                used_storage += storage

        current_selected = {
            item["candidate_id"]
            for item in selected
        }

        # -----------------------------------------------------
        # Detect changes
        # -----------------------------------------------------

        created = sorted(
            current_selected
            - previous_selected
        )

        dropped = sorted(
            previous_selected
            - current_selected
        )

        unchanged = (
            current_selected
            == previous_selected
        )

        # -----------------------------------------------------
        # Save interval
        # -----------------------------------------------------

        interval_result = {

            "interval":
                interval,

            "phase":
                phase,

            "selected_views":
                sorted(
                    current_selected
                ),

            "storage_used_mb":
                round(
                    used_storage,
                    4
                ),

            "storage_budget_mb":
                STORAGE_BUDGET_MB,

            "created_views":
                created,

            "dropped_views":
                dropped,

            "configuration_changed":
                not unchanged,

            "ranking": [
                {
                    "candidate_id":
                        item["candidate_id"],

                    "score":
                        round(
                            item["score"],
                            4
                        ),

                    "storage_mb":
                        item["storage_mb"],

                    "utility":
                        round(
                            item["utility"],
                            4
                        ),

                    "source_queries":
                        item["source_queries"],
                }
                for item in scored
            ],
        }

        history.append(
            interval_result
        )

        # -----------------------------------------------------
        # Console output
        # -----------------------------------------------------

        print(
            f"\nInterval {interval:2d} | "
            f"{phase:9s}"
        )

        print(
            "  Selected : "
            + ", ".join(
                sorted(current_selected)
            )
        )

        print(
            f"  Storage  : "
            f"{used_storage:.4f} / "
            f"{STORAGE_BUDGET_MB:.4f} MB"
        )

        if created:

            print(
                "  CREATE   : "
                + ", ".join(created)
            )

        if dropped:

            print(
                "  DROP     : "
                + ", ".join(dropped)
            )

        if unchanged:

            print(
                "  CHANGE   : none"
            )

        previous_selected = (
            current_selected
        )

    # ---------------------------------------------------------
    # Write output
    # ---------------------------------------------------------

    OUTPUT_FILE.write_text(
        json.dumps(
            history,
            indent=2
        )
    )

    # ---------------------------------------------------------
    # Summary
    # ---------------------------------------------------------

    print("\n" + "=" * 80)
    print("ADAPTIVE SELECTION SUMMARY")
    print("=" * 80)

    changed_intervals = sum(
        1
        for item in history
        if item["configuration_changed"]
    )

    total_created = sum(
        len(item["created_views"])
        for item in history
    )

    total_dropped = sum(
        len(item["dropped_views"])
        for item in history
    )

    print(
        f"Intervals evaluated : "
        f"{len(history)}"
    )

    print(
        f"Configuration changes: "
        f"{changed_intervals}"
    )

    print(
        f"Views created       : "
        f"{total_created}"
    )

    print(
        f"Views dropped       : "
        f"{total_dropped}"
    )

    if history:

        print(
            "\nInitial configuration:"
        )

        print(
            "  "
            + ", ".join(
                history[0]["selected_views"]
            )
        )

        print(
            "\nFinal configuration:"
        )

        print(
            "  "
            + ", ".join(
                history[-1]["selected_views"]
            )
        )

    print(
        f"\nOutput: "
        f"{OUTPUT_FILE}"
    )

    print("=" * 80)


if __name__ == "__main__":
    main()

from pathlib import Path
import json


INPUT_FILE = Path(
    "workload/drift_workload.json"
)

OUTPUT_FILE = Path(
    "workload/adaptive_frequency.json"
)


# -------------------------------------------------------------
# Exponential decay factor
# -------------------------------------------------------------
#
# A value closer to 1 remembers older workload observations
# for longer.
#
# A smaller value reacts faster to workload drift.
# -------------------------------------------------------------

DECAY_FACTOR = 0.70


def main():

    observations = json.loads(
        INPUT_FILE.read_text()
    )

    query_scores = {}

    history = []

    for observation in observations:

        interval = observation["interval"]
        phase = observation["phase"]
        current_counts = observation["query_counts"]

        # -----------------------------------------------------
        # Decay previous workload scores
        # -----------------------------------------------------
        for query in list(query_scores):

            query_scores[query] *= DECAY_FACTOR

        # -----------------------------------------------------
        # Add current workload
        # -----------------------------------------------------
        for query, count in current_counts.items():

            query_scores[query] = (
                query_scores.get(query, 0)
                + count
            )

        # Remove extremely small values.
        for query in list(query_scores):

            if query_scores[query] < 0.01:
                del query_scores[query]

        ranking = sorted(
            query_scores.items(),
            key=lambda x: x[1],
            reverse=True
        )

        history.append({

            "interval": interval,

            "phase": phase,

            "scores": {
                query: round(
                    score,
                    4
                )
                for query, score in query_scores.items()
            },

            "ranking": [
                {
                    "query": query,
                    "score": round(
                        score,
                        4
                    )
                }
                for query, score in ranking
            ],
        })

        print(
            f"Interval {interval:2d} | "
            f"{phase:9s} | "
            + ", ".join(
                f"{query}={score:.2f}"
                for query, score in ranking[:5]
            )
        )

    OUTPUT_FILE.write_text(
        json.dumps(
            history,
            indent=2
        )
    )

    print("\n" + "=" * 75)
    print("ADAPTIVE FREQUENCY SUMMARY")
    print("=" * 75)

    print(
        f"Decay factor : "
        f"{DECAY_FACTOR}"
    )

    print(
        f"Intervals    : "
        f"{len(history)}"
    )

    if history:

        final = history[-1]

        print("\nFinal ranking:")

        for rank, item in enumerate(
            final["ranking"],
            start=1
        ):

            print(
                f"{rank:2d}. "
                f"{item['query']:5s} "
                f"score={item['score']:.4f}"
            )

    print(
        f"\nOutput: "
        f"{OUTPUT_FILE}"
    )

    print("=" * 75)


if __name__ == "__main__":
    main()

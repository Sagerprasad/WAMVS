from pathlib import Path
import json


OUTPUT_FILE = Path("workload/drift_workload.json")


# -------------------------------------------------------------
# Workload phases
# -------------------------------------------------------------
#
# Each phase contains query executions observed during a
# monitoring interval.
#
# Stable:
#   Q01, Q03, Q05 and Q08 dominate.
#
# Drifting:
#   Q09, Q10 and Q06 gradually become more important.
#
# Bursty:
#   Q09 and Q10 experience a temporary workload burst.
# -------------------------------------------------------------

PHASES = [

    {
        "phase": "stable",
        "intervals": [
            {
                "q01": 10,
                "q03": 8,
                "q05": 6,
                "q08": 4,
                "q04": 2,
                "q07": 2,
            },
            {
                "q01": 10,
                "q03": 8,
                "q05": 6,
                "q08": 4,
                "q04": 2,
                "q07": 2,
            },
            {
                "q01": 10,
                "q03": 8,
                "q05": 6,
                "q08": 4,
                "q04": 2,
                "q07": 2,
            },
        ],
    },

    {
        "phase": "drifting",
        "intervals": [
            {
                "q01": 8,
                "q03": 6,
                "q05": 4,
                "q08": 3,
                "q06": 4,
                "q09": 4,
                "q10": 3,
            },
            {
                "q01": 5,
                "q03": 4,
                "q05": 2,
                "q08": 2,
                "q06": 7,
                "q09": 8,
                "q10": 7,
            },
            {
                "q01": 3,
                "q03": 2,
                "q05": 1,
                "q08": 1,
                "q06": 9,
                "q09": 12,
                "q10": 10,
            },
        ],
    },

    {
        "phase": "bursty",
        "intervals": [
            {
                "q09": 25,
                "q10": 20,
                "q06": 15,
                "q01": 2,
            },
            {
                "q09": 30,
                "q10": 25,
                "q06": 18,
            },
            {
                "q09": 5,
                "q10": 4,
                "q06": 3,
                "q01": 5,
                "q03": 4,
            },
        ],
    },
]


def main():

    observations = []

    interval_id = 0

    for phase in PHASES:

        for workload in phase["intervals"]:

            interval_id += 1

            observations.append({
                "interval": interval_id,
                "phase": phase["phase"],
                "query_counts": workload,
            })

    OUTPUT_FILE.write_text(
        json.dumps(
            observations,
            indent=2
        )
    )

    print("=" * 75)
    print("WAMVS WORKLOAD DRIFT SIMULATOR")
    print("=" * 75)

    print(
        f"\nIntervals generated : "
        f"{len(observations)}"
    )

    print("\nWorkload phases:")
    print("-" * 75)

    for observation in observations:

        counts = observation["query_counts"]

        ordered = sorted(
            counts.items(),
            key=lambda x: x[1],
            reverse=True
        )

        summary = ", ".join(
            f"{q}={count}"
            for q, count in ordered
        )

        print(
            f"Interval "
            f"{observation['interval']:2d} | "
            f"{observation['phase']:9s} | "
            f"{summary}"
        )

    print(
        f"\nOutput: "
        f"{OUTPUT_FILE}"
    )

    print("=" * 75)


if __name__ == "__main__":
    main()

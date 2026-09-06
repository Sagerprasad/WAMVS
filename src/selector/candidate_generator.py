from pathlib import Path
import json


WORKLOAD_FILE = Path("workload/workload_features.json")
OUTPUT_FILE = Path("workload/candidate_views.json")


MEASURE_EXPRESSIONS = {
    "revenue": "SUM(ss_ext_sales_price)",
    "profit": "SUM(ss_net_profit)",
    "total_quantity": "SUM(ss_quantity)",
    "avg_price": "AVG(ss_sales_price)",
    "count": "COUNT(*)",
}


QUERY_MEASURES = {
    "q01": ["revenue", "profit"],
    "q02": ["revenue", "profit"],
    "q03": ["revenue"],
    "q04": ["total_quantity", "avg_price"],
    "q05": ["revenue"],
    "q06": ["revenue"],
    "q07": ["profit"],
    "q08": ["revenue", "profit"],
    "q09": ["count", "revenue"],
    "q10": ["revenue"],
}


def load_workload():
    return json.loads(WORKLOAD_FILE.read_text())


def canonicalize_columns(columns):
    """
    Treat GROUP BY columns as an unordered set for candidate identity.

    Example:
        [d_year, i_category]
        [i_category, d_year]

    become the same canonical representation.
    """
    return tuple(sorted(set(columns)))


def canonicalize_tables(tables):
    """
    Canonicalize table order so that the same collection of tables
    produces the same candidate identity.
    """
    return tuple(sorted(set(tables)))


def generate_candidates(workload):
    candidates = {}

    for query in workload:
        qid = query["query_id"]
        features = query["features"]

        # -----------------------------------------------------
        # Canonical candidate identity
        # -----------------------------------------------------
        tables = canonicalize_tables(
            features["tables"]
        )

        group_by = canonicalize_columns(
            features["group_by"]
        )

        key = (
            tables,
            group_by,
        )

        # -----------------------------------------------------
        # Create candidate if it does not exist
        # -----------------------------------------------------
        if key not in candidates:
            candidates[key] = {
                "candidate_id": f"MV_{len(candidates) + 1:03d}",
                "tables": list(tables),
                "group_by": list(group_by),
                "measures": [],
                "measure_expressions": {},
                "joins": [],
                "source_queries": [],
                "frequency": 0,
            }

        candidate = candidates[key]

        # -----------------------------------------------------
        # Merge required measures
        # -----------------------------------------------------
        for measure in QUERY_MEASURES.get(qid, []):

            if measure not in candidate["measures"]:
                candidate["measures"].append(measure)

                expression = MEASURE_EXPRESSIONS.get(
                    measure
                )

                if expression:
                    candidate["measure_expressions"][
                        measure
                    ] = expression

        # -----------------------------------------------------
        # Merge JOIN definitions
        # -----------------------------------------------------
        for join in features["joins"]:

            join_definition = {
                "table": join["table"],
                "condition": join["condition"],
            }

            if join_definition not in candidate["joins"]:
                candidate["joins"].append(
                    join_definition
                )

        # -----------------------------------------------------
        # Track workload contribution
        # -----------------------------------------------------
        if qid not in candidate["source_queries"]:
            candidate["source_queries"].append(qid)

        candidate["frequency"] += 1

    return list(candidates.values())


def main():
    workload = load_workload()

    candidates = generate_candidates(workload)

    OUTPUT_FILE.write_text(
        json.dumps(candidates, indent=2)
    )

    print("=" * 70)
    print("WAMVS GENERIC CANDIDATE VIEW GENERATOR")
    print("=" * 70)

    for candidate in candidates:

        print(
            f"\n{candidate['candidate_id']}"
        )

        print(
            f"  Tables       : "
            f"{', '.join(candidate['tables'])}"
        )

        print(
            f"  GROUP BY     : "
            f"{candidate['group_by']}"
        )

        print(
            f"  Measures     : "
            f"{candidate['measures']}"
        )

        print("  Expressions  :")

        for measure, expression in (
            candidate["measure_expressions"].items()
        ):
            print(
                f"    {measure:15s} -> {expression}"
            )

        print("  Joins        :")

        for join in candidate["joins"]:
            print(
                f"    JOIN {join['table']} "
                f"ON {join['condition']}"
            )

        print(
            f"  Frequency    : "
            f"{candidate['frequency']}"
        )

        print(
            f"  Queries      : "
            f"{', '.join(candidate['source_queries'])}"
        )

    print("\n" + "=" * 70)
    print(
        f"Candidates generated : "
        f"{len(candidates)}"
    )
    print(
        f"Output                : "
        f"{OUTPUT_FILE}"
    )
    print("=" * 70)


if __name__ == "__main__":
    main()

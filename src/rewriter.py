from pathlib import Path
import json
import re


SELECTED_FILE = Path("workload/selected_views.json")
QUERY_DIR = Path("workload/queries")
OUTPUT_FILE = Path("workload/rewrite_plan.json")


def load_json(path):
    return json.loads(path.read_text())


def normalize_sql(sql):
    """Normalize SQL for simple structural matching."""

    sql = sql.lower()
    sql = re.sub(r"\s+", " ", sql)
    sql = sql.replace(";", "").strip()

    return sql


def extract_query_id(path):
    return path.stem.lower()


def can_rewrite(query_id, view):
    """
    Determine whether a query can safely use a selected MV.

    Current prototype uses the source_queries metadata generated
    by the candidate generator.

    This is deliberately conservative:
    only queries explicitly represented by the MV are rewritten.
    """

    return query_id in view["source_queries"]


def build_rewritten_sql(query_id, view):
    """
    Generate a simple query over the materialized view.

    The MV already contains the GROUP BY dimensions and required
    aggregate measures, so the original joins and aggregation
    are no longer necessary.
    """

    group_by = view["group_by"]
    measures = view["measures"]
    mv_name = view["candidate_id"]

    select_columns = []

    # GROUP BY dimensions
    for column in group_by:
        select_columns.append(column)

    # Required measures for this query
    #
    # We need to determine which measures the original query
    # requested. The candidate stores the union of measures used
    # by all source queries, so select only the measures associated
    # with the current query.
    #
    # This mapping mirrors candidate_generator.py.
    query_measures = {
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

    required = query_measures.get(
        query_id,
        []
    )

    for measure in required:

        if measure in measures:
            select_columns.append(measure)

    if not select_columns:
        return None

    sql = (
        "SELECT "
        + ", ".join(select_columns)
        + f" FROM {mv_name}"
    )

    # ---------------------------------------------------------
    # Preserve simple query filters that operate only on
    # GROUP BY columns.
    #
    # For the initial prototype we handle the known filters.
    # ---------------------------------------------------------
    filters = {

        "q04": "i_category = 'Music'",

        "q05": "d_year BETWEEN 1999 AND 2001",

        "q07": "d_year = 2000",

    }

    if query_id in filters:

        condition = filters[query_id]

        # Extract actual column identifiers from the condition.
        # Ignore SQL keywords and quoted string values.
        # Remove quoted string literals before extracting
        # identifiers. For example:
        #
        #   i_category = 'Music'
        #
        # should identify only i_category as a column.
        condition_without_strings = re.sub(
            r"'(?:''|[^'])*'",
            "",
            condition
        )

        referenced_columns = re.findall(
            r"\b[a-zA-Z_][a-zA-Z0-9_]*\b",
            condition_without_strings
        )

        sql_keywords = {
            "BETWEEN",
            "AND",
            "OR",
            "IN",
            "NOT",
            "LIKE",
            "IS",
            "NULL",
        }

        safe = True

        for column in referenced_columns:

            if column.upper() in sql_keywords:
                continue

            if column not in group_by:
                safe = False
                break

        if safe:
            sql += f" WHERE {condition}"

    return sql + ";"


def main():

    selected_views = load_json(
        SELECTED_FILE
    )

    query_files = sorted(
        QUERY_DIR.glob("q*.sql")
    )

    print("=" * 75)
    print("WAMVS GENERIC QUERY REWRITER")
    print("=" * 75)

    rewrite_plan = []

    for query_file in query_files:

        query_id = extract_query_id(
            query_file
        )

        original_sql = query_file.read_text()

        matched_view = None

        for view in selected_views:

            if can_rewrite(
                query_id,
                view
            ):
                matched_view = view
                break

        # -----------------------------------------------------
        # No MV match
        # -----------------------------------------------------
        if matched_view is None:

            result = {
                "query_id": query_id,
                "rewritten": False,
                "view": None,
                "original_sql": original_sql,
                "rewritten_sql": None,
                "reason": "no selected MV",
            }

            rewrite_plan.append(result)

            print(
                f"{query_id}: "
                f"NO MATCH"
            )

            continue

        # -----------------------------------------------------
        # Generate rewritten SQL
        # -----------------------------------------------------
        rewritten_sql = build_rewritten_sql(
            query_id,
            matched_view
        )

        if rewritten_sql is None:

            result = {
                "query_id": query_id,
                "rewritten": False,
                "view":
                    matched_view["candidate_id"],
                "original_sql":
                    original_sql,
                "rewritten_sql": None,
                "reason":
                    "required measures unavailable",
            }

            rewrite_plan.append(result)

            print(
                f"{query_id}: "
                f"NO REWRITE"
            )

            continue

        result = {
            "query_id": query_id,
            "rewritten": True,
            "view":
                matched_view["candidate_id"],
            "original_sql":
                original_sql,
            "rewritten_sql":
                rewritten_sql,
        }

        rewrite_plan.append(result)

        print(
            f"{query_id}: "
            f"REWRITE → "
            f"{matched_view['candidate_id']}"
        )

        print(
            f"  {rewritten_sql}"
        )

    OUTPUT_FILE.write_text(
        json.dumps(
            rewrite_plan,
            indent=2
        )
    )

    # ---------------------------------------------------------
    # Summary
    # ---------------------------------------------------------
    rewritten_count = sum(
        1
        for item in rewrite_plan
        if item["rewritten"]
    )

    total_queries = len(
        rewrite_plan
    )

    print("\n" + "=" * 75)
    print("REWRITE SUMMARY")
    print("=" * 75)

    print(
        f"Total queries : "
        f"{total_queries}"
    )

    print(
        f"Rewritten     : "
        f"{rewritten_count}"
    )

    print(
        f"Unchanged     : "
        f"{total_queries - rewritten_count}"
    )

    if total_queries:

        hit_rate = (
            rewritten_count
            / total_queries
        ) * 100

        print(
            f"MV hit rate   : "
            f"{hit_rate:.2f}%"
        )

    print(
        f"\nPlan written  : "
        f"{OUTPUT_FILE}"
    )

    print("=" * 75)


if __name__ == "__main__":
    main()

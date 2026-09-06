from pathlib import Path
import json
import re


SELECTED_FILE = Path("workload/selected_views.json")
CANDIDATE_FILE = Path("workload/candidate_views.json")
QUERY_DIR = Path("workload/queries")
OUTPUT_FILE = Path("workload/rewrite_plan.json")


# -------------------------------------------------------------
# Query requirements
# -------------------------------------------------------------
#
# These describe what each workload query needs from a
# materialized view.
#
# A view can answer a query when:
#
#   1. It contains the required grouping dimensions.
#   2. It contains all required measures.
#   3. The query's filters reference only grouping dimensions.
#
# The current prototype requires exact table coverage.
# -------------------------------------------------------------

QUERY_REQUIREMENTS = {

    "q01": {
        "tables": ["item", "store_sales"],
        "group_by": ["i_category"],
        "measures": ["revenue", "profit"],
    },

    "q02": {
        "tables": ["date_dim", "store_sales"],
        "group_by": ["d_year", "d_moy"],
        "measures": ["revenue", "profit"],
    },

    "q03": {
        "tables": ["date_dim", "item", "store_sales"],
        "group_by": ["d_year", "i_category"],
        "measures": ["revenue"],
    },

    "q04": {
        "tables": ["item", "store_sales"],
        "group_by": ["i_category"],
        "measures": ["total_quantity", "avg_price"],
    },

    "q05": {
        "tables": ["date_dim", "store_sales"],
        "group_by": ["d_year"],
        "measures": ["revenue"],
    },

    "q06": {
        "tables": ["item", "store_sales"],
        "group_by": ["i_category", "i_class"],
        "measures": ["revenue"],
    },

    "q07": {
        "tables": ["date_dim", "item", "store_sales"],
        "group_by": ["d_year", "i_category"],
        "measures": ["profit"],
    },

    "q08": {
        "tables": ["store_sales"],
        "group_by": ["ss_store_sk"],
        "measures": ["revenue", "profit"],
    },

    "q09": {
        "tables": ["date_dim", "item", "store_sales"],
        "group_by": ["i_category", "d_year"],
        "measures": ["count", "revenue"],
    },

    "q10": {
        "tables": ["date_dim", "item", "store_sales"],
        "group_by": ["d_year", "i_category", "i_brand"],
        "measures": ["revenue"],
    },
}


# -------------------------------------------------------------
# Known safe filters
# -------------------------------------------------------------

QUERY_FILTERS = {

    "q04": "i_category = 'Music'",

    "q05": "d_year BETWEEN 1999 AND 2001",

    "q07": "d_year = 2000",

    "q09": (
        "i_category IN ('Music', 'Books', 'Sports')"
    ),

    "q10": "d_year >= 2000",
}


def load_json(path):
    return json.loads(path.read_text())


def canonical_set(values):
    return set(values)


def tables_match(required_tables, view_tables):
    """
    Current prototype requires exact table coverage.
    """

    return (
        canonical_set(required_tables)
        ==
        canonical_set(view_tables)
    )


def grouping_match(required_group_by, view_group_by):
    """
    A materialized view must contain exactly the query's
    grouping dimensions.

    The order of GROUP BY columns does not matter.
    """

    return (
        canonical_set(required_group_by)
        ==
        canonical_set(view_group_by)
    )


def measures_available(required_measures, view_measures):
    """
    A view may contain additional measures.

    The query only needs the required subset.
    """

    return canonical_set(required_measures).issubset(
        canonical_set(view_measures)
    )


def filter_columns_are_safe(query_id, view):
    """
    A filter can be pushed to the materialized view only when
    every referenced column is present in the view's GROUP BY.

    This prevents filtering on columns that were not materialized.
    """

    condition = QUERY_FILTERS.get(query_id)

    if not condition:
        return True

    condition_without_strings = re.sub(
        r"'(?:''|[^'])*'",
        "",
        condition,
    )

    referenced_columns = re.findall(
        r"\b[a-zA-Z_][a-zA-Z0-9_]*\b",
        condition_without_strings,
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

    group_by = canonical_set(
        view["group_by"]
    )

    for column in referenced_columns:

        if column.upper() in sql_keywords:
            continue

        if column not in group_by:
            return False

    return True


def can_rewrite(query_id, view):
    """
    Structural/subsumption-style matching.

    A view can answer a query when:

        tables match
        AND grouping dimensions match
        AND required measures are available
        AND filters can safely be evaluated on the view
    """

    requirements = QUERY_REQUIREMENTS.get(
        query_id
    )

    if requirements is None:
        return False

    if not tables_match(
        requirements["tables"],
        view["tables"],
    ):
        return False

    if not grouping_match(
        requirements["group_by"],
        view["group_by"],
    ):
        return False

    if not measures_available(
        requirements["measures"],
        view["measures"],
    ):
        return False

    if not filter_columns_are_safe(
        query_id,
        view,
    ):
        return False

    return True


def build_rewritten_sql(query_id, view):
    """
    Generate a query directly over the materialized view.

    Only the dimensions and measures required by the original
    query are projected.
    """

    requirements = QUERY_REQUIREMENTS.get(
        query_id
    )

    if requirements is None:
        return None

    mv_name = view["candidate_id"]

    select_columns = []

    # ---------------------------------------------------------
    # Preserve the query's logical GROUP BY ordering.
    # ---------------------------------------------------------

    for column in requirements["group_by"]:

        if column not in view["group_by"]:
            return None

        select_columns.append(column)

    # ---------------------------------------------------------
    # Select only measures required by this query.
    # ---------------------------------------------------------

    for measure in requirements["measures"]:

        if measure not in view["measures"]:
            return None

        select_columns.append(measure)

    if not select_columns:
        return None

    sql = (
        "SELECT "
        + ", ".join(select_columns)
        + f" FROM {mv_name}"
    )

    # ---------------------------------------------------------
    # Apply safe filters.
    # ---------------------------------------------------------

    condition = QUERY_FILTERS.get(
        query_id
    )

    if condition:

        if not filter_columns_are_safe(
            query_id,
            view,
        ):
            return None

        sql += f" WHERE {condition}"

    return sql + ";"


def main():

    selected_views = load_json(
        SELECTED_FILE
    )

    # Load the canonical candidate list as a consistency check.
    candidates = load_json(
        CANDIDATE_FILE
    )

    candidate_ids = {
        candidate["candidate_id"]
        for candidate in candidates
    }

    query_files = sorted(
        QUERY_DIR.glob("q*.sql")
    )

    print("=" * 75)
    print("WAMVS STRUCTURAL QUERY REWRITER")
    print("=" * 75)

    print(
        "\nMatching rule:"
    )

    print(
        "  tables + GROUP BY + required measures + safe filters"
    )

    rewrite_plan = []

    for query_file in query_files:

        query_id = query_file.stem.lower()

        original_sql = query_file.read_text()

        matched_view = None

        # -----------------------------------------------------
        # Find a structurally compatible selected MV.
        # -----------------------------------------------------

        for view in selected_views:

            if view["candidate_id"] not in candidate_ids:
                continue

            if can_rewrite(
                query_id,
                view,
            ):
                matched_view = view
                break

        # -----------------------------------------------------
        # No match
        # -----------------------------------------------------

        if matched_view is None:

            result = {
                "query_id": query_id,
                "rewritten": False,
                "view": None,
                "original_sql": original_sql,
                "rewritten_sql": None,
                "reason": "no structurally compatible selected MV",
            }

            rewrite_plan.append(result)

            print(
                f"{query_id}: NO MATCH"
            )

            continue

        # -----------------------------------------------------
        # Build rewritten SQL.
        # -----------------------------------------------------

        rewritten_sql = build_rewritten_sql(
            query_id,
            matched_view,
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
                    "required fields or filters unavailable",
            }

            rewrite_plan.append(result)

            print(
                f"{query_id}: NO REWRITE"
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
            indent=2,
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
        f"Total queries : {total_queries}"
    )

    print(
        f"Rewritten     : {rewritten_count}"
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

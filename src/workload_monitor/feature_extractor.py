from pathlib import Path
import re
import json


QUERY_DIR = Path("workload/queries")


def extract_clause(sql: str, clause: str, next_clauses: list[str]) -> str:
    """
    Extract the text after a SQL clause until the next clause.
    """
    pattern = rf"\b{clause}\b\s+(.+?)"

    if next_clauses:
        stop = "|".join(
            rf"\b{re.escape(c)}\b"
            for c in next_clauses
        )
        pattern += rf"(?=\s+(?:{stop})|;|$)"
    else:
        pattern += r"(?=;|$)"

    match = re.search(pattern, sql, re.IGNORECASE)

    return match.group(1).strip() if match else ""


def extract_features(sql: str) -> dict:
    normalized = re.sub(r"\s+", " ", sql.strip().lower())

    # Remove final semicolon for easier parsing
    normalized = normalized.rstrip(";").strip()

    # ---------------------------------------------------------
    # Tables
    # ---------------------------------------------------------
    tables = set(
        re.findall(
            r"\b(?:from|join)\s+([a-zA-Z_][a-zA-Z0-9_]*)",
            normalized,
            re.IGNORECASE
        )
    )

    # ---------------------------------------------------------
    # JOINs
    # ---------------------------------------------------------
    joins = re.findall(
        r"\bjoin\s+([a-zA-Z_][a-zA-Z0-9_]*)"
        r"\s+on\s+(.+?)"
        r"(?=\s+(?:join|where|group\s+by|order\s+by|having|limit)|$)",
        normalized,
        re.IGNORECASE
    )

    # ---------------------------------------------------------
    # WHERE
    # ---------------------------------------------------------
    filters = extract_clause(
        normalized,
        "where",
        ["group by", "order by", "having", "limit"]
    )

    # ---------------------------------------------------------
    # GROUP BY
    # ---------------------------------------------------------
    group_text = extract_clause(
        normalized,
        "group by",
        ["order by", "having", "limit"]
    )

    group_by = []

    if group_text:
        group_by = [
            column.strip()
            for column in group_text.split(",")
        ]

    # ---------------------------------------------------------
    # Aggregations
    # ---------------------------------------------------------
    aggregates = re.findall(
        r"\b(sum|avg|count|min|max)\s*\(",
        normalized,
        re.IGNORECASE
    )

    return {
        "tables": sorted(tables),
        "num_tables": len(tables),

        "joins": [
            {
                "table": table,
                "condition": condition.strip()
            }
            for table, condition in joins
        ],
        "num_joins": len(joins),

        "filters": filters,
        "has_filter": bool(filters),

        "group_by": group_by,
        "num_group_by": len(group_by),

        "aggregates": sorted(set(aggregates)),
        "num_aggregates": len(aggregates),
    }


def extract_workload():
    workload = []

    for query_file in sorted(QUERY_DIR.glob("q*.sql")):
        sql = query_file.read_text()

        features = extract_features(sql)

        workload.append({
            "query_id": query_file.stem,
            "sql_file": str(query_file),
            "features": features
        })

    return workload


if __name__ == "__main__":
    workload = extract_workload()

    output = Path("workload/workload_features.json")

    output.write_text(
        json.dumps(workload, indent=2)
    )

    print("=" * 60)
    print("WAMVS WORKLOAD FEATURE EXTRACTION")
    print("=" * 60)

    for q in workload:
        f = q["features"]

        print(f"\n{q['query_id']}")
        print(f"  Tables       : {', '.join(f['tables'])}")
        print(f"  Joins        : {f['num_joins']}")
        print(f"  Filter       : {f['has_filter']}")
        print(f"  GROUP BY     : {f['group_by']}")
        print(f"  Aggregates   : {f['aggregates']}")

    print("\n" + "=" * 60)
    print(f"Queries extracted : {len(workload)}")
    print(f"Output            : {output}")
    print("=" * 60)

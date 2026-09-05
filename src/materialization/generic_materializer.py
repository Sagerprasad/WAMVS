from pathlib import Path
import json
import shutil
import time

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


CANDIDATE_FILE = Path("workload/candidate_views.json")
DELTA_DIR = Path("data/tpcds/delta")
OUTPUT_FILE = Path("workload/generic_materialization_metrics.json")


def create_spark():
    builder = (
        SparkSession.builder
        .appName("WAMVS-Generic-Materializer")
        .master("local[8]")
        .config("spark.driver.memory", "6g")
        .config("spark.sql.shuffle.partitions", "64")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension"
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
    )

    return configure_spark_with_delta_pip(builder).getOrCreate()


def register_base_tables(spark):
    print("=" * 70)
    print("REGISTERING BASE DELTA TABLES")
    print("=" * 70)

    for path in sorted(DELTA_DIR.iterdir()):

        # Skip existing materialized views.
        if path.name.startswith("MV_"):
            continue

        if path.is_dir():
            (
                spark.read
                .format("delta")
                .load(str(path))
                .createOrReplaceTempView(path.name)
            )

            print(f"  registered: {path.name}")


def generate_sql(candidate):
    """
    Automatically generate the SQL definition of an MV
    from candidate metadata.
    """

    tables = candidate["tables"]
    group_by = candidate["group_by"]
    joins = candidate["joins"]
    expressions = candidate["measure_expressions"]

    if not tables:
        raise ValueError("Candidate has no tables")

    # ---------------------------------------------------------
    # SELECT
    # ---------------------------------------------------------
    select_columns = []

    for column in group_by:
        select_columns.append(column)

    for measure, expression in expressions.items():
        select_columns.append(
            f"{expression} AS {measure}"
        )

    if not select_columns:
        raise ValueError(
            f"{candidate['candidate_id']} has no SELECT columns"
        )

    select_clause = ",\n    ".join(
        select_columns
    )

    # ---------------------------------------------------------
    # FROM
    #
    # TPC-DS fact tables are the natural starting point.
    # For our current workload this is store_sales.
    # ---------------------------------------------------------
    if "store_sales" in tables:
        from_table = "store_sales"
    else:
        from_table = tables[0]

    sql = (
        "SELECT\n    "
        + select_clause
        + f"\nFROM {from_table}"
    )

    # ---------------------------------------------------------
    # JOINs
    # ---------------------------------------------------------
    for join in joins:
        sql += (
            f"\nJOIN {join['table']}"
            f"\n    ON {join['condition']}"
        )

    # ---------------------------------------------------------
    # GROUP BY
    # ---------------------------------------------------------
    if group_by:
        sql += (
            "\nGROUP BY "
            + ", ".join(group_by)
        )

    sql += ";"

    return sql


def directory_size(path):
    if not path.exists():
        return 0

    return sum(
        f.stat().st_size
        for f in path.rglob("*")
        if f.is_file()
    )


def materialize_candidate(spark, candidate):
    candidate_id = candidate["candidate_id"]

    sql = generate_sql(candidate)

    output_path = DELTA_DIR / candidate_id

    if output_path.exists():
        shutil.rmtree(output_path)

    print("\n" + "=" * 70)
    print(f"MATERIALIZING {candidate_id}")
    print("=" * 70)

    print("\nGenerated SQL:")
    print(sql)

    start = time.perf_counter()

    df = spark.sql(sql)

    row_count = df.count()

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .save(str(output_path))
    )

    elapsed_ms = (
        time.perf_counter() - start
    ) * 1000

    storage_bytes = directory_size(
        output_path
    )

    storage_mb = (
        storage_bytes / (1024 * 1024)
    )

    print(
        f"\nRows       : {row_count:,}"
    )

    print(
        f"Build time : {elapsed_ms:,.2f} ms"
    )

    print(
        f"Storage    : {storage_mb:,.4f} MB"
    )

    return {
        "candidate_id": candidate_id,
        "source_queries": candidate["source_queries"],
        "tables": candidate["tables"],
        "group_by": candidate["group_by"],
        "measures": candidate["measures"],
        "sql": sql,
        "row_count": row_count,
        "materialization_time_ms": round(
            elapsed_ms,
            2
        ),
        "storage_bytes": storage_bytes,
        "storage_mb": round(
            storage_mb,
            4
        ),
        "status": "success",
    }


def main():

    candidates = json.loads(
        CANDIDATE_FILE.read_text()
    )

    spark = create_spark()

    try:
        register_base_tables(spark)

        results = []

        print("\n" + "=" * 70)
        print("WAMVS GENERIC MATERIALIZATION")
        print("=" * 70)

        print(
            f"Candidates : {len(candidates)}"
        )

        # -----------------------------------------------------
        # Materialize every candidate.
        #
        # We intentionally do ALL candidates here.
        # The selector will decide which ones should survive
        # under the storage budget later.
        # -----------------------------------------------------
        for candidate in candidates:

            try:
                result = materialize_candidate(
                    spark,
                    candidate
                )

                results.append(result)

            except Exception as e:

                print(
                    f"\nERROR materializing "
                    f"{candidate['candidate_id']}:"
                )

                print(e)

                results.append({
                    "candidate_id":
                        candidate["candidate_id"],
                    "status": "failed",
                    "error": str(e),
                })

        OUTPUT_FILE.write_text(
            json.dumps(
                results,
                indent=2
            )
        )

        print("\n" + "=" * 70)
        print("GENERIC MATERIALIZATION SUMMARY")
        print("=" * 70)

        for result in results:

            if result["status"] == "success":

                print(
                    f"{result['candidate_id']:8s} | "
                    f"rows={result['row_count']:>8,} | "
                    f"time="
                    f"{result['materialization_time_ms']:>12,.2f} ms | "
                    f"storage="
                    f"{result['storage_mb']:>10.4f} MB"
                )

            else:

                print(
                    f"{result['candidate_id']:8s} | "
                    f"FAILED | "
                    f"{result.get('error', '')}"
                )

        print("-" * 70)

        successful = sum(
            1
            for r in results
            if r["status"] == "success"
        )

        print(
            f"Successful : "
            f"{successful}/{len(results)}"
        )

        print(
            f"Metrics    : "
            f"{OUTPUT_FILE}"
        )

        print("=" * 70)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()

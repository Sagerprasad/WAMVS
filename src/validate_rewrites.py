from pathlib import Path
import json
import time
from decimal import Decimal

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


REWRITE_FILE = Path("workload/rewrite_plan.json")
DELTA_DIR = Path("data/tpcds/delta")


def create_spark():

    builder = (
        SparkSession.builder
        .appName("WAMVS-Rewrite-Validation")
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

    return configure_spark_with_delta_pip(
        builder
    ).getOrCreate()


def register_tables(spark):

    print("=" * 75)
    print("REGISTERING TABLES")
    print("=" * 75)

    for path in sorted(DELTA_DIR.iterdir()):

        if not path.is_dir():
            continue

        (
            spark.read
            .format("delta")
            .load(str(path))
            .createOrReplaceTempView(path.name)
        )

        print(f"  registered: {path.name}")


def normalize_value(value):

    if value is None:
        return "<NULL>"

    if isinstance(value, Decimal):
        return (
            "DECIMAL:"
            + format(value, "f")
        )

    if isinstance(value, float):
        return (
            "FLOAT:"
            + format(value, ".12g")
        )

    if isinstance(value, int):
        return (
            "INT:"
            + str(value)
        )

    return (
        type(value).__name__
        + ":"
        + str(value)
    )


def normalize_rows(rows):

    normalized = []

    for row in rows:

        normalized_row = tuple(
            normalize_value(value)
            for value in row
        )

        normalized.append(
            normalized_row
        )

    # Sorting is now safe because every value
    # is converted to a string.
    return sorted(
        normalized
    )


def execute_query(spark, sql):

    start = time.perf_counter()

    result = (
        spark.sql(sql)
        .collect()
    )

    elapsed_ms = (
        time.perf_counter() - start
    ) * 1000

    return result, elapsed_ms


def main():

    rewrite_plan = json.loads(
        REWRITE_FILE.read_text()
    )

    spark = create_spark()

    try:

        register_tables(spark)

        print("\n" + "=" * 75)
        print("WAMVS REWRITE VALIDATION")
        print("=" * 75)

        results = []

        for plan in rewrite_plan:

            query_id = plan["query_id"]

            if not plan["rewritten"]:
                continue

            original_sql = plan["original_sql"]
            rewritten_sql = plan["rewritten_sql"]

            print("\n" + "-" * 75)
            print(
                f"{query_id} → "
                f"{plan['view']}"
            )

            print("\nOriginal:")
            print(original_sql.strip())

            print("\nRewritten:")
            print(rewritten_sql)

            try:

                original_result, original_time = (
                    execute_query(
                        spark,
                        original_sql
                    )
                )

                rewritten_result, rewritten_time = (
                    execute_query(
                        spark,
                        rewritten_sql
                    )
                )

                original_rows = normalize_rows(
                    original_result
                )

                rewritten_rows = normalize_rows(
                    rewritten_result
                )

                match = (
                    original_rows
                    == rewritten_rows
                )

                if rewritten_time > 0:

                    speedup = (
                        original_time
                        / rewritten_time
                    )

                else:

                    speedup = 0

                status = (
                    "PASS"
                    if match
                    else "FAIL"
                )

                print(
                    f"\nOriginal rows : "
                    f"{len(original_result)}"
                )

                print(
                    f"Rewritten rows: "
                    f"{len(rewritten_result)}"
                )

                print(
                    f"Original time : "
                    f"{original_time:.2f} ms"
                )

                print(
                    f"Rewrite time  : "
                    f"{rewritten_time:.2f} ms"
                )

                print(
                    f"Speedup       : "
                    f"{speedup:.2f}x"
                )

                print(
                    f"Result match  : "
                    f"{status}"
                )

                if not match:

                    print(
                        "\nWARNING: "
                        "Result mismatch."
                    )

                    print(
                        "\nOriginal sample:"
                    )

                    for row in original_result[:5]:
                        print(
                            f"  {row}"
                        )

                    print(
                        "\nRewritten sample:"
                    )

                    for row in rewritten_result[:5]:
                        print(
                            f"  {row}"
                        )

                results.append({

                    "query_id":
                        query_id,

                    "view":
                        plan["view"],

                    "original_rows":
                        len(original_result),

                    "rewritten_rows":
                        len(rewritten_result),

                    "original_time_ms":
                        round(
                            original_time,
                            2
                        ),

                    "rewritten_time_ms":
                        round(
                            rewritten_time,
                            2
                        ),

                    "speedup":
                        round(
                            speedup,
                            3
                        ),

                    "result_match":
                        match,

                    "status":
                        status,
                })

            except Exception as e:

                print(
                    f"\nERROR: {e}"
                )

                results.append({

                    "query_id":
                        query_id,

                    "view":
                        plan["view"],

                    "status":
                        "ERROR",

                    "error":
                        str(e),
                })

        output = Path(
            "workload/rewrite_validation.json"
        )

        output.write_text(
            json.dumps(
                results,
                indent=2
            )
        )

        passed = sum(
            1
            for r in results
            if r["status"] == "PASS"
        )

        failed = sum(
            1
            for r in results
            if r["status"] == "FAIL"
        )

        errors = sum(
            1
            for r in results
            if r["status"] == "ERROR"
        )

        print("\n" + "=" * 75)
        print("VALIDATION SUMMARY")
        print("=" * 75)

        print(
            f"Rewritten queries : "
            f"{len(results)}"
        )

        print(
            f"PASS              : "
            f"{passed}"
        )

        print(
            f"FAIL              : "
            f"{failed}"
        )

        print(
            f"ERROR             : "
            f"{errors}"
        )

        valid = [
            r for r in results
            if r["status"] == "PASS"
        ]

        if valid:

            avg_speedup = (
                sum(
                    r["speedup"]
                    for r in valid
                )
                / len(valid)
            )

            print(
                f"Average speedup   : "
                f"{avg_speedup:.2f}x"
            )

        print(
            f"\nResults saved to: "
            f"{output}"
        )

        print("=" * 75)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()

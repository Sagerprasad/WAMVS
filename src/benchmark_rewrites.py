from pathlib import Path
import json
import statistics
import time

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


REWRITE_FILE = Path("workload/rewrite_plan.json")
DELTA_DIR = Path("data/tpcds/delta")
OUTPUT_FILE = Path("workload/rewrite_benchmark.json")

WARMUP_RUNS = 1
MEASURED_RUNS = 5


def create_spark():

    builder = (
        SparkSession.builder
        .appName("WAMVS-Rewrite-Benchmark")
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


def run_query(spark, sql):

    start = time.perf_counter()

    rows = (
        spark.sql(sql)
        .collect()
    )

    elapsed_ms = (
        time.perf_counter() - start
    ) * 1000

    return rows, elapsed_ms


def normalize_value(value):

    if value is None:
        return "<NULL>"

    return (
        type(value).__name__
        + ":"
        + str(value)
    )


def normalize_rows(rows):

    normalized = []

    for row in rows:

        normalized.append(
            tuple(
                normalize_value(value)
                for value in row
            )
        )

    return sorted(normalized)


def benchmark_query(spark, query_id, original_sql, rewritten_sql):

    print("\n" + "-" * 75)
    print(f"{query_id}")

    # ---------------------------------------------------------
    # Warm-up
    # ---------------------------------------------------------
    print(
        f"Warm-up runs : {WARMUP_RUNS}"
    )

    for _ in range(WARMUP_RUNS):

        run_query(
            spark,
            original_sql
        )

        run_query(
            spark,
            rewritten_sql
        )

    # ---------------------------------------------------------
    # Measure baseline
    # ---------------------------------------------------------
    baseline_times = []
    baseline_result = None

    print(
        f"Baseline runs: {MEASURED_RUNS}"
    )

    for i in range(MEASURED_RUNS):

        result, elapsed = run_query(
            spark,
            original_sql
        )

        if baseline_result is None:
            baseline_result = result

        baseline_times.append(
            elapsed
        )

        print(
            f"  baseline {i + 1}: "
            f"{elapsed:.2f} ms"
        )

    # ---------------------------------------------------------
    # Measure rewritten query
    # ---------------------------------------------------------
    rewritten_times = []
    rewritten_result = None

    print(
        f"MV runs      : {MEASURED_RUNS}"
    )

    for i in range(MEASURED_RUNS):

        result, elapsed = run_query(
            spark,
            rewritten_sql
        )

        if rewritten_result is None:
            rewritten_result = result

        rewritten_times.append(
            elapsed
        )

        print(
            f"  MV       {i + 1}: "
            f"{elapsed:.2f} ms"
        )

    # ---------------------------------------------------------
    # Correctness
    # ---------------------------------------------------------
    result_match = (
        normalize_rows(baseline_result)
        ==
        normalize_rows(rewritten_result)
    )

    # ---------------------------------------------------------
    # Statistics
    # ---------------------------------------------------------
    baseline_median = statistics.median(
        baseline_times
    )

    rewritten_median = statistics.median(
        rewritten_times
    )

    baseline_mean = statistics.mean(
        baseline_times
    )

    rewritten_mean = statistics.mean(
        rewritten_times
    )

    if rewritten_median > 0:

        speedup = (
            baseline_median
            / rewritten_median
        )

    else:

        speedup = 0

    improvement = (
        (
            baseline_median
            - rewritten_median
        )
        / baseline_median
        * 100
    )

    print(
        f"\nMedian baseline : "
        f"{baseline_median:.2f} ms"
    )

    print(
        f"Median MV       : "
        f"{rewritten_median:.2f} ms"
    )

    print(
        f"Speedup         : "
        f"{speedup:.2f}x"
    )

    print(
        f"Improvement     : "
        f"{improvement:.2f}%"
    )

    print(
        f"Result match    : "
        f"{'PASS' if result_match else 'FAIL'}"
    )

    return {

        "query_id":
            query_id,

        "baseline_runs_ms":
            [round(x, 2) for x in baseline_times],

        "mv_runs_ms":
            [round(x, 2) for x in rewritten_times],

        "baseline_mean_ms":
            round(
                baseline_mean,
                2
            ),

        "mv_mean_ms":
            round(
                rewritten_mean,
                2
            ),

        "baseline_median_ms":
            round(
                baseline_median,
                2
            ),

        "mv_median_ms":
            round(
                rewritten_median,
                2
            ),

        "speedup":
            round(
                speedup,
                3
            ),

        "improvement_percent":
            round(
                improvement,
                2
            ),

        "result_match":
            result_match,

        "baseline_rows":
            len(baseline_result),

        "mv_rows":
            len(rewritten_result),
    }


def main():

    rewrite_plan = json.loads(
        REWRITE_FILE.read_text()
    )

    spark = create_spark()

    try:

        register_tables(spark)

        print("\n" + "=" * 75)
        print("WAMVS REPEATED PERFORMANCE BENCHMARK")
        print("=" * 75)

        print(
            f"Warm-up runs : {WARMUP_RUNS}"
        )

        print(
            f"Measured runs: {MEASURED_RUNS}"
        )

        results = []

        for plan in rewrite_plan:

            if not plan["rewritten"]:
                continue

            result = benchmark_query(
                spark,
                plan["query_id"],
                plan["original_sql"],
                plan["rewritten_sql"]
            )

            result["view"] = plan["view"]

            results.append(result)

        # -----------------------------------------------------
        # Overall statistics
        # -----------------------------------------------------
        valid = [
            r for r in results
            if r["result_match"]
        ]

        print("\n" + "=" * 75)
        print("BENCHMARK SUMMARY")
        print("=" * 75)

        print(
            f"Queries benchmarked : "
            f"{len(results)}"
        )

        print(
            f"Correct rewrites    : "
            f"{len(valid)}/{len(results)}"
        )

        if valid:

            median_speedups = [
                r["speedup"]
                for r in valid
            ]

            improvements = [
                r["improvement_percent"]
                for r in valid
            ]

            print(
                f"Average speedup     : "
                f"{statistics.mean(median_speedups):.2f}x"
            )

            print(
                f"Median speedup      : "
                f"{statistics.median(median_speedups):.2f}x"
            )

            print(
                f"Average improvement : "
                f"{statistics.mean(improvements):.2f}%"
            )

        print("\nPer-query results:")
        print("-" * 75)

        for r in results:

            print(
                f"{r['query_id']:5s} → "
                f"{r['view']:7s} | "
                f"baseline="
                f"{r['baseline_median_ms']:8.2f} ms | "
                f"MV="
                f"{r['mv_median_ms']:8.2f} ms | "
                f"speedup="
                f"{r['speedup']:.2f}x | "
                f"{'PASS' if r['result_match'] else 'FAIL'}"
            )

        OUTPUT_FILE.write_text(
            json.dumps(
                results,
                indent=2
            )
        )

        print(
            f"\nResults saved to: "
            f"{OUTPUT_FILE}"
        )

        print("=" * 75)

    finally:

        spark.stop()


if __name__ == "__main__":
    main()

from pathlib import Path
import json
import shutil
import time

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


DELTA_DIR = Path("data/tpcds/delta")
INCREMENTAL_DIR = Path("data/tpcds/incremental")
OUTPUT_FILE = Path(
    "workload/maintenance_benchmark.json"
)

TARGET_VIEW = "MV_005"

BATCH_SIZES = [
    100,
    500,
    1000,
    5000,
    10000,
]


def create_spark():

    builder = (
        SparkSession.builder
        .appName("WAMVS-Maintenance-Benchmark")
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


def register_base_tables(spark):

    for path in sorted(DELTA_DIR.iterdir()):

        if (
            not path.is_dir()
            or path.name.startswith("MV_")
        ):
            continue

        (
            spark.read
            .format("delta")
            .load(str(path))
            .createOrReplaceTempView(path.name)
        )


def create_batch(
    spark,
    size,
    seed
):

    """
    Create a deterministic incoming batch.

    Rows are selected from valid store_sales/item
    combinations so the MV join semantics remain valid.
    """

    return spark.sql(f"""
        SELECT
            s.ss_item_sk,
            s.ss_ext_sales_price,
            s.ss_quantity,
            s.ss_sales_price
        FROM store_sales s
        INNER JOIN item i
          ON s.ss_item_sk = i.i_item_sk
        ORDER BY
            s.ss_item_sk,
            s.ss_ext_sales_price
        LIMIT {size}
    """)


def write_batch(
    batch,
    size
):

    INCREMENTAL_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    output = (
        INCREMENTAL_DIR
        / f"batch_{size}"
    )

    if output.exists():
        shutil.rmtree(output)

    (
        batch.write
        .format("delta")
        .mode("overwrite")
        .save(str(output))
    )

    return output


def full_refresh(
    spark,
    batch
):

    batch.createOrReplaceTempView(
        "benchmark_batch"
    )

    sql = """
        SELECT
            i.i_category,
            i.i_class,
            SUM(s.ss_ext_sales_price) AS revenue
        FROM (
            SELECT
                ss_item_sk,
                ss_ext_sales_price
            FROM store_sales

            UNION ALL

            SELECT
                ss_item_sk,
                ss_ext_sales_price
            FROM benchmark_batch
        ) s

        INNER JOIN item i
          ON s.ss_item_sk = i.i_item_sk

        GROUP BY
            i.i_category,
            i.i_class
    """

    start = time.perf_counter()

    result = (
        spark.sql(sql)
        .cache()
    )

    rows = result.count()

    elapsed = (
        time.perf_counter()
        - start
    ) * 1000

    return result, rows, elapsed


def incremental_refresh(
    spark,
    batch
):

    batch.createOrReplaceTempView(
        "benchmark_batch"
    )

    sql = """
        SELECT
            i.i_category,
            i.i_class,
            SUM(s.ss_ext_sales_price) AS revenue
        FROM benchmark_batch s

        INNER JOIN item i
          ON s.ss_item_sk = i.i_item_sk

        GROUP BY
            i.i_category,
            i.i_class
    """

    start = time.perf_counter()

    contribution = (
        spark.sql(sql)
        .cache()
    )

    contribution_rows = (
        contribution.count()
    )

    existing = (
        spark.read
        .format("delta")
        .load(
            str(
                DELTA_DIR
                / TARGET_VIEW
            )
        )
    )

    updated = (
        existing
        .unionByName(
            contribution
        )
        .groupBy(
            "i_category",
            "i_class"
        )
        .sum("revenue")
        .withColumnRenamed(
            "sum(revenue)",
            "revenue"
        )
        .cache()
    )

    updated.count()

    elapsed = (
        time.perf_counter()
        - start
    ) * 1000

    return (
        updated,
        contribution_rows,
        elapsed
    )


def check_correctness(
    updated,
    full
):

    updated.createOrReplaceTempView(
        "_updated_check"
    )

    full.createOrReplaceTempView(
        "_full_check"
    )

    differences = spark.sql("""
        SELECT
            u.i_category,
            u.i_class,
            u.revenue AS updated_revenue,
            f.revenue AS full_revenue
        FROM _updated_check u

        FULL OUTER JOIN _full_check f
          ON u.i_category <=> f.i_category
         AND u.i_class <=> f.i_class

        WHERE
            u.revenue IS NULL
            OR f.revenue IS NULL
            OR ABS(
                COALESCE(u.revenue, 0)
                -
                COALESCE(f.revenue, 0)
            ) > 0.01
    """)

    return differences.count()


def main():

    global spark

    spark = create_spark()

    try:

        register_base_tables(
            spark
        )

        print("=" * 80)
        print("WAMVS INCREMENTAL MAINTENANCE BENCHMARK")
        print("=" * 80)

        results = []

        for size in BATCH_SIZES:

            print(
                "\n" + "-" * 80
            )

            print(
                f"Batch size: {size:,} rows"
            )

            batch = create_batch(
                spark,
                size,
                size
            )

            actual_rows = batch.count()

            print(
                f"Actual batch rows: "
                f"{actual_rows:,}"
            )

            write_batch(
                batch,
                size
            )

            # ----------------------------------------------
            # Warm-up
            # ----------------------------------------------

            warm_full, _, _ = full_refresh(
                spark,
                batch
            )

            warm_full.unpersist()

            warm_inc, _, _ = incremental_refresh(
                spark,
                batch
            )

            warm_inc.unpersist()

            # ----------------------------------------------
            # Measured full refresh
            # ----------------------------------------------

            full_mv, full_groups, full_time = (
                full_refresh(
                    spark,
                    batch
                )
            )

            # ----------------------------------------------
            # Measured incremental
            # ----------------------------------------------

            incremental_mv, incremental_groups, inc_time = (
                incremental_refresh(
                    spark,
                    batch
                )
            )

            # ----------------------------------------------
            # Correctness
            # ----------------------------------------------

            differences = check_correctness(
                incremental_mv,
                full_mv
            )

            correct = (
                differences == 0
            )

            speedup = (
                full_time / inc_time
                if inc_time > 0
                else 0
            )

            reduction = (
                (
                    full_time
                    - inc_time
                )
                / full_time
                * 100
                if full_time > 0
                else 0
            )

            result = {

                "batch_size":
                    actual_rows,

                "full_refresh_groups":
                    full_groups,

                "incremental_groups":
                    incremental_groups,

                "full_refresh_time_ms":
                    round(
                        full_time,
                        2
                    ),

                "incremental_refresh_time_ms":
                    round(
                        inc_time,
                        2
                    ),

                "speedup":
                    round(
                        speedup,
                        2
                    ),

                "time_reduction_percent":
                    round(
                        reduction,
                        2
                    ),

                "differences":
                    differences,

                "correctness":
                    correct,
            }

            results.append(
                result
            )

            print(
                f"Full refresh : "
                f"{full_time:.2f} ms"
            )

            print(
                f"Incremental  : "
                f"{inc_time:.2f} ms"
            )

            print(
                f"Speedup       : "
                f"{speedup:.2f}x"
            )

            print(
                f"Reduction     : "
                f"{reduction:.2f}%"
            )

            print(
                f"Correctness   : "
                f"{'PASS' if correct else 'FAIL'}"
            )

            print(
                f"Differences   : "
                f"{differences}"
            )

            full_mv.unpersist()
            incremental_mv.unpersist()

        # --------------------------------------------------
        # Save results
        # --------------------------------------------------

        summary = {

            "target_view":
                TARGET_VIEW,

            "batch_sizes":
                BATCH_SIZES,

            "results":
                results,

            "all_correct":
                all(
                    r["correctness"]
                    for r in results
                ),
        }

        OUTPUT_FILE.write_text(
            json.dumps(
                summary,
                indent=2
            )
        )

        print("\n" + "=" * 80)
        print("BENCHMARK SUMMARY")
        print("=" * 80)

        for r in results:

            print(
                f"{r['batch_size']:>6,} rows | "
                f"Full {r['full_refresh_time_ms']:>8.2f} ms | "
                f"Inc {r['incremental_refresh_time_ms']:>8.2f} ms | "
                f"{r['speedup']:>5.2f}x | "
                f"{r['time_reduction_percent']:>6.2f}% | "
                f"{'PASS' if r['correctness'] else 'FAIL'}"
            )

        print(
            f"\nAll correctness checks: "
            f"{'PASS' if summary['all_correct'] else 'FAIL'}"
        )

        print(
            f"Results saved to: "
            f"{OUTPUT_FILE}"
        )

        print("=" * 80)

    finally:

        spark.stop()


if __name__ == "__main__":
    main()

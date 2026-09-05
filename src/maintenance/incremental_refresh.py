from pathlib import Path
import json
import shutil
import time

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


DELTA_DIR = Path("data/tpcds/delta")
INCREMENTAL_DIR = Path("data/tpcds/incremental")

METRICS_FILE = Path(
    "workload/incremental_refresh_metrics.json"
)

TARGET_VIEW = "MV_005"


def create_spark():

    builder = (
        SparkSession.builder
        .appName("WAMVS-Incremental-Maintenance")
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

        if not path.is_dir():
            continue

        if path.name.startswith("MV_"):
            continue

        (
            spark.read
            .format("delta")
            .load(str(path))
            .createOrReplaceTempView(path.name)
        )


def create_incremental_batch(
    spark,
    batch_size=1000
):
    """
    Create a synthetic append-only batch containing
    ONLY store_sales rows whose item key exists in
    the item dimension.

    This guarantees that the incremental batch follows
    the same INNER JOIN semantics as MV_005.
    """

    source = spark.sql("""
        SELECT
            s.ss_item_sk,
            s.ss_ext_sales_price,
            s.ss_quantity,
            s.ss_sales_price
        FROM store_sales s
        INNER JOIN item i
          ON s.ss_item_sk = i.i_item_sk
        LIMIT 1000
    """)

    batch = source.limit(
        batch_size
    )

    INCREMENTAL_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    output = (
        INCREMENTAL_DIR
        / "store_sales_batch"
    )

    if output.exists():
        shutil.rmtree(output)

    (
        batch.write
        .format("delta")
        .mode("overwrite")
        .save(str(output))
    )

    batch.createOrReplaceTempView(
        "new_store_sales"
    )

    return batch


def compute_full_refresh(spark):

    """
    Ground-truth MV after the new batch arrives.

    This recomputes the complete materialized view
    from original data + incoming data.
    """

    return spark.sql("""
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
            FROM new_store_sales
        ) s

        INNER JOIN item i
          ON s.ss_item_sk = i.i_item_sk

        GROUP BY
            i.i_category,
            i.i_class
    """)


def compute_incremental_contribution(spark):

    """
    Compute ONLY the contribution of the incoming batch.
    """

    return spark.sql("""
        SELECT
            i.i_category,
            i.i_class,
            SUM(s.ss_ext_sales_price) AS revenue
        FROM new_store_sales s

        INNER JOIN item i
          ON s.ss_item_sk = i.i_item_sk

        GROUP BY
            i.i_category,
            i.i_class
    """)


def build_updated_mv(
    spark,
    incremental_contribution
):

    """
    Existing MV + contribution from new records.
    """

    existing_mv = (
        spark.read
        .format("delta")
        .load(
            str(
                DELTA_DIR
                / TARGET_VIEW
            )
        )
    )

    updated_mv = (
        existing_mv
        .unionByName(
            incremental_contribution
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
    )

    return (
        existing_mv,
        updated_mv
    )


def compare_results(
    spark,
    updated_mv,
    full_mv
):
    """
    Compare incremental and full-refresh results using
    NULL-safe equality.

    TPC-DS dimension attributes such as i_category and
    i_class may legitimately be NULL.
    """

    from pyspark.sql.functions import expr

    updated = updated_mv.selectExpr(
        "i_category",
        "i_class",
        "ROUND(revenue, 2) AS revenue"
    )

    full = full_mv.selectExpr(
        "i_category",
        "i_class",
        "ROUND(revenue, 2) AS revenue"
    )

    updated.createOrReplaceTempView(
        "_updated_mv_check"
    )

    full.createOrReplaceTempView(
        "_full_mv_check"
    )

    differences = spark.sql("""
        SELECT
            u.i_category,
            u.i_class,
            u.revenue AS updated_revenue,
            f.revenue AS full_revenue
        FROM _updated_mv_check u
        FULL OUTER JOIN _full_mv_check f
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

    spark = create_spark()

    try:

        register_base_tables(
            spark
        )

        print("=" * 80)
        print("WAMVS INCREMENTAL MAINTENANCE")
        print("=" * 80)

        # --------------------------------------------------
        # 1. Incoming data
        # --------------------------------------------------

        print(
            "\nCreating incremental store_sales batch..."
        )

        batch = create_incremental_batch(
            spark,
            batch_size=1000
        )

        batch_count = batch.count()

        print(
            f"New rows: {batch_count}"
        )

        # --------------------------------------------------
        # 2. Full refresh
        # --------------------------------------------------

        print(
            "\nRunning FULL refresh..."
        )

        start = time.perf_counter()

        full_mv = (
            compute_full_refresh(
                spark
            )
            .cache()
        )

        full_rows = full_mv.count()

        full_time = (
            time.perf_counter()
            - start
        ) * 1000

        print(
            f"Full refresh: "
            f"{full_time:.2f} ms"
        )

        # --------------------------------------------------
        # 3. Incremental computation
        # --------------------------------------------------

        print(
            "\nRunning INCREMENTAL refresh..."
        )

        start = time.perf_counter()

        incremental_contribution = (
            compute_incremental_contribution(
                spark
            )
            .cache()
        )

        contribution_rows = (
            incremental_contribution.count()
        )

        incremental_time = (
            time.perf_counter()
            - start
        ) * 1000

        print(
            f"Incremental computation: "
            f"{incremental_time:.2f} ms"
        )

        # --------------------------------------------------
        # 4. Apply contribution
        # --------------------------------------------------

        existing_mv, updated_mv = (
            build_updated_mv(
                spark,
                incremental_contribution
            )
        )

        updated_rows = (
            updated_mv.count()
        )

        # --------------------------------------------------
        # 5. Correctness
        # --------------------------------------------------

        print(
            "\nChecking correctness..."
        )

        differences = compare_results(
            spark,
            updated_mv,
            full_mv
        )

        correct = (
            differences == 0
        )

        print(
            f"Correctness: "
            f"{'PASS' if correct else 'FAIL'}"
        )

        print(
            f"Differences: "
            f"{differences}"
        )

        # --------------------------------------------------
        # 6. Performance
        # --------------------------------------------------

        if incremental_time > 0:

            speedup = (
                full_time
                / incremental_time
            )

        else:

            speedup = 0.0

        time_reduction = (
            (
                full_time
                - incremental_time
            )
            / full_time
            * 100
        )

        # --------------------------------------------------
        # 7. Metrics
        # --------------------------------------------------

        metrics = {

            "target_view":
                TARGET_VIEW,

            "batch_rows":
                batch_count,

            "existing_mv_rows":
                existing_mv.count(),

            "incremental_contribution_groups":
                contribution_rows,

            "full_refresh_groups":
                full_rows,

            "updated_mv_groups":
                updated_rows,

            "full_refresh_time_ms":
                round(
                    full_time,
                    2
                ),

            "incremental_refresh_time_ms":
                round(
                    incremental_time,
                    2
                ),

            "maintenance_speedup":
                round(
                    speedup,
                    2
                ),

            "time_reduction_percent":
                round(
                    time_reduction,
                    2
                ),

            "correctness":
                correct,

            "differences":
                differences,
        }

        METRICS_FILE.write_text(
            json.dumps(
                metrics,
                indent=2
            )
        )

        # --------------------------------------------------
        # 8. Final report
        # --------------------------------------------------

        print("\n" + "=" * 80)
        print("INCREMENTAL MAINTENANCE RESULTS")
        print("=" * 80)

        print(
            f"Batch rows             : "
            f"{batch_count}"
        )

        print(
            f"Existing MV groups    : "
            f"{existing_mv.count()}"
        )

        print(
            f"Incremental groups    : "
            f"{contribution_rows}"
        )

        print(
            f"Full refresh groups   : "
            f"{full_rows}"
        )

        print(
            f"Updated MV groups     : "
            f"{updated_rows}"
        )

        print(
            f"Full refresh          : "
            f"{full_time:.2f} ms"
        )

        print(
            f"Incremental          : "
            f"{incremental_time:.2f} ms"
        )

        print(
            f"Speedup               : "
            f"{speedup:.2f}x"
        )

        print(
            f"Time reduction        : "
            f"{time_reduction:.2f}%"
        )

        print(
            f"Correctness           : "
            f"{'PASS' if correct else 'FAIL'}"
        )

        print(
            f"Differences           : "
            f"{differences}"
        )

        print(
            f"\nMetrics written to: "
            f"{METRICS_FILE}"
        )

        print("=" * 80)

    finally:

        spark.stop()


if __name__ == "__main__":
    main()

import json
import statistics
import shutil
import time
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from pyspark.sql import functions as F


BASE_PATH = Path("data/tpcds/delta")
WORK_ROOT = Path("data/tpcds/incremental_maintenance")
RESULT_PATH = Path("results/incremental_maintenance_benchmark.json")

NEW_DATA_FRACTIONS = {
    "1pct": 0.01,
    "5pct": 0.05,
    "10pct": 0.10,
}

def create_spark():
    builder = (
        SparkSession.builder
        .appName("WAMVS-Incremental-Maintenance")
        .master("local[8]")
        .config("spark.driver.memory", "6g")
        .config("spark.sql.shuffle.partitions", "64")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def remove(path):
    if path.exists():
        shutil.rmtree(path)


def timed_write(df, path):
    remove(path)
    start = time.perf_counter()
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .save(str(path))
    )
    elapsed = (time.perf_counter() - start) * 1000
    return elapsed


def normalize(rows):
    result = {}
    for r in rows:
        result[r["i_category"]] = {
            "revenue": float(r["revenue"] or 0),
            "profit": float(r["profit"] or 0),
            "quantity": float(r["quantity"] or 0),
            "avg_price": float(r["avg_price"] or 0),
            "row_count": int(r["row_count"] or 0),
        }
    return result


def max_difference(a, b):
    categories = set(a) | set(b)
    max_diff = 0.0

    for category in categories:
        if category not in a or category not in b:
            return float("inf")

        for key in ["revenue", "profit", "quantity", "avg_price"]:
            diff = abs(a[category][key] - b[category][key])
            max_diff = max(max_diff, diff)

        if a[category]["row_count"] != b[category]["row_count"]:
            return float("inf")

    return max_diff


def main():
    print("=" * 80)
    print("WAMVS INCREMENTAL MAINTENANCE BENCHMARK")
    print("=" * 80)

    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    # ------------------------------------------------------------------
    # Load source data
    # ------------------------------------------------------------------
    store_sales = (
        spark.read
        .format("delta")
        .load(str(BASE_PATH / "store_sales"))
    )

    item = (
        spark.read
        .format("delta")
        .load(str(BASE_PATH / "item"))
    )

    store_sales.createOrReplaceTempView("store_sales")
    item.createOrReplaceTempView("item")

    print(f"Base store_sales rows: {store_sales.count():,}")
    print(f"Item rows: {item.count():,}")

    WORK_ROOT.mkdir(parents=True, exist_ok=True)

    results = {}

    # ------------------------------------------------------------------
    # Build the initial materialized view.
    #
    # We retain row_count so AVG can be maintained correctly without
    # rescanning the complete source table.
    # ------------------------------------------------------------------
    base_mv = (
        store_sales.alias("ss")
        .join(
            item.alias("i"),
            F.col("ss.ss_item_sk") == F.col("i.i_item_sk"),
        )
        .groupBy("i.i_category")
        .agg(
            F.sum("ss.ss_ext_sales_price").alias("revenue"),
            F.sum("ss.ss_net_profit").alias("profit"),
            F.sum("ss.ss_quantity").alias("quantity"),
            F.sum("ss.ss_sales_price").alias("price_sum"),
            F.count(F.lit(1)).alias("row_count"),
        )
        .withColumn(
            "avg_price",
            F.col("price_sum") / F.col("row_count"),
        )
        .drop("price_sum")
        .cache()
    )

    base_mv.count()

    initial_path = WORK_ROOT / "initial_mv"
    remove(initial_path)

    (
        base_mv.write
        .format("delta")
        .mode("overwrite")
        .save(str(initial_path))
    )

    base_result = normalize(
        base_mv.select(
            "i_category",
            "revenue",
            "profit",
            "quantity",
            "avg_price",
            "row_count",
        ).collect()
    )

    print(f"Initial MV groups: {len(base_result)}")
    print()

    # ------------------------------------------------------------------
    # Test several update sizes
    # ------------------------------------------------------------------
    for label, fraction in NEW_DATA_FRACTIONS.items():
        print("=" * 80)
        print(f"UPDATE SIZE: {label} ({fraction * 100:.0f}%)")
        print("=" * 80)

        update_path = WORK_ROOT / f"new_data_{label}"
        full_path = WORK_ROOT / f"full_refresh_{label}"
        inc_path = WORK_ROOT / f"incremental_refresh_{label}"

        remove(update_path)
        remove(full_path)
        remove(inc_path)

        # Deterministic subset of store_sales used as newly arriving data.
        #
        # xxhash64 provides a stable pseudo-random partition without
        # requiring a full order-by.
        new_data = (
            store_sales
            .where(
                F.pmod(
                    F.abs(F.xxhash64(*store_sales.columns)),
                    F.lit(10000),
                ) < int(fraction * 10000)
            )
            .cache()
        )

        new_count = new_data.count()

        (
            new_data.write
            .format("delta")
            .mode("overwrite")
            .save(str(update_path))
        )

        print(f"New rows: {new_count:,}")

        # ==============================================================
        # FULL REFRESH
        # Recompute the complete MV from base + new data.
        # ==============================================================

        combined = store_sales.unionByName(new_data)

        full_agg = (
            combined.alias("ss")
            .join(
                item.alias("i"),
                F.col("ss.ss_item_sk") == F.col("i.i_item_sk"),
            )
            .groupBy("i.i_category")
            .agg(
                F.sum("ss.ss_ext_sales_price").alias("revenue"),
                F.sum("ss.ss_net_profit").alias("profit"),
                F.sum("ss.ss_quantity").alias("quantity"),
                F.sum("ss.ss_sales_price").alias("price_sum"),
                F.count(F.lit(1)).alias("row_count"),
            )
            .withColumn(
                "avg_price",
                F.col("price_sum") / F.col("row_count"),
            )
            .drop("price_sum")
        )

        full_runs = []

        for run in range(3):
            elapsed = timed_write(full_agg, full_path)
            full_runs.append(elapsed)
            print(f"  Full refresh {run + 1}: {elapsed:.2f} ms")

        full_median = statistics.median(full_runs)

        full_result = normalize(
            spark.read
            .format("delta")
            .load(str(full_path))
            .select(
                "i_category",
                "revenue",
                "profit",
                "quantity",
                "avg_price",
                "row_count",
            )
            .collect()
        )

        # ==============================================================
        # INCREMENTAL REFRESH
        #
        # Only the new rows are joined/aggregated. The existing MV is
        # combined with those small aggregates.
        # ==============================================================

        incremental_delta = (
            new_data.alias("ss")
            .join(
                item.alias("i"),
                F.col("ss.ss_item_sk") == F.col("i.i_item_sk"),
            )
            .groupBy("i.i_category")
            .agg(
                F.sum("ss.ss_ext_sales_price").alias("revenue"),
                F.sum("ss.ss_net_profit").alias("profit"),
                F.sum("ss.ss_quantity").alias("quantity"),
                F.sum("ss.ss_sales_price").alias("price_sum"),
                F.count(F.lit(1)).alias("row_count"),
            )
        )

        existing_mv = spark.read.format("delta").load(str(initial_path))

        incremental_result_df = (
            existing_mv
            .select(
                "i_category",
                "revenue",
                "profit",
                "quantity",
                (F.col("avg_price") * F.col("row_count")).alias("price_sum"),
                "row_count",
            )
            .unionByName(incremental_delta)
            .groupBy("i_category")
            .agg(
                F.sum("revenue").alias("revenue"),
                F.sum("profit").alias("profit"),
                F.sum("quantity").alias("quantity"),
                F.sum("price_sum").alias("price_sum"),
                F.sum("row_count").alias("row_count"),
            )
            .withColumn(
                "avg_price",
                F.col("price_sum") / F.col("row_count"),
            )
            .drop("price_sum")
        )

        incremental_runs = []

        for run in range(3):
            elapsed = timed_write(
                incremental_result_df,
                inc_path,
            )
            incremental_runs.append(elapsed)
            print(f"  Incremental refresh {run + 1}: {elapsed:.2f} ms")

        incremental_median = statistics.median(incremental_runs)

        inc_result = normalize(
            spark.read
            .format("delta")
            .load(str(inc_path))
            .select(
                "i_category",
                "revenue",
                "profit",
                "quantity",
                "avg_price",
                "row_count",
            )
            .collect()
        )

        difference = max_difference(full_result, inc_result)

        speedup = full_median / incremental_median

        print(f"  Full median       : {full_median:.2f} ms")
        print(f"  Incremental median: {incremental_median:.2f} ms")
        print(f"  Incremental speedup: {speedup:.2f}x")
        print(f"  Max result difference: {difference:.10f}")
        print()

        results[label] = {
            "fraction": fraction,
            "new_rows": new_count,
            "full_refresh_runs_ms": [
                round(x, 2) for x in full_runs
            ],
            "full_refresh_median_ms": round(full_median, 2),
            "incremental_refresh_runs_ms": [
                round(x, 2) for x in incremental_runs
            ],
            "incremental_refresh_median_ms": round(
                incremental_median, 2
            ),
            "incremental_speedup": round(speedup, 3),
            "max_result_difference": difference,
            "correct": difference < 1e-6,
        }

        new_data.unpersist()

    # ------------------------------------------------------------------
    # Save results
    # ------------------------------------------------------------------
    output = {
        "dataset": "TPC-DS SF1",
        "source_tables": ["store_sales", "item"],
        "materialized_view": {
            "type": "aggregation",
            "group_by": ["i_category"],
            "measures": [
                "revenue",
                "profit",
                "quantity",
                "avg_price",
            ],
        },
        "methodology": {
            "update_sizes": ["1pct", "5pct", "10pct"],
            "runs_per_method": 3,
            "runtime_statistic": "median",
            "comparison": [
                "full_refresh",
                "incremental_refresh",
            ],
            "correctness_tolerance": 1e-6,
        },
        "results": results,
    }

    RESULT_PATH.parent.mkdir(exist_ok=True)

    with open(RESULT_PATH, "w") as f:
        json.dump(output, f, indent=2)

    print("=" * 80)
    print("INCREMENTAL MAINTENANCE SUMMARY")
    print("=" * 80)

    for label, r in results.items():
        print(
            f"{label}: "
            f"{r['new_rows']:,} new rows | "
            f"Full {r['full_refresh_median_ms']:.2f} ms | "
            f"Incremental {r['incremental_refresh_median_ms']:.2f} ms | "
            f"{r['incremental_speedup']:.2f}x | "
            f"Correct={r['correct']}"
        )

    print()
    print(f"Saved: {RESULT_PATH}")

    spark.stop()


if __name__ == "__main__":
    main()

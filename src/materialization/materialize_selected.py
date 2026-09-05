from pathlib import Path
import json
import time

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


DELTA_DIR = Path("data/tpcds/delta")
SELECTED_FILE = Path("workload/selected_views.json")
METRICS_FILE = Path("workload/mv_materialization_metrics.json")


def create_spark():
    builder = (
        SparkSession.builder
        .appName("WAMVS-Materialize-Selected")
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


def register_tables(spark):
    for path in sorted(DELTA_DIR.iterdir()):
        if path.is_dir():
            path.name
            (
                spark.read
                .format("delta")
                .load(str(path))
                .createOrReplaceTempView(path.name)
            )


def get_view_sql(candidate):
    tables = candidate["tables"]
    group_by = candidate["group_by"]
    aggregates = candidate["aggregates"]

    # ---------------------------------------------------------
    # MV_003
    # store_sales + item + date_dim
    # GROUP BY d_year, i_category
    # ---------------------------------------------------------
    if (
        set(tables) == {"date_dim", "item", "store_sales"}
        and set(group_by) == {"d_year", "i_category"}
    ):
        return """
        SELECT
            d_year,
            i_category,
            SUM(ss_ext_sales_price) AS revenue
        FROM store_sales
        JOIN item
            ON ss_item_sk = i_item_sk
        JOIN date_dim
            ON ss_sold_date_sk = d_date_sk
        GROUP BY d_year, i_category
        """

    # ---------------------------------------------------------
    # MV_007
    # store_sales
    # GROUP BY ss_store_sk
    # ---------------------------------------------------------
    if (
        tables == ["store_sales"]
        and group_by == ["ss_store_sk"]
    ):
        return """
        SELECT
            ss_store_sk,
            SUM(ss_ext_sales_price) AS revenue,
            SUM(ss_net_profit) AS profit
        FROM store_sales
        GROUP BY ss_store_sk
        """

    return None


def directory_size(path):
    if not path.exists():
        return 0

    return sum(
        file.stat().st_size
        for file in path.rglob("*")
        if file.is_file()
    )


def materialize(spark, candidate):
    view_id = candidate["candidate_id"]

    sql = get_view_sql(candidate)

    if sql is None:
        return {
            "candidate_id": view_id,
            "status": "skipped",
            "reason": "No SQL generator available"
        }

    output_path = Path("data/tpcds/delta") / view_id

    # Remove previous prototype version if it exists.
    if output_path.exists():
        import shutil
        shutil.rmtree(output_path)

    print("\n" + "=" * 70)
    print(f"MATERIALIZING {view_id}")
    print("=" * 70)

    print("SQL:")
    print(sql.strip())

    start = time.perf_counter()

    df = spark.sql(sql)

    row_count = df.count()

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .save(str(output_path))
    )

    elapsed_ms = (time.perf_counter() - start) * 1000

    size_bytes = directory_size(output_path)
    size_mb = size_bytes / (1024 * 1024)

    print(f"\nRows       : {row_count:,}")
    print(f"Runtime    : {elapsed_ms:,.2f} ms")
    print(f"Storage    : {size_mb:,.2f} MB")
    print(f"Delta path : {output_path}")

    return {
        "candidate_id": view_id,
        "status": "materialized",
        "row_count": row_count,
        "materialization_time_ms": round(elapsed_ms, 2),
        "storage_bytes": size_bytes,
        "storage_mb": round(size_mb, 4),
        "delta_path": str(output_path)
    }


def main():
    spark = create_spark()

    try:
        register_tables(spark)

        selected = json.loads(
            SELECTED_FILE.read_text()
        )

        print("=" * 70)
        print("WAMVS SELECTED VIEW MATERIALIZATION")
        print("=" * 70)

        print(f"Selected candidates : {len(selected)}")

        results = []

        for candidate in selected:
            result = materialize(
                spark,
                candidate
            )
            results.append(result)

        METRICS_FILE.write_text(
            json.dumps(results, indent=2)
        )

        print("\n" + "=" * 70)
        print("MATERIALIZATION SUMMARY")
        print("=" * 70)

        for result in results:
            if result["status"] == "materialized":
                print(
                    f"{result['candidate_id']:8s} | "
                    f"rows={result['row_count']:>8,} | "
                    f"time={result['materialization_time_ms']:>12,.2f} ms | "
                    f"storage={result['storage_mb']:>10.4f} MB"
                )
            else:
                print(
                    f"{result['candidate_id']:8s} | "
                    f"{result['status']}"
                )

        print("-" * 70)
        print(f"Metrics file : {METRICS_FILE}")
        print("=" * 70)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()

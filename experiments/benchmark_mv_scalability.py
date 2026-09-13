import json
import statistics
import time
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


SCALES = {
    "SF1": Path("data/tpcds/delta"),
    "SF10": Path("data/tpcds/delta_sf10"),
}

MV_PATHS = {
    "SF1": Path("data/tpcds/scalability_mv_sf1"),
    "SF10": Path("data/tpcds/scalability_mv_sf10"),
}


def create_spark(app_name):

    builder = (
        SparkSession.builder
        .appName(app_name)
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

    return (
        configure_spark_with_delta_pip(builder)
        .getOrCreate()
    )


def register_tables(spark, root):

    for table in [
        "store_sales",
        "item",
    ]:

        (
            spark.read
            .format("delta")
            .load(str(root / table))
            .createOrReplaceTempView(table)
        )


def directory_size(path):

    if not path.exists():
        return 0

    return sum(
        p.stat().st_size
        for p in path.rglob("*")
        if p.is_file()
    )


def main():

    results = {}

    sql = """
    SELECT
        i.i_category,
        SUM(ss.ss_ext_sales_price) AS revenue,
        SUM(ss.ss_net_profit) AS profit,
        SUM(ss.ss_quantity) AS total_quantity,
        AVG(ss.ss_sales_price) AS avg_price
    FROM store_sales ss
    JOIN item i
        ON ss.ss_item_sk = i.i_item_sk
    GROUP BY i.i_category
    """

    for scale, root in SCALES.items():

        print()
        print("=" * 80)
        print(f"{scale} MV SCALABILITY")
        print("=" * 80)

        spark = create_spark(
            f"WAMVS-MV-Scalability-{scale}"
        )

        spark.sparkContext.setLogLevel(
            "WARN"
        )

        register_tables(
            spark,
            root
        )

        output_path = MV_PATHS[scale]

        if output_path.exists():
            import shutil
            shutil.rmtree(output_path)

        # Warmup
        spark.sql(sql).collect()

        runs = []

        for run in range(3):

            import shutil

            if output_path.exists():
                shutil.rmtree(output_path)

            start = time.perf_counter()

            (
                spark.sql(sql)
                .write
                .format("delta")
                .mode("overwrite")
                .save(str(output_path))
            )

            elapsed = (
                time.perf_counter()
                - start
            ) * 1000

            runs.append(elapsed)

            print(
                f"  Build {run + 1}: "
                f"{elapsed:.2f} ms"
            )

        median_build = statistics.median(
            runs
        )

        size_bytes = directory_size(
            output_path
        )

        size_mb = (
            size_bytes /
            (1024 * 1024)
        )

        row_count = (
            spark.read
            .format("delta")
            .load(str(output_path))
            .count()
        )

        results[scale] = {
            "build_runs_ms": [
                round(x, 2)
                for x in runs
            ],
            "median_build_ms": round(
                median_build,
                2
            ),
            "storage_mb": round(
                size_mb,
                4
            ),
            "row_count": row_count,
        }

        print(
            f"  Median build: "
            f"{median_build:.2f} ms"
        )

        print(
            f"  MV storage: "
            f"{size_mb:.4f} MB"
        )

        print(
            f"  MV rows: "
            f"{row_count}"
        )

        spark.stop()

    # --------------------------------------------------------
    # Scaling comparison
    # --------------------------------------------------------

    sf1 = results["SF1"]
    sf10 = results["SF10"]

    results["comparison"] = {
        "build_time_growth":
            round(
                sf10["median_build_ms"]
                / sf1["median_build_ms"],
                3
            ),

        "storage_growth":
            round(
                sf10["storage_mb"]
                / sf1["storage_mb"],
                3
            ),
    }

    output = {
        "methodology": {
            "view": "MV_001-equivalent",
            "source_tables": [
                "store_sales",
                "item",
            ],
            "build_runs": 3,
            "runtime_statistic": "median",
        },
        "scales": results,
    }

    Path("results").mkdir(
        exist_ok=True
    )

    output_path = (
        Path("results") /
        "mv_scalability_benchmark.json"
    )

    with open(output_path, "w") as f:
        json.dump(
            output,
            f,
            indent=2
        )

    print()
    print("=" * 80)
    print("MV SCALABILITY SUMMARY")
    print("=" * 80)

    print(
        f"SF1 build : "
        f"{sf1['median_build_ms']:.2f} ms"
    )

    print(
        f"SF10 build: "
        f"{sf10['median_build_ms']:.2f} ms"
    )

    print(
        f"Build growth: "
        f"{results['comparison']['build_time_growth']:.2f}x"
    )

    print(
        f"SF1 storage : "
        f"{sf1['storage_mb']:.4f} MB"
    )

    print(
        f"SF10 storage: "
        f"{sf10['storage_mb']:.4f} MB"
    )

    print(
        f"Storage growth: "
        f"{results['comparison']['storage_growth']:.2f}x"
    )

    print()
    print(
        f"Saved: {output_path}"
    )

    print("=" * 80)


if __name__ == "__main__":
    main()

from pathlib import Path
import shutil
import time

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

DELTA_DIR = Path("data/tpcds/delta")
TEST_DIR = DELTA_DIR / "_deepsea_fragment_test"

def create_spark():
    builder = (
        SparkSession.builder
        .appName("WAMVS-DeepSea-Fragment-Measurement")
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


def directory_size(path):
    if not path.exists():
        return 0

    return sum(
        f.stat().st_size
        for f in path.rglob("*")
        if f.is_file()
    )


def main():
    spark = create_spark()

    try:
        # Register only base tables.
        for path in sorted(DELTA_DIR.iterdir()):
            if not path.is_dir():
                continue

            if path.name.startswith("MV_"):
                continue

            if path.name.startswith("_"):
                continue

            (
                spark.read
                .format("delta")
                .load(str(path))
                .createOrReplaceTempView(path.name)
            )

        fragments = {
            "HF_001": """
                SELECT ss.*
                FROM store_sales ss
                JOIN item i
                  ON ss.ss_item_sk = i.i_item_sk
                WHERE i.i_category = 'Music'
            """,

            "HF_002": """
                SELECT ss.*
                FROM store_sales ss
                JOIN date_dim d
                  ON ss.ss_sold_date_sk = d.d_date_sk
                WHERE d.d_year BETWEEN 1999 AND 2001
            """,

            "HF_003": """
                SELECT ss.*
                FROM store_sales ss
                JOIN date_dim d
                  ON ss.ss_sold_date_sk = d.d_date_sk
                WHERE d.d_year = 2000
            """,

            "HF_004": """
                SELECT ss.*
                FROM store_sales ss
                JOIN item i
                  ON ss.ss_item_sk = i.i_item_sk
                WHERE i.i_category IN ('Music', 'Books', 'Sports')
            """,

            "HF_005": """
                SELECT ss.*
                FROM store_sales ss
                JOIN date_dim d
                  ON ss.ss_sold_date_sk = d.d_date_sk
                WHERE d.d_year >= 2000
            """,
        }

        print("=" * 75)
        print("HORIZONTAL FRAGMENT SIZE MEASUREMENT")
        print("=" * 75)

        results = []

        for fragment_id, sql in fragments.items():

            if TEST_DIR.exists():
                shutil.rmtree(TEST_DIR)

            output_path = TEST_DIR / fragment_id

            print()
            print("-" * 75)
            print(fragment_id)
            print("-" * 75)

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

            print(f"Rows       : {row_count:,}")
            print(f"Build time : {elapsed_ms:,.2f} ms")
            print(f"Storage    : {size_mb:.4f} MB")
            print(f"Storage    : {size_bytes:,} bytes")

            results.append({
                "fragment_id": fragment_id,
                "row_count": row_count,
                "build_time_ms": round(elapsed_ms, 2),
                "storage_bytes": size_bytes,
                "storage_mb": round(size_mb, 4),
            })

        print()
        print("=" * 75)
        print("SUMMARY")
        print("=" * 75)

        for r in results:
            print(
                f'{r["fragment_id"]}: '
                f'{r["row_count"]:,} rows | '
                f'{r["storage_mb"]:.4f} MB'
            )

        print()
        print("Storage budget: 0.0200 MB")

    finally:
        if TEST_DIR.exists():
            shutil.rmtree(TEST_DIR)

        spark.stop()


if __name__ == "__main__":
    main()

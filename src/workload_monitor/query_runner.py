from pathlib import Path
from datetime import datetime
import json
import time

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


QUERY_DIR = Path("workload/queries")
DELTA_DIR = Path("data/tpcds/delta")
LOG_FILE = Path("workload/workload_log.json")


def create_spark():
    builder = (
        SparkSession.builder
        .appName("WAMVS-Workload-Runner")
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
    print("=" * 60)
    print("REGISTERING TPC-DS DELTA TABLES")
    print("=" * 60)

    for delta_path in sorted(DELTA_DIR.iterdir()):
        if delta_path.is_dir():
            table_name = delta_path.name

            spark.read.format("delta") \
                .load(str(delta_path)) \
                .createOrReplaceTempView(table_name)

            print(f"  registered: {table_name}")


def run_query(spark, query_file):
    sql = query_file.read_text().strip()

    print("\n" + "=" * 60)
    print(f"RUNNING: {query_file.stem}")
    print("=" * 60)

    start = time.perf_counter()

    try:
        result = spark.sql(sql)

        # Force execution so runtime represents actual query work.
        rows = result.collect()

        elapsed_ms = (time.perf_counter() - start) * 1000

        print(f"Result rows : {len(rows)}")
        print(f"Runtime     : {elapsed_ms:,.2f} ms")

        return {
            "query_id": query_file.stem,
            "timestamp": datetime.now().isoformat(),
            "execution_time_ms": round(elapsed_ms, 2),
            "result_rows": len(rows),
            "status": "success",
        }

    except Exception as e:
        elapsed_ms = (time.perf_counter() - start) * 1000

        print(f"ERROR: {e}")

        return {
            "query_id": query_file.stem,
            "timestamp": datetime.now().isoformat(),
            "execution_time_ms": round(elapsed_ms, 2),
            "result_rows": 0,
            "status": "failed",
            "error": str(e),
        }


def main():
    spark = create_spark()

    try:
        register_tables(spark)

        results = []

        query_files = sorted(QUERY_DIR.glob("q*.sql"))

        print("\n" + "=" * 60)
        print(f"QUERIES FOUND: {len(query_files)}")
        print("=" * 60)

        for query_file in query_files:
            result = run_query(spark, query_file)
            results.append(result)

        LOG_FILE.parent.mkdir(parents=True, exist_ok=True)

        LOG_FILE.write_text(
            json.dumps(results, indent=2)
        )

        successful = [
            r for r in results
            if r["status"] == "success"
        ]

        failed = [
            r for r in results
            if r["status"] == "failed"
        ]

        print("\n" + "=" * 60)
        print("WORKLOAD EXECUTION SUMMARY")
        print("=" * 60)

        for r in results:
            print(
                f"{r['query_id']:5s} | "
                f"{r['execution_time_ms']:>12,.2f} ms | "
                f"rows={r['result_rows']:>6} | "
                f"{r['status']}"
            )

        print("-" * 60)
        print(f"Successful : {len(successful)}")
        print(f"Failed     : {len(failed)}")
        print(f"Log file   : {LOG_FILE}")
        print("=" * 60)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()

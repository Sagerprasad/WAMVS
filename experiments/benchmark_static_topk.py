from pathlib import Path
import json
import statistics
import time
import subprocess

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

ROOT = Path(__file__).resolve().parents[1]
DELTA_DIR = ROOT / "data/tpcds/delta"
CANDIDATE_FILE = ROOT / "workload/candidate_views.json"
NO_MV_FILE = ROOT / "results/no_mv_baseline.json"
OUTPUT_FILE = ROOT / "results/static_topk_baseline.json"

QUERIES = [f"q{i:02d}" for i in range(1, 11)]

def create_spark():
    builder = (
        SparkSession.builder
        .appName("WAMVS-Static-TopK")
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


def load_queries():
    queries = {}
    query_dir = ROOT / "workload/queries"

    for qid in QUERIES:
        queries[qid] = (query_dir / f"{qid}.sql").read_text().strip()

    return queries


def register_base_tables(spark):
    for path in sorted(DELTA_DIR.iterdir()):
        if not path.is_dir() or path.name.startswith("MV_"):
            continue

        (
            spark.read
            .format("delta")
            .load(str(path))
            .createOrReplaceTempView(path.name)
        )


def register_materialized_views(spark, selected):
    for view_id in selected:
        path = DELTA_DIR / view_id

        if not path.exists():
            print(f"Missing {view_id}; materializing static Top-K views...")
            subprocess.run(
                ["python", "src/materialization/generic_materializer.py"],
                cwd=str(ROOT),
                check=True,
            )

        if not path.exists():
            raise FileNotFoundError(
                f"Materialization completed but {view_id} is still missing: {path}"
            )

        (
            spark.read
            .format("delta")
            .load(str(path))
            .createOrReplaceTempView(view_id)
        )


def benchmark_query(spark, sql, warmup=1, runs=5):
    for _ in range(warmup):
        spark.sql(sql).collect()

    times = []
    rows = None

    for _ in range(runs):
        start = time.perf_counter()
        result = spark.sql(sql).collect()
        elapsed = (time.perf_counter() - start) * 1000

        times.append(elapsed)
        rows = len(result)

    return {
        "runs_ms": times,
        "median_ms": statistics.median(times),
        "mean_ms": statistics.mean(times),
        "rows": rows,
    }


def main():
    print("=" * 70)
    print("WAMVS STATIC TOP-K BASELINE")
    print("=" * 70)

    candidates = json.loads(CANDIDATE_FILE.read_text())

    # Static Top-K = highest workload frequency.
    # This represents a workload-oblivious-to-performance
    # but frequency-driven static materialization policy.
    candidates = sorted(
        candidates,
        key=lambda x: x.get("frequency", 0),
        reverse=True,
    )

    TOP_K = 4
    selected = [c["candidate_id"] for c in candidates[:TOP_K]]

    print(f"\nStatic Top-K selected views: {selected}")

    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    try:
        register_base_tables(spark)
        register_materialized_views(spark, selected)

        queries = load_queries()

        results = {}

        for qid in QUERIES:
            benchmark = benchmark_query(spark, queries[qid])

            results[qid] = {
                "query_id": qid,
                "selected_views": selected,
                **benchmark,
            }

            print(
                f"{qid}: median={benchmark['median_ms']:.2f} ms "
                f"| rows={benchmark['rows']}"
            )

        no_mv_raw = json.loads(NO_MV_FILE.read_text())

        # no_mv_baseline.json stores query results as a list.
        if isinstance(no_mv_raw, list):
            no_mv = {
                item["query_id"]: item
                for item in no_mv_raw
            }
        else:
            no_mv = no_mv_raw

        comparison = {}

        for qid in QUERIES:
            baseline = no_mv[qid]["median_ms"]
            static = results[qid]["median_ms"]

            comparison[qid] = {
                "no_mv_ms": baseline,
                "static_topk_ms": static,
                "speedup": baseline / static if static else None,
                "improvement_pct": (
                    (baseline - static) / baseline * 100
                    if baseline
                    else None
                ),
            }

        output = {
            "method": "static_top_k",
            "top_k": TOP_K,
            "selected_views": selected,
            "results": results,
            "comparison_with_no_mv": comparison,
        }

        OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
        OUTPUT_FILE.write_text(json.dumps(output, indent=2))

        print("\n" + "=" * 70)
        print("STATIC TOP-K BASELINE COMPLETE")
        print("=" * 70)
        print(f"Selected views: {selected}")
        print(f"Output: {OUTPUT_FILE}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()

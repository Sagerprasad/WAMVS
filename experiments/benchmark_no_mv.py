import json
import time
from pathlib import Path

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


ROOT = Path(__file__).resolve().parent.parent
QUERY_DIR = ROOT / "workload" / "queries"
OUTPUT = ROOT / "results" / "no_mv_baseline.json"

spark = (
    SparkSession.builder
    .appName("WAMVS-NoMV-Baseline")
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

spark = configure_spark_with_delta_pip(
    spark
).getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

delta_root = ROOT / "data" / "tpcds" / "delta"

# Register all base TPC-DS tables.
for path in sorted(delta_root.iterdir()):

    if not path.is_dir():
        continue

    name = path.name

    if name.startswith("MV_"):
        continue

    try:
        df = spark.read.format("delta").load(str(path))
        df.createOrReplaceTempView(name)
    except Exception:
        pass


results = []

print("=" * 70)
print("WAMVS NO-MV BASELINE")
print("=" * 70)

# Warm-up run.
for q in sorted(QUERY_DIR.glob("q*.sql")):
    sql = q.read_text()
    try:
        spark.sql(sql).collect()
    except Exception:
        pass

# Five measured runs per query.
for q in sorted(QUERY_DIR.glob("q*.sql")):

    query_id = q.stem
    sql = q.read_text()

    runs = []

    for _ in range(5):

        start = time.perf_counter()

        rows = spark.sql(sql).collect()

        elapsed = (
            time.perf_counter() - start
        ) * 1000

        runs.append(elapsed)

    runs_sorted = sorted(runs)

    median = runs_sorted[len(runs_sorted) // 2]

    result = {
        "query_id": query_id,
        "runs_ms": [round(x, 2) for x in runs],
        "median_ms": round(median, 2),
        "rows": len(rows)
    }

    results.append(result)

    print(
        f"{query_id}: "
        f"median={median:.2f} ms | "
        f"rows={len(rows)}"
    )


OUTPUT.parent.mkdir(parents=True, exist_ok=True)

with open(OUTPUT, "w") as f:
    json.dump(results, f, indent=2)

spark.stop()

print()
print("=" * 70)
print("NO-MV BASELINE COMPLETE")
print("=" * 70)
print("Output:", OUTPUT.relative_to(ROOT))

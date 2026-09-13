import json
import shutil
import time
from pathlib import Path

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


BASE_DIR = Path("data/tpcds/delta")
FRAGMENT_DIR = BASE_DIR / "_deepsea_fragments"
WORKLOAD_FILE = Path("workload/drift_workload.json")
NO_MV_FILE = Path("results/no_mv_baseline.json")

BUDGET_MB = 100.0

FRAGMENTS = {
    "HF_001": {
        "query": "q04",
        "description": "i_category = Music",
        "sql": """
            SELECT ss.*
            FROM store_sales ss
            JOIN item i
              ON ss.ss_item_sk = i.i_item_sk
            WHERE i.i_category = 'Music'
        """,
        "queries": ["q04"],
    },
    "HF_002": {
        "query": "q05",
        "description": "d_year BETWEEN 1999 AND 2001",
        "sql": """
            SELECT ss.*
            FROM store_sales ss
            JOIN date_dim d
              ON ss.ss_sold_date_sk = d.d_date_sk
            WHERE d.d_year BETWEEN 1999 AND 2001
        """,
        "queries": ["q05"],
    },
    "HF_003": {
        "query": "q07",
        "description": "d_year = 2000",
        "sql": """
            SELECT ss.*
            FROM store_sales ss
            JOIN date_dim d
              ON ss.ss_sold_date_sk = d.d_date_sk
            WHERE d.d_year = 2000
        """,
        "queries": ["q07"],
    },
    "HF_004": {
        "query": "q09",
        "description": "i_category IN Music, Books, Sports",
        "sql": """
            SELECT ss.*
            FROM store_sales ss
            JOIN item i
              ON ss.ss_item_sk = i.i_item_sk
            WHERE i.i_category IN ('Music', 'Books', 'Sports')
        """,
        "queries": ["q09"],
    },
    "HF_005": {
        "query": "q10",
        "description": "d_year >= 2000",
        "sql": """
            SELECT ss.*
            FROM store_sales ss
            JOIN date_dim d
              ON ss.ss_sold_date_sk = d.d_date_sk
            WHERE d.d_year >= 2000
        """,
        "queries": ["q10"],
    },
}


def create_spark():
    builder = (
        SparkSession.builder
        .appName("WAMVS-Physical-DeepSea-Baseline")
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


def load_base_tables(spark):
    for path in sorted(BASE_DIR.iterdir()):
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


def get_total_frequency():
    workload = json.loads(WORKLOAD_FILE.read_text())

    frequencies = {}

    for interval in workload:
        for query, count in interval["query_counts"].items():
            frequencies[query] = frequencies.get(query, 0) + count

    return frequencies


def build_fragments(spark):
    if FRAGMENT_DIR.exists():
        shutil.rmtree(FRAGMENT_DIR)

    FRAGMENT_DIR.mkdir(parents=True)

    frequencies = get_total_frequency()

    candidates = []

    print()
    print("=" * 80)
    print("DEEPSEA-STYLE HORIZONTAL FRAGMENT SELECTION")
    print("=" * 80)

    for fragment_id, info in FRAGMENTS.items():
        frequency = frequencies.get(info["query"], 0)

        start = time.perf_counter()

        df = spark.sql(info["sql"])
        row_count = df.count()

        path = FRAGMENT_DIR / fragment_id

        (
            df.write
            .format("delta")
            .mode("overwrite")
            .save(str(path))
        )

        build_ms = (time.perf_counter() - start) * 1000
        size_bytes = directory_size(path)
        size_mb = size_bytes / (1024 * 1024)

        candidates.append({
            "fragment_id": fragment_id,
            "description": info["description"],
            "source_query": info["query"],
            "frequency": frequency,
            "row_count": row_count,
            "build_time_ms": round(build_ms, 2),
            "storage_bytes": size_bytes,
            "storage_mb": round(size_mb, 4),
            "queries": info["queries"],
        })

        print(
            f"{fragment_id}: "
            f"freq={frequency}, "
            f"rows={row_count:,}, "
            f"storage={size_mb:.4f} MB"
        )

    # Fixed frequency-greedy selection.
    candidates.sort(
        key=lambda x: (-x["frequency"], x["storage_mb"])
    )

    selected = []
    used_mb = 0.0

    for candidate in candidates:
        if used_mb + candidate["storage_mb"] <= BUDGET_MB:
            selected.append(candidate)
            used_mb += candidate["storage_mb"]

    print()
    print("-" * 80)
    print("SELECTED HORIZONTAL FRAGMENTS")
    print("-" * 80)

    for x in selected:
        print(
            f'{x["fragment_id"]}: '
            f'frequency={x["frequency"]}, '
            f'storage={x["storage_mb"]:.4f} MB'
        )

    print(f"Used storage: {used_mb:.4f} / {BUDGET_MB:.4f} MB")

    selection = {
        "baseline": "simplified_deepsea_style_physical",
        "selection_policy": "fixed_frequency_greedy",
        "adaptive": False,
        "budget_mb": BUDGET_MB,
        "used_storage_mb": round(used_mb, 4),
        "candidates": candidates,
        "selected_fragments": selected,
    }

    Path("workload/deepsea_physical_selection.json").write_text(
        json.dumps(selection, indent=2)
    )

    return selected


def query_sql(query_id):
    queries = {
        "q01": """
            SELECT i.i_category,
                   SUM(ss.ss_ext_sales_price) AS revenue,
                   SUM(ss.ss_net_profit) AS profit
            FROM store_sales ss
            JOIN item i ON ss.ss_item_sk = i.i_item_sk
            GROUP BY i.i_category
        """,

        "q02": """
            SELECT d.d_year,
                   d.d_moy,
                   SUM(ss.ss_ext_sales_price) AS revenue,
                   SUM(ss.ss_net_profit) AS profit
            FROM store_sales ss
            JOIN date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
            GROUP BY d.d_year, d.d_moy
        """,

        "q03": """
            SELECT d.d_year,
                   i.i_category,
                   SUM(ss.ss_ext_sales_price) AS revenue
            FROM store_sales ss
            JOIN item i ON ss.ss_item_sk = i.i_item_sk
            JOIN date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
            GROUP BY d.d_year, i.i_category
        """,

        "q04": """
            SELECT i.i_category,
                   SUM(ss.ss_quantity) AS total_quantity,
                   AVG(ss.ss_sales_price) AS avg_price
            FROM store_sales ss
            JOIN item i ON ss.ss_item_sk = i.i_item_sk
            WHERE i.i_category = 'Music'
            GROUP BY i.i_category
        """,

        "q05": """
            SELECT d.d_year,
                   SUM(ss.ss_ext_sales_price) AS revenue
            FROM store_sales ss
            JOIN date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
            WHERE d.d_year BETWEEN 1999 AND 2001
            GROUP BY d.d_year
        """,

        "q06": """
            SELECT i.i_category,
                   i.i_class,
                   SUM(ss.ss_ext_sales_price) AS revenue
            FROM store_sales ss
            JOIN item i ON ss.ss_item_sk = i.i_item_sk
            GROUP BY i.i_category, i.i_class
        """,

        "q07": """
            SELECT d.d_year,
                   i.i_category,
                   SUM(ss.ss_net_profit) AS profit
            FROM store_sales ss
            JOIN item i ON ss.ss_item_sk = i.i_item_sk
            JOIN date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
            WHERE d.d_year = 2000
            GROUP BY d.d_year, i.i_category
        """,

        "q08": """
            SELECT ss.ss_store_sk,
                   SUM(ss.ss_ext_sales_price) AS revenue,
                   SUM(ss.ss_net_profit) AS profit
            FROM store_sales ss
            GROUP BY ss.ss_store_sk
        """,

        "q09": """
            SELECT i.i_category,
                   d.d_year,
                   COUNT(*) AS row_count,
                   SUM(ss.ss_ext_sales_price) AS revenue
            FROM store_sales ss
            JOIN item i ON ss.ss_item_sk = i.i_item_sk
            JOIN date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
            WHERE i.i_category IN ('Music', 'Books', 'Sports')
            GROUP BY i.i_category, d.d_year
        """,

        "q10": """
            SELECT d.d_year,
                   i.i_category,
                   i.i_brand,
                   SUM(ss.ss_ext_sales_price) AS revenue
            FROM store_sales ss
            JOIN item i ON ss.ss_item_sk = i.i_item_sk
            JOIN date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
            WHERE d.d_year >= 2000
            GROUP BY d.d_year, i.i_category, i.i_brand
        """,
    }

    return queries[query_id]


def benchmark_query(spark, query_id, selected_ids):
    original_sql = query_sql(query_id)

    # Only use a horizontal fragment when it is a direct predicate match.
    fragment_for_query = {
        "q04": "HF_001",
        "q05": "HF_002",
        "q07": "HF_003",
        "q09": "HF_004",
        "q10": "HF_005",
    }

    fragment_id = fragment_for_query.get(query_id)

    if fragment_id not in selected_ids:
        chosen_sql = original_sql
        used_fragment = None
    else:
        # Register selected raw fragment as a temporary replacement
        # for store_sales.
        fragment_path = FRAGMENT_DIR / fragment_id

        (
            spark.read
            .format("delta")
            .load(str(fragment_path))
            .createOrReplaceTempView("deepsea_store_sales")
        )

        chosen_sql = original_sql.replace(
            "FROM store_sales ss",
            "FROM deepsea_store_sales ss"
        )

        used_fragment = fragment_id

    # Warmup.
    spark.sql(chosen_sql).collect()

    timings = []

    for _ in range(5):
        start = time.perf_counter()
        result = spark.sql(chosen_sql).collect()
        elapsed = (time.perf_counter() - start) * 1000
        timings.append(elapsed)

    timings.sort()
    median_ms = timings[len(timings) // 2]

    return {
        "query_id": query_id,
        "fragment_used": used_fragment,
        "median_ms": round(median_ms, 2),
        "rows": len(result),
    }


def main():
    spark = create_spark()

    try:
        load_base_tables(spark)

        selected = build_fragments(spark)
        selected_ids = {x["fragment_id"] for x in selected}

        print()
        print("=" * 80)
        print("PHYSICAL DEEPSEA-STYLE QUERY BENCHMARK")
        print("=" * 80)

        results = []

        for query_id in [f"q{i:02d}" for i in range(1, 11)]:
            result = benchmark_query(
                spark,
                query_id,
                selected_ids
            )

            results.append(result)

            print(
                f'{query_id}: '
                f'{result["median_ms"]:.2f} ms | '
                f'fragment={result["fragment_used"] or "NONE"} | '
                f'rows={result["rows"]}'
            )

        # Load canonical No-MV measurements.
        no_mv = json.loads(NO_MV_FILE.read_text())

        if isinstance(no_mv, list):
            no_mv = {
                x["query_id"]: x
                for x in no_mv
            }

        for result in results:
            q = result["query_id"]

            baseline = no_mv[q]["median_ms"]

            result["no_mv_ms"] = baseline
            result["speedup_vs_no_mv"] = round(
                baseline / result["median_ms"],
                3
            )
            result["improvement_pct"] = round(
                (baseline - result["median_ms"])
                / baseline * 100,
                2
            )

        output = {
            "methodology": {
                "description": (
                    "Physical simplified DeepSea-style baseline using "
                    "fixed frequency-selected horizontal fragments."
                ),
                "budget_mb": BUDGET_MB,
                "adaptive": False,
                "selection_policy": "fixed_frequency_greedy",
                "warmup_runs": 1,
                "measured_runs": 5,
                "runtime_statistic": "median",
            },
            "selected_fragments": selected,
            "results": results,
        }

        Path("results").mkdir(exist_ok=True)

        Path(
            "results/deepsea_physical_baseline.json"
        ).write_text(
            json.dumps(output, indent=2)
        )

        print()
        print("=" * 80)
        print("FINAL RESULTS")
        print("=" * 80)

        accelerated = [
            x for x in results
            if x["fragment_used"] is not None
        ]

        if accelerated:
            avg_speedup = sum(
                x["speedup_vs_no_mv"]
                for x in accelerated
            ) / len(accelerated)

            avg_improvement = sum(
                x["improvement_pct"]
                for x in accelerated
            ) / len(accelerated)

            print(
                f"Accelerated queries : {len(accelerated)}"
            )
            print(
                f"Mean speedup        : {avg_speedup:.2f}x"
            )
            print(
                f"Mean improvement    : {avg_improvement:.2f}%"
            )

        print()
        print(
            "Saved: results/deepsea_physical_baseline.json"
        )
        print(
            "Saved: workload/deepsea_physical_selection.json"
        )

    finally:
        if FRAGMENT_DIR.exists():
            shutil.rmtree(FRAGMENT_DIR)

        spark.stop()


if __name__ == "__main__":
    main()

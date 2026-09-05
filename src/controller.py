from pathlib import Path
import json
import shutil
import time

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


SELECTION_FILE = Path(
    "workload/adaptive_selection.json"
)

CANDIDATE_FILE = Path(
    "workload/candidate_views.json"
)

DELTA_DIR = Path(
    "data/tpcds/delta"
)

OUTPUT_FILE = Path(
    "workload/adaptation_metrics.json"
)


def create_spark():

    builder = (
        SparkSession.builder
        .appName("WAMVS-Adaptive-Controller")
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


def load_json(path):
    return json.loads(
        path.read_text()
    )


def register_base_tables(spark):

    for path in sorted(
        DELTA_DIR.iterdir()
    ):

        if not path.is_dir():
            continue

        # Never register an MV as a base table here.
        if path.name.startswith("MV_"):
            continue

        (
            spark.read
            .format("delta")
            .load(str(path))
            .createOrReplaceTempView(
                path.name
            )
        )


def generate_sql(candidate):

    group_by = candidate["group_by"]
    expressions = candidate[
        "measure_expressions"
    ]

    tables = candidate["tables"]
    joins = candidate["joins"]

    select_columns = []

    for column in group_by:
        select_columns.append(column)

    for measure, expression in expressions.items():

        select_columns.append(
            f"{expression} AS {measure}"
        )

    if "store_sales" in tables:
        base_table = "store_sales"
    else:
        base_table = tables[0]

    sql = (
        "SELECT\n    "
        + ",\n    ".join(
            select_columns
        )
        + f"\nFROM {base_table}"
    )

    for join in joins:

        sql += (
            f"\nJOIN {join['table']}"
            f"\n    ON {join['condition']}"
        )

    if group_by:

        sql += (
            "\nGROUP BY "
            + ", ".join(group_by)
        )

    return sql


def directory_size(path):

    if not path.exists():
        return 0

    return sum(
        f.stat().st_size
        for f in path.rglob("*")
        if f.is_file()
    )


def materialize_view(
    spark,
    candidate
):

    candidate_id = candidate[
        "candidate_id"
    ]

    output_path = (
        DELTA_DIR / candidate_id
    )

    if output_path.exists():

        shutil.rmtree(
            output_path
        )

    sql = generate_sql(
        candidate
    )

    start = time.perf_counter()

    df = spark.sql(sql)

    row_count = df.count()

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .save(
            str(output_path)
        )
    )

    elapsed_ms = (
        time.perf_counter()
        - start
    ) * 1000

    storage_mb = (
        directory_size(
            output_path
        )
        / (1024 * 1024)
    )

    return {
        "candidate_id":
            candidate_id,

        "row_count":
            row_count,

        "storage_mb":
            round(
                storage_mb,
                4
            ),

        "materialization_time_ms":
            round(
                elapsed_ms,
                2
            ),
    }


def drop_view(candidate_id):

    path = (
        DELTA_DIR / candidate_id
    )

    if path.exists():

        start = time.perf_counter()

        shutil.rmtree(
            path
        )

        elapsed_ms = (
            time.perf_counter()
            - start
        ) * 1000

        return round(
            elapsed_ms,
            2
        )

    return 0.0


def main():

    selections = load_json(
        SELECTION_FILE
    )

    candidates = load_json(
        CANDIDATE_FILE
    )

    candidates_by_id = {
        candidate["candidate_id"]:
            candidate
        for candidate in candidates
    }

    spark = create_spark()

    try:

        register_base_tables(
            spark
        )

        print("=" * 80)
        print("WAMVS ADAPTIVE CONTROLLER")
        print("=" * 80)

        print(
            "\nThe controller will replay "
            "the adaptive workload decisions."
        )

        previous_selected = set()

        history = []

        total_create_time = 0.0
        total_drop_time = 0.0

        total_created = 0
        total_dropped = 0

        for interval in selections:

            interval_id = interval[
                "interval"
            ]

            phase = interval[
                "phase"
            ]

            desired = set(
                interval[
                    "selected_views"
                ]
            )

            to_create = sorted(
                desired
                - previous_selected
            )

            to_drop = sorted(
                previous_selected
                - desired
            )

            print(
                "\n" + "-" * 80
            )

            print(
                f"Interval {interval_id:2d} | "
                f"{phase:9s}"
            )

            print(
                "Desired: "
                + ", ".join(
                    sorted(desired)
                )
            )

            create_results = []
            drop_results = []

            # -------------------------------------------------
            # DROP first
            # -------------------------------------------------

            for candidate_id in to_drop:

                elapsed = drop_view(
                    candidate_id
                )

                total_drop_time += elapsed
                total_dropped += 1

                drop_results.append({
                    "candidate_id":
                        candidate_id,

                    "latency_ms":
                        elapsed,
                })

                print(
                    f"  DROP   {candidate_id} "
                    f"({elapsed:.2f} ms)"
                )

            # -------------------------------------------------
            # CREATE
            # -------------------------------------------------

            for candidate_id in to_create:

                candidate = candidates_by_id.get(
                    candidate_id
                )

                if candidate is None:

                    print(
                        f"  ERROR: candidate "
                        f"{candidate_id} not found"
                    )

                    continue

                try:

                    result = materialize_view(
                        spark,
                        candidate
                    )

                    elapsed = result[
                        "materialization_time_ms"
                    ]

                    total_create_time += elapsed
                    total_created += 1

                    create_results.append(
                        result
                    )

                    print(
                        f"  CREATE {candidate_id} "
                        f"({elapsed:.2f} ms, "
                        f"{result['storage_mb']:.4f} MB)"
                    )

                except Exception as e:

                    print(
                        f"  CREATE {candidate_id} "
                        f"FAILED: {e}"
                    )

            # -------------------------------------------------
            # Measure current physical storage
            # -------------------------------------------------

            current_storage = 0.0
            physical_views = []

            for candidate_id in sorted(
                desired
            ):

                path = (
                    DELTA_DIR
                    / candidate_id
                )

                if path.exists():

                    size = (
                        directory_size(
                            path
                        )
                        / (1024 * 1024)
                    )

                    current_storage += size

                    physical_views.append(
                        candidate_id
                    )

            # -------------------------------------------------
            # Adaptation latency
            # -------------------------------------------------

            create_latency = sum(
                item["materialization_time_ms"]
                for item in create_results
            )

            drop_latency = sum(
                item["latency_ms"]
                for item in drop_results
            )

            adaptation_latency = (
                create_latency
                + drop_latency
            )

            changed = (
                desired
                != previous_selected
            )

            history.append({

                "interval":
                    interval_id,

                "phase":
                    phase,

                "selected_views":
                    sorted(desired),

                "created_views":
                    to_create,

                "dropped_views":
                    to_drop,

                "create_results":
                    create_results,

                "drop_results":
                    drop_results,

                "adaptation_latency_ms":
                    round(
                        adaptation_latency,
                        2
                    ),

                "storage_used_mb":
                    round(
                        current_storage,
                        4
                    ),

                "physical_views":
                    physical_views,

                "configuration_changed":
                    changed,
            })

            print(
                f"  Adaptation latency: "
                f"{adaptation_latency:.2f} ms"
            )

            print(
                f"  Physical storage : "
                f"{current_storage:.4f} MB"
            )

            previous_selected = desired

        # -----------------------------------------------------
        # Final physical state
        # -----------------------------------------------------

        all_mv_dirs = [
            path
            for path in DELTA_DIR.iterdir()
            if path.is_dir()
            and path.name.startswith("MV_")
        ]

        desired_final = (
            previous_selected
        )

        for path in all_mv_dirs:

            if path.name not in desired_final:

                shutil.rmtree(
                    path
                )

        # -----------------------------------------------------
        # Summary
        # -----------------------------------------------------

        changed_intervals = sum(
            1
            for item in history
            if item[
                "configuration_changed"
            ]
        )

        adaptation_events = [
            item[
                "adaptation_latency_ms"
            ]
            for item in history
            if item[
                "configuration_changed"
            ]
        ]

        if adaptation_events:

            average_adaptation = (
                sum(adaptation_events)
                / len(adaptation_events)
            )

        else:

            average_adaptation = 0.0

        max_storage = max(
            (
                item[
                    "storage_used_mb"
                ]
                for item in history
            ),
            default=0.0
        )

        output = {

            "storage_budget_mb":
                0.0200,

            "intervals":
                history,

            "summary": {

                "intervals_evaluated":
                    len(history),

                "configuration_changes":
                    changed_intervals,

                "views_created":
                    total_created,

                "views_dropped":
                    total_dropped,

                "total_create_time_ms":
                    round(
                        total_create_time,
                        2
                    ),

                "total_drop_time_ms":
                    round(
                        total_drop_time,
                        2
                    ),

                "average_adaptation_latency_ms":
                    round(
                        average_adaptation,
                        2
                    ),

                "maximum_storage_used_mb":
                    round(
                        max_storage,
                        4
                    ),

                "final_views":
                    sorted(
                        desired_final
                    ),
            },
        }

        OUTPUT_FILE.write_text(
            json.dumps(
                output,
                indent=2
            )
        )

        print("\n" + "=" * 80)
        print("ADAPTIVE CONTROLLER SUMMARY")
        print("=" * 80)

        print(
            f"Intervals evaluated : "
            f"{len(history)}"
        )

        print(
            f"Configuration changes: "
            f"{changed_intervals}"
        )

        print(
            f"Views created       : "
            f"{total_created}"
        )

        print(
            f"Views dropped       : "
            f"{total_dropped}"
        )

        print(
            f"Total CREATE time   : "
            f"{total_create_time:.2f} ms"
        )

        print(
            f"Total DROP time     : "
            f"{total_drop_time:.2f} ms"
        )

        print(
            f"Avg adaptation time : "
            f"{average_adaptation:.2f} ms"
        )

        print(
            f"Maximum storage     : "
            f"{max_storage:.4f} MB"
        )

        print(
            f"Storage budget      : "
            f"0.0200 MB"
        )

        print(
            "\nFinal physical MVs:"
        )

        for view in sorted(
            desired_final
        ):

            print(
                f"  {view}"
            )

        print(
            f"\nMetrics: "
            f"{OUTPUT_FILE}"
        )

        print("=" * 80)

    finally:

        spark.stop()


if __name__ == "__main__":
    main()

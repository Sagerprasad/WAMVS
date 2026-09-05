from pathlib import Path

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

from src.data.tpcds_schemas import TPCDS_SCHEMAS


RAW_DIR = Path.home() / "WAMVS" / "data" / "tpcds" / "raw"
DELTA_DIR = Path.home() / "WAMVS" / "data" / "tpcds" / "delta"


def create_spark():
    builder = (
        SparkSession.builder
        .appName("WAMVS-TPCDS-Full-Loader")
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


def load_table(spark, table_name, schema):
    source = RAW_DIR / f"{table_name}.dat"
    destination = DELTA_DIR / table_name

    print(f"\n{'=' * 60}")
    print(f"LOADING: {table_name}")
    print(f"{'=' * 60}")

    if not source.exists():
        raise FileNotFoundError(f"Missing source: {source}")

    df = (
        spark.read
        .option("header", "false")
        .option("delimiter", "|")
        .option("mode", "PERMISSIVE")
        .schema(schema)
        .csv(str(source))
    )

    # The TPC-DS .dat files use a trailing pipe delimiter.
    # With an explicit schema, Spark reads the expected columns.
    actual_columns = len(df.columns)
    expected_columns = len(schema.fields)

    if actual_columns != expected_columns:
        raise RuntimeError(
            f"{table_name}: expected {expected_columns} columns, "
            f"got {actual_columns}"
        )

    row_count = df.count()

    print(f"Rows:    {row_count:,}")
    print(f"Columns: {actual_columns}")

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(str(destination))
    )

    print("Delta write: OK")

    return row_count


def register_tables(spark):
    print("\n" + "=" * 60)
    print("REGISTERING DELTA TABLES")
    print("=" * 60)

    for table_name in TPCDS_SCHEMAS:
        path = DELTA_DIR / table_name

        if not path.exists():
            continue

        df = spark.read.format("delta").load(str(path))
        df.createOrReplaceTempView(table_name)

        print(f"  registered: {table_name}")


def validate_cross_table_queries(spark):
    print("\n" + "=" * 60)
    print("CROSS-TABLE VALIDATION")
    print("=" * 60)

    # ---------------------------------------------------------
    # Q1: Fact + item dimension
    # ---------------------------------------------------------
    print("\n[Q1] Store sales + item")

    q1 = spark.sql("""
        SELECT
            i.i_category,
            SUM(ss.ss_ext_sales_price) AS revenue,
            SUM(ss.ss_net_profit) AS profit,
            COUNT(*) AS rows
        FROM store_sales ss
        JOIN item i
          ON ss.ss_item_sk = i.i_item_sk
        GROUP BY i.i_category
        ORDER BY revenue DESC
        LIMIT 10
    """)

    q1.show(truncate=False)

    # ---------------------------------------------------------
    # Q2: Fact + date dimension
    # ---------------------------------------------------------
    print("\n[Q2] Store sales + date")

    q2 = spark.sql("""
        SELECT
            d.d_year,
            d.d_moy,
            SUM(ss.ss_ext_sales_price) AS revenue,
            SUM(ss.ss_net_profit) AS profit
        FROM store_sales ss
        JOIN date_dim d
          ON ss.ss_sold_date_sk = d.d_date_sk
        GROUP BY d.d_year, d.d_moy
        ORDER BY d.d_year, d.d_moy
        LIMIT 20
    """)

    q2.show(truncate=False)

    # ---------------------------------------------------------
    # Q3: Fact + item + date
    # ---------------------------------------------------------
    print("\n[Q3] Store sales + item + date")

    q3 = spark.sql("""
        SELECT
            d.d_year,
            i.i_category,
            SUM(ss.ss_ext_sales_price) AS revenue
        FROM store_sales ss
        JOIN item i
          ON ss.ss_item_sk = i.i_item_sk
        JOIN date_dim d
          ON ss.ss_sold_date_sk = d.d_date_sk
        GROUP BY d.d_year, i.i_category
        ORDER BY revenue DESC
        LIMIT 20
    """)

    q3.show(truncate=False)

    print("\nCross-table validation: SUCCESS")


def main():
    spark = create_spark()

    try:
        print("=" * 60)
        print("WAMVS - FULL TPC-DS → DELTA INGESTION")
        print("=" * 60)

        print(f"Tables to load: {len(TPCDS_SCHEMAS)}")

        counts = {}

        for table_name, schema in TPCDS_SCHEMAS.items():
            counts[table_name] = load_table(
                spark,
                table_name,
                schema,
            )

        print("\n" + "=" * 60)
        print("LOAD SUMMARY")
        print("=" * 60)

        total_rows = 0

        for table_name, count in counts.items():
            print(f"{table_name:25s} {count:>12,}")
            total_rows += count

        print("-" * 40)
        print(f"{'TOTAL':25s} {total_rows:>12,}")

        register_tables(spark)

        validate_cross_table_queries(spark)

        print("\n" + "=" * 60)
        print("SUCCESS")
        print("All 25 TPC-DS tables are available through Spark SQL.")
        print("=" * 60)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()

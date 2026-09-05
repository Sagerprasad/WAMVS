from pathlib import Path

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

from src.data.tpcds_schemas import STORE_SALES_SCHEMA


RAW_DIR = Path.home() / "WAMVS" / "data" / "tpcds" / "raw"
DELTA_DIR = Path.home() / "WAMVS" / "data" / "tpcds" / "delta"


def create_spark():
    builder = (
        SparkSession.builder
        .appName("WAMVS-TPCDS-Loader")
        .master("local[8]")
        .config("spark.driver.memory", "6g")
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


def load_store_sales(spark):
    source = RAW_DIR / "store_sales.dat"
    destination = DELTA_DIR / "store_sales"

    print("=" * 70)
    print("Loading TPC-DS STORE_SALES")
    print("=" * 70)

    df = (
        spark.read
        .option("header", "false")
        .option("delimiter", "|")
        .option("mode", "PERMISSIVE")
        .schema(STORE_SALES_SCHEMA)
        .csv(str(source))
    )

    print("\nSchema:")
    df.printSchema()

    print("\nRow count:")
    print(df.count())

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(str(destination))
    )

    return spark.read.format("delta").load(str(destination))


def validate_workload_queries(df):
    print("\n" + "=" * 70)
    print("WAMVS WORKLOAD VALIDATION")
    print("=" * 70)

    df.createOrReplaceTempView("store_sales")

    # Query 1: filtering + aggregation
    print("\n[Q1] Sales by store")
    q1 = df.sparkSession.sql("""
        SELECT
            ss_sold_store_sk,
            SUM(ss_ext_sales_price) AS total_sales,
            AVG(ss_sales_price) AS avg_price
        FROM store_sales
        WHERE ss_quantity > 50
        GROUP BY ss_sold_store_sk
        ORDER BY total_sales DESC
        LIMIT 10
    """)
    q1.show(truncate=False)

    # Query 2: filtering on date key + aggregation
    print("\n[Q2] Sales by item")
    q2 = df.sparkSession.sql("""
        SELECT
            ss_sold_item_sk,
            SUM(ss_net_profit) AS total_profit,
            COUNT(*) AS transactions
        FROM store_sales
        WHERE ss_sold_date_sk BETWEEN 2451000 AND 2452000
        GROUP BY ss_sold_item_sk
        ORDER BY total_profit DESC
        LIMIT 10
    """)
    q2.show(truncate=False)

    # Query 3: common WAMVS candidate-view pattern
    print("\n[Q3] Store + date + aggregate")
    q3 = df.sparkSession.sql("""
        SELECT
            ss_sold_store_sk,
            ss_sold_date_sk,
            SUM(ss_ext_sales_price) AS revenue,
            SUM(ss_net_profit) AS profit
        FROM store_sales
        GROUP BY
            ss_sold_store_sk,
            ss_sold_date_sk
        LIMIT 10
    """)
    q3.show(truncate=False)


def main():
    spark = create_spark()

    try:
        df = load_store_sales(spark)

        print("\nFinal Delta schema:")
        df.printSchema()

        print("\nSample:")
        df.show(5, truncate=False)

        validate_workload_queries(df)

        print("\n" + "=" * 70)
        print("SUCCESS: Named TPC-DS schema + Delta + analytical SQL works.")
        print("=" * 70)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()

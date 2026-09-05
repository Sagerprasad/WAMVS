from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


def create_spark_session():
    builder = (
        SparkSession.builder
        .appName("WAMVS-Test")
        .master("local[8]")
        .config("spark.driver.memory", "6g")
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


def main():
    spark = create_spark_session()

    print("\n==============================")
    print("       WAMVS ENVIRONMENT")
    print("==============================")
    print("Spark version:", spark.version)
    print("Python version:", spark.sparkContext.pythonVer)

    data = [
        (1, "Laptop", 50000),
        (2, "Phone", 30000),
        (3, "Tablet", 20000),
        (4, "Monitor", 15000),
    ]

    df = spark.createDataFrame(
        data,
        ["id", "product", "price"]
    )

    print("\nOriginal Data:")
    df.show()

    delta_path = "data/test_products"

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .save(delta_path)
    )

    print("Delta table successfully written.")

    delta_df = (
        spark.read
        .format("delta")
        .load(delta_path)
    )

    print("\nData read from Delta:")
    delta_df.show()

    delta_df.createOrReplaceTempView("products")

    result = spark.sql("""
        SELECT
            COUNT(*) AS product_count,
            AVG(price) AS average_price,
            MAX(price) AS maximum_price
        FROM products
    """)

    print("\nSpark SQL Result:")
    result.show()

    print("==============================")
    print("       TEST SUCCESSFUL")
    print("==============================\n")

    spark.stop()


if __name__ == "__main__":
    main()

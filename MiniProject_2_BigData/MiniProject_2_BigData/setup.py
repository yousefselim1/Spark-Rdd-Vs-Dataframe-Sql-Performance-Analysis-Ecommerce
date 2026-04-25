from pyspark.sql import SparkSession

def get_spark():
    spark = SparkSession.builder \
        .appName("EcommerceAnalysis") \
        .master("local[*]") \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    return spark


def load_data(spark, path="data/ecommerce.csv"):
    df = spark.read.csv(
        path,
        header=True,
        inferSchema=True
    )

    print("\n=== SCHEMA ===")
    df.printSchema()

    print("\n=== TOTAL RECORDS ===")
    print(df.count())

    print("\n=== SAMPLE DATA ===")
    df.show(5)

    df.createOrReplaceTempView("ecommerce")

    return df


if __name__ == "__main__":
    spark = get_spark()
    df = load_data(spark)
    spark.stop()

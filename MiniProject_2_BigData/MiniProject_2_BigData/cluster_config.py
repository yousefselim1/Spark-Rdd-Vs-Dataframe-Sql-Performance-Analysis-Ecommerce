from pyspark.sql import SparkSession

def create_spark(app_name="ClusterConfigTest", partitions=8, memory="2g"):
    spark = SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", str(partitions)) \
        .config("spark.driver.memory", memory) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    print("\n=== CLUSTER CONFIGURATION ===")
    print("Shuffle Partitions:", spark.conf.get("spark.sql.shuffle.partitions"))
    print("Driver Memory:", spark.conf.get("spark.driver.memory"))

    return spark


if __name__ == "__main__":
    spark = create_spark(partitions=8, memory="2g")
    spark.stop()

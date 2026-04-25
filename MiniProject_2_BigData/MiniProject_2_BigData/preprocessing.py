from setup import get_spark, load_data
from pyspark.sql import functions as F

spark = get_spark()
df = load_data(spark)

# Remove null prices
df = df.filter(F.col("total_price_usd").isNotNull())

# Convert date column
df = df.withColumn("order_date", F.to_timestamp("order_date"))

# Remove duplicates
df = df.dropDuplicates()

# Save cleaned version
df.write.mode("overwrite").parquet("data/ecommerce_clean.parquet")

print("Preprocessing complete.")
spark.stop()

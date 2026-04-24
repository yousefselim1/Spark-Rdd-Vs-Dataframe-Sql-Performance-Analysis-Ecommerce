from setup import get_spark, load_data
from pyspark.sql import functions as F

spark = get_spark()
df = load_data(spark)

print("\n=== BASIC INFO ===")
print("Columns:", df.columns)
print("Total Rows:", df.count())

print("\n=== MISSING VALUES ===")
df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns]).show()

print("\n=== DATA TYPES ===")
df.printSchema()

print("\n=== SAMPLE DATA ===")
df.show(5)

print("\n=== NUMERICAL SUMMARY ===")
df.describe().show()

print("\n=== TOP CATEGORIES ===")
df.groupBy("product_category").count().orderBy(F.desc("count")).show(10)

print("\n=== TOP COUNTRIES ===")
df.groupBy("customer_country").count().orderBy(F.desc("count")).show(10)

print("\n=== ORDER STATUS DISTRIBUTION ===")
df.groupBy("order_status").count().show()

print("\n=== PRICE ANALYSIS ===")
df.select(
    F.min("total_price"),
    F.max("total_price"),
    F.avg("total_price")
).show()

spark.stop()

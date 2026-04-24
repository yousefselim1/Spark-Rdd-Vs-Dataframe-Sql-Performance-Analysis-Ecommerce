import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import sys, time, json
sys.path.insert(0, '/root/spark_project')
from setup import get_spark, load_data
from pyspark.sql import functions as F

spark = get_spark()
df = load_data(spark)
timings = {}

# Create a small lookup table (product categories with discount rates)
category_discounts = spark.createDataFrame([
    ("Electronics", 0.10), ("Clothing", 0.15),
    ("Books", 0.05), ("Home", 0.12), ("Sports", 0.08)
], ["category", "discount_rate"])

print("\n" + "="*60)
print("JOIN OPTIMIZATION: Broadcast Join vs Sort-Merge Join")
print("="*60)

# --- BROADCAST JOIN (small table broadcast) ---
t = time.time()
broadcast_join = df.join(
    F.broadcast(category_discounts),
    on="category", how="left"
).withColumn("discounted_price",
    F.col("total_price_usd") * (1 - F.col("discount_rate")))
print(f"\nBroadcast Join count: {broadcast_join.count()}")
timings['broadcast_join_time'] = time.time() - t
print("=== BROADCAST JOIN EXPLAIN ===")
broadcast_join.explain(True)

# --- SORT-MERGE JOIN (default for large tables) ---
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")  # Disable broadcast
t = time.time()
smj = df.join(category_discounts, on="category", how="left") \
        .withColumn("discounted_price",
            F.col("total_price_usd") * (1 - F.col("discount_rate")))
print(f"\nSort-Merge Join count: {smj.count()}")
timings['sort_merge_join_time'] = time.time() - t
print("=== SORT-MERGE JOIN EXPLAIN ===")
smj.explain(True)

# Reset threshold
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")

print("\n--- JOIN PERFORMANCE COMPARISON ---")
for k, v in timings.items():
    print(f"  {k}: {v:.3f}s")

# --- CACHING DEMONSTRATION ---
print("\n" + "="*60)
print("CACHING: With Cache vs Without Cache")
print("="*60)

# Without cache
t = time.time()
df.groupBy("category").agg(F.sum("total_price_usd")).show()
df.groupBy("category").agg(F.count("*")).show()
timings['without_cache_time'] = time.time() - t

# With cache
df.cache()
df.count()  # Trigger caching
t = time.time()
df.groupBy("category").agg(F.sum("total_price_usd")).show()
df.groupBy("category").agg(F.count("*")).show()
timings['with_cache_time'] = time.time() - t
df.unpersist()

# --- PARTITIONING DEMONSTRATION ---
print("\n" + "="*60)
print("PARTITIONING: Impact on Query Performance")
print("="*60)

# Default partitions
t = time.time()
df.groupBy("country").agg(F.sum("total_price_usd")).show()
timings['default_partitions_time'] = time.time() - t

# Repartitioned by country
df_repartitioned = df.repartition(20, "country")
t = time.time()
df_repartitioned.groupBy("country").agg(F.sum("total_price_usd")).show()
timings['repartitioned_time'] = time.time() - t

# --- CSV vs Parquet comparison ---
print("\n" + "="*60)
print("FILE FORMAT: CSV vs Parquet")
print("="*60)
try:
    t = time.time()
    df_parquet = spark.read.parquet("data/ecommerce.parquet")
    df_parquet.groupBy("category").agg(F.sum("total_price_usd")).show()
    timings['parquet_read_time'] = time.time() - t

    t = time.time()
    df_csv = spark.read.csv("data/ecommerce.csv", header=True, inferSchema=True)
    df_csv.groupBy("category").agg(F.sum("total_price_usd")).show()
    timings['csv_read_time'] = time.time() - t
except Exception as e:
    print(f"Parquet/CSV comparison skipped: {e}")

with open('results/optimization_timings.json', 'w') as f:
    json.dump(timings, f, indent=2)
print("\nOptimization timings saved.")
spark.stop()

import sys
import os
import time
import json

# Fix import path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from setup import get_spark, load_data
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = get_spark()
df = load_data(spark)

timings = {}

# Helper function (clean output)
def run_query(name, df_query, show_n=10):
    print("\n" + "="*60)
    print(f"{name} RESULT")
    print("="*60)
    df_query.show(show_n)

    print("\n" + "-"*60)
    print(f"{name} EXPLAIN PLAN")
    print("-"*60)
    df_query.explain(True)


# --- Q1: Filter ---
t = time.time()
q1 = df.filter((F.col("order_status") == "Completed") & (F.col("total_price_usd") > 500))
run_query("Q1 - Completed orders > 500", q1)
timings['Q1_time'] = time.time() - t


# --- Q2: Aggregation ---
t = time.time()
q2 = df.groupBy("category") \
       .agg(F.round(F.sum("total_price_usd"), 2).alias("total_revenue")) \
       .orderBy(F.desc("total_revenue"))
run_query("Q2 - Revenue by category", q2)
timings['Q2_time'] = time.time() - t


# --- Q3: Average by country ---
t = time.time()
q3 = df.groupBy("country") \
       .agg(F.round(F.avg("total_price_usd"), 2).alias("avg_order_value")) \
       .orderBy(F.desc("avg_order_value"))
run_query("Q3 - Avg order value by country", q3)
timings['Q3_time'] = time.time() - t


# --- Q4: Count by payment method ---
t = time.time()
q4 = df.groupBy("payment_method") \
       .agg(F.count("*").alias("order_count")) \
       .orderBy(F.desc("order_count"))
run_query("Q4 - Orders by payment method", q4)
timings['Q4_time'] = time.time() - t


# --- Q5: Max order per customer ---
t = time.time()
q5 = df.groupBy("customer_id") \
       .agg(F.max("total_price_usd").alias("max_order")) \
       .orderBy(F.desc("max_order"))
run_query("Q5 - Max order per customer", q5)
timings['Q5_time'] = time.time() - t


# --- Q6: Orders per month ---
t = time.time()
q6 = df.withColumn("month", F.date_format(F.col("order_date"), "yyyy-MM")) \
       .groupBy("month") \
       .agg(F.count("*").alias("order_count")) \
       .orderBy("month")
run_query("Q6 - Orders per month", q6, show_n=24)
timings['Q6_time'] = time.time() - t


# --- Q7: Multi-group aggregation ---
t = time.time()
q7 = df.groupBy("category", "country") \
       .agg(F.round(F.sum("total_price_usd"), 2).alias("revenue")) \
       .orderBy(F.desc("revenue"))
run_query("Q7 - Revenue by category & country", q7)
timings['Q7_time'] = time.time() - t


# --- Q8: Distinct count ---
t = time.time()
q8 = df.groupBy("category") \
       .agg(F.countDistinct("product_id").alias("distinct_products")) \
       .orderBy(F.desc("distinct_products"))
run_query("Q8 - Distinct products per category", q8)
timings['Q8_time'] = time.time() - t


# --- Q9: Window function ---
t = time.time()
w = Window.orderBy("order_date").rowsBetween(Window.unboundedPreceding, Window.currentRow)
q9 = df.withColumn("cumulative_revenue", F.sum("total_price_usd").over(w)) \
       .select("order_date", "total_price_usd", "cumulative_revenue")
run_query("Q9 - Cumulative revenue (window)", q9)
timings['Q9_time'] = time.time() - t


# --- Q10: Subquery ---
t = time.time()
customer_spend = df.groupBy("customer_id") \
                   .agg(F.sum("total_price_usd").alias("total_spend"))

avg_spend = customer_spend.agg(F.avg("total_spend")).collect()[0][0]

q10 = customer_spend.filter(F.col("total_spend") > avg_spend) \
                    .orderBy(F.desc("total_spend"))

print(f"\nQ10 Average Spend: {avg_spend:.2f}")
run_query("Q10 - High-value customers", q10)
timings['Q10_time'] = time.time() - t


# Save timings
with open('results/df_timings.json', 'w') as f:
    json.dump(timings, f, indent=2)

print("\nAll DataFrame timings saved.")

spark.stop()

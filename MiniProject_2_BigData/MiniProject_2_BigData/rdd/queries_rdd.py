import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import sys, time
sys.path.insert(0, '/root/spark_project')
from setup import get_spark, load_data

spark = get_spark()
df = load_data(spark)
rdd = df.rdd

results = {}

# --- Q1: Filter orders above $500 with status = Completed ---
t = time.time()
q1 = rdd.filter(lambda r: r['order_status'] == 'Completed' and r['total_price_usd'] is not None and float(r['total_price_usd']) > 500)
results['Q1_count'] = q1.count()
results['Q1_time'] = time.time() - t
print("\n=== RDD LINEAGE (Execution Flow) ===")
print(q1.toDebugString())
print(f"Q1 RDD | Count={results['Q1_count']} | Time={results['Q1_time']:.3f}s")

# --- Q2: Total revenue by category ---
t = time.time()
q2 = rdd.map(lambda r: (r['category'], float(r['total_price_usd']) if r['total_price_usd'] else 0)) \
         .reduceByKey(lambda a, b: a + b) \
         .sortBy(lambda x: -x[1])
results['Q2'] = q2.take(10)
results['Q2_time'] = time.time() - t
print("\n=== RDD LINEAGE (Execution Flow) ===")
print(q2.toDebugString())
print(f"Q2 RDD | Top category: {results['Q2'][0]} | Time={results['Q2_time']:.3f}s")

# --- Q3: Average order value by country ---
t = time.time()
q3 = rdd.map(lambda r: (r['country'], (float(r['total_price_usd']) if r['total_price_usd'] else 0, 1))) \
         .reduceByKey(lambda a, b: (a[0]+b[0], a[1]+b[1])) \
         .mapValues(lambda x: round(x[0]/x[1], 2)) \
         .sortBy(lambda x: -x[1])
results['Q3'] = q3.take(10)
results['Q3_time'] = time.time() - t
print("\n=== RDD LINEAGE (Execution Flow) ===")
print(q3.toDebugString())
print(f"Q3 RDD | Top country avg: {results['Q3'][0]} | Time={results['Q3_time']:.3f}s")

# --- Q4: Count orders per payment method ---
t = time.time()
q4 = rdd.map(lambda r: (r['payment_method'], 1)) \
         .reduceByKey(lambda a, b: a + b) \
         .sortBy(lambda x: -x[1])
results['Q4'] = q4.collect()
results['Q4_time'] = time.time() - t
print("\n=== RDD LINEAGE (Execution Flow) ===")
print(q4.toDebugString())
print(f"Q4 RDD | Payment methods: {results['Q4']} | Time={results['Q4_time']:.3f}s")

# --- Q5: Max single order per customer ---
t = time.time()
q5 = rdd.map(lambda r: (r['customer_id'], float(r['total_price_usd']) if r['total_price_usd'] else 0)) \
         .reduceByKey(lambda a, b: max(a, b)) \
         .sortBy(lambda x: -x[1])
results['Q5'] = q5.take(10)
results['Q5_time'] = time.time() - t
print("\n=== RDD LINEAGE (Execution Flow) ===")
print(q5.toDebugString())
print(f"Q5 RDD | Top spender: {results['Q5'][0]} | Time={results['Q5_time']:.3f}s")

# --- Q6: Orders per month ---
t = time.time()
q6 = rdd.map(lambda r: (str(r['order_date'])[:7] if r['order_date'] else 'Unknown', 1)) \
         .reduceByKey(lambda a, b: a + b) \
         .sortBy(lambda x: x[0])
results['Q6'] = q6.collect()
results['Q6_time'] = time.time() - t
print("\n=== RDD LINEAGE (Execution Flow) ===")
print(q6.toDebugString())
print(f"Q6 RDD | Months: {len(results['Q6'])} | Time={results['Q6_time']:.3f}s")

# --- Q7: Revenue by category + country (multi-group) ---
t = time.time()
q7 = rdd.map(lambda r: ((r['category'], r['country']), float(r['total_price_usd']) if r['total_price_usd'] else 0)) \
         .reduceByKey(lambda a, b: a + b) \
         .sortBy(lambda x: -x[1])
results['Q7'] = q7.take(10)
results['Q7_time'] = time.time() - t
print("\n=== RDD LINEAGE (Execution Flow) ===")
print(q7.toDebugString())
print(f"Q7 RDD | Top group: {results['Q7'][0]} | Time={results['Q7_time']:.3f}s")

# --- Q8: Count distinct products per category ---
t = time.time()
q8 = rdd.map(lambda r: (r['category'], r['product_id'])) \
         .distinct() \
         .map(lambda r: (r[0], 1)) \
         .reduceByKey(lambda a, b: a + b) \
         .sortBy(lambda x: -x[1])
results['Q8'] = q8.collect()
results['Q8_time'] = time.time() - t
print("\n=== RDD LINEAGE (Execution Flow) ===")
print(q8.toDebugString())
print(f"Q8 RDD | Distinct products/category: {results['Q8'][:3]} | Time={results['Q8_time']:.3f}s")

# --- Q9: Cancelled orders rate per country ---
t = time.time()
total_by_country = rdd.map(lambda r: (r['country'], 1)).reduceByKey(lambda a,b: a+b)
cancelled = rdd.filter(lambda r: r['order_status'] == 'Cancelled') \
               .map(lambda r: (r['country'], 1)).reduceByKey(lambda a,b: a+b)
q9 = cancelled.join(total_by_country) \
              .mapValues(lambda x: round(x[0]/x[1]*100, 2)) \
              .sortBy(lambda x: -x[1])
results['Q9'] = q9.take(10)
results['Q9_time'] = time.time() - t
print("\n=== RDD LINEAGE (Execution Flow) ===")
print(q9.toDebugString())
print(f"Q9 RDD | Top cancelled rate: {results['Q9'][0]} | Time={results['Q9_time']:.3f}s")

# --- Q10: High-value customers (total spend > 5000) ---
t = time.time()
q10 = rdd.map(lambda r: (r['customer_id'], float(r['total_price_usd']) if r['total_price_usd'] else 0)) \
          .reduceByKey(lambda a, b: a + b) \
          .filter(lambda x: x[1] > 5000) \
          .sortBy(lambda x: -x[1])
results['Q10_count'] = q10.count()
results['Q10_time'] = time.time() - t
print("\n=== RDD LINEAGE (Execution Flow) ===")
print(q10.toDebugString())
print(f"Q10 RDD | High-value customers: {results['Q10_count']} | Time={results['Q10_time']:.3f}s")

# --- Save timing summary ---
import json
with open('results/rdd_timings.json', 'w') as f:
    timing = {k: v for k, v in results.items() if 'time' in k}
    json.dump(timing, f, indent=2)
print("\nAll RDD timings saved to results/rdd_timings.json")
spark.stop()

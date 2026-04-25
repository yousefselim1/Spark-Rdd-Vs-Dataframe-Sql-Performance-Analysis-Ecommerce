import time
from setup import load_data
from cluster_config import create_spark
from pyspark.sql import functions as F

configs = [
    {"partitions": 4, "memory": "1g"},
    {"partitions": 8, "memory": "2g"},
    {"partitions": 16, "memory": "4g"},
]

results = []

for config in configs:
    print("\n" + "="*50)
    print(f"Testing config: {config}")
    print("="*50)

    spark = create_spark(
        partitions=config["partitions"],
        memory=config["memory"]
    )

    df = load_data(spark)

    t = time.time()

    # Simple aggregation test
    df.groupBy("category").agg(F.sum("total_price_usd")).show()

    elapsed = time.time() - t

    results.append({
        "partitions": config["partitions"],
        "memory": config["memory"],
        "time": elapsed
    })

    print(f"Execution Time: {elapsed:.3f}s")

    spark.stop()

print("\n=== SCALABILITY RESULTS ===")
for r in results:
    print(r)

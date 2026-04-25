import sys
import os
import time
import json

# Fix import path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from setup import get_spark, load_data

spark = get_spark()
df = load_data(spark)

timings = {}

# Helper function for clean output
def run_query(name, sql_query):
    print("\n" + "="*60)
    print(f"{name} RESULT")
    print("="*60)

    result = spark.sql(sql_query)
    result.show(10)

    print("\n" + "-"*60)
    print(f"{name} EXPLAIN PLAN")
    print("-"*60)
    result.explain(True)

    return result


queries = {

    "Q1 - Filter Completed > 500": """
        SELECT COUNT(*) as completed_over_500
        FROM ecommerce
        WHERE order_status = 'Completed' AND total_price_usd > 500
    """,

    "Q2 - Revenue by cateSgory": """
        SELECT category, ROUND(SUM(total_price_usd),2) AS total_revenue
        FROM ecommerce
        GROUP BY category
        ORDER BY total_revenue DESC
    """,

    "Q3 - Avg order value by country": """
        SELECT country, ROUND(AVG(total_price_usd),2) AS avg_order_value
        FROM ecommerce
        GROUP BY country
        ORDER BY avg_order_value DESC
    """,

    "Q4 - Orders by payment method": """
        SELECT payment_method, COUNT(*) AS order_count
        FROM ecommerce
        GROUP BY payment_method
        ORDER BY order_count DESC
    """,

    "Q5 - Max order per customer": """
        SELECT customer_id, MAX(total_price_usd) AS max_order
        FROM ecommerce
        GROUP BY customer_id
        ORDER BY max_order DESC
        LIMIT 10
    """,

    "Q6 - Orders per month": """
        SELECT DATE_FORMAT(order_date, 'yyyy-MM') AS month,
               COUNT(*) AS order_count
        FROM ecommerce
        GROUP BY month
        ORDER BY month
    """,

    "Q7 - Revenue by category & country": """
        SELECT category, country,
               ROUND(SUM(total_price_usd),2) AS revenue
        FROM ecommerce
        GROUP BY category, country
        ORDER BY revenue DESC
        LIMIT 10
    """,

    "Q8 - Distinct products per category": """
        SELECT category, COUNT(DISTINCT product_id) AS distinct_products
        FROM ecommerce
        GROUP BY category
        ORDER BY distinct_products DESC
    """,

    "Q9 - Window function ranking": """
        SELECT customer_id,
               SUM(total_price_usd) AS total_spend,
               RANK() OVER (ORDER BY SUM(total_price_usd) DESC) AS spend_rank
        FROM ecommerce
        GROUP BY customer_id
        ORDER BY spend_rank
        LIMIT 20
    """,

    "Q10 - High-value customers (subquery)": """
        SELECT *
        FROM (
            SELECT customer_id, SUM(total_price_usd) AS total_spend
            FROM ecommerce
            GROUP BY customer_id
        ) customer_totals
        WHERE total_spend > (
            SELECT AVG(total_spend) FROM (
                SELECT customer_id, SUM(total_price_usd) AS total_spend
                FROM ecommerce
                GROUP BY customer_id
            )
        )
        ORDER BY total_spend DESC
        LIMIT 10
    """
}

# Run all queries
for name, sql_query in queries.items():
    t = time.time()

    run_query(name, sql_query)

    elapsed = time.time() - t
    timings[name] = elapsed

    print(f"\n{name} Time: {elapsed:.3f} seconds")


# Save timings
with open('results/sql_timings.json', 'w') as f:
    json.dump(timings, f, indent=2)

print("\nAll SQL timings saved.")

spark.stop()

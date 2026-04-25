from setup import get_spark, load_data

def main():
    spark = get_spark()

    # Load CSV
    df = load_data(spark, "data/ecommerce.csv")

    print("\n=== CONVERTING TO PARQUET ===")

    # Save as Parquet
    df.write.mode("overwrite").parquet("data/ecommerce.parquet")

    print("✅ Parquet file saved at: data/ecommerce.parquet")

    spark.stop()


if __name__ == "__main__":
    main()

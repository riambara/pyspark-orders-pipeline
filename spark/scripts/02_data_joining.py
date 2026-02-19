"""
2. Data Joining - Combine Orders and Order Items
=================================================
Task: Perform inner join between orders and order_items using order_id
"""

from pyspark.sql import SparkSession

# Initialize Spark
spark = SparkSession.builder \
    .appName("02 - Data Joining") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

# Read CSV files
orders_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("/opt/bitnami/spark/data/orders.csv")

order_items_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("/opt/bitnami/spark/data/order_items.csv")

# Perform JOIN operation
joined_df = orders_df.join(
    order_items_df,
    orders_df.order_id == order_items_df.order_id,
    "inner"
)

# Show results
print("="*50)
print("JOINED DATA (orders + order_items):")
print("="*50)
joined_df.show(5, truncate=False)

print(f"\nTotal records after join: {joined_df.count()}")
print(f"Total columns: {len(joined_df.columns)}")
print("Columns:", joined_df.columns)

spark.stop()
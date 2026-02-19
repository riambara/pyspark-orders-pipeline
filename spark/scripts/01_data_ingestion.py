"""
1. Data Ingestion - Read CSV Files
===================================
Task: Read orders.csv and order_items.csv using PySpark
"""

from pyspark.sql import SparkSession

# Initialize Spark
spark = SparkSession.builder \
    .appName("01 - Data Ingestion") \
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

# Show results
print("="*50)
print("ORDERS DATA:")
print("="*50)
orders_df.show(5)
print(f"Total orders: {orders_df.count()}")

print("\n" + "="*50)
print("ORDER ITEMS DATA:")
print("="*50)
order_items_df.show(5)
print(f"Total order items: {order_items_df.count()}")

spark.stop()
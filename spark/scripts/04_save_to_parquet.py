"""
4. Save Processed Data to Parquet Format
=========================================
Task: Save the joined and transformed data in Parquet format
Mode: overwrite (can be changed to append if needed)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round
from pyspark.sql.types import DecimalType, DateType
import os

# Initialize Spark
spark = SparkSession.builder \
    .appName("04 - Save to Parquet") \
    .master("spark://spark-master:7077") \
    .config("spark.sql.parquet.compression.codec", "snappy") \
    .getOrCreate()

print("="*60)
print("STEP 1: Read and Transform Data")
print("="*60)

# Read CSV files
orders_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("/opt/bitnami/spark/data/orders.csv")

order_items_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("/opt/bitnami/spark/data/order_items.csv")

print(f"✓ Orders loaded: {orders_df.count():,} rows")
print(f"✓ Order items loaded: {order_items_df.count():,} rows")

# Apply transformations (same as script 03)
print("\nApplying transformations...")

# Clean
orders_clean = orders_df.drop("total_amount").filter(col("order_id") > 0)
items_clean = order_items_df.filter(col("quantity") > 0)

# Standardize
orders_std = orders_clean \
    .withColumn("order_date", col("order_date").cast(DateType())) \
    .withColumn("user_id", col("user_id").cast(DecimalType(10,0)))

items_std = items_clean \
    .withColumn("price", col("price").cast(DecimalType(10,2))) \
    .withColumn("quantity", col("quantity").cast(DecimalType(10,0))) \
    .withColumnRenamed("price", "unit_price")

# Create GMV
items_with_gmv = items_std \
    .withColumn("gmv", round(col("quantity") * col("unit_price"), 2))

# Join
final_df = orders_std.join(
    items_with_gmv,
    orders_std.order_id == items_with_gmv.order_id,
    "inner"
).select(
    orders_std.order_id,
    "user_id",
    "order_date",
    "product_id",
    "quantity",
    "unit_price",
    "gmv"
)

print(f"✓ Transformed data: {final_df.count():,} rows")
print("\nSample transformed data:")
final_df.show(5, truncate=False)

# ============================================
# SAVE TO PARQUET
# ============================================
print("\n" + "="*60)
print("STEP 2: Save to Parquet Format")
print("="*60)

# Define output path
output_path = "/opt/bitnami/spark/data/output/processed_orders.parquet"

# Create output directory if it doesn't exist
os.makedirs("/opt/bitnami/spark/data/output", exist_ok=True)

# Save as Parquet with overwrite mode
final_df.write \
    .mode("overwrite") \
    .option("compression", "snappy") \
    .parquet(output_path)

print(f"✓ Data saved to: {output_path}")
print(f"✓ Mode: overwrite")
print(f"✓ Format: Parquet (with Snappy compression)")
print(f"✓ Total rows: {final_df.count():,}")

# ============================================
# VERIFICATION
# ============================================
print("\n" + "="*60)
print("STEP 3: Verification - Read Parquet File")
print("="*60)

# Read back the Parquet file
verification_df = spark.read.parquet(output_path)

print(f"✓ Successfully read Parquet file")
print(f"✓ Rows in Parquet: {verification_df.count():,}")
print("\nSample data from Parquet file:")
verification_df.show(5, truncate=False)

print("\nParquet file schema:")
verification_df.printSchema()

# ============================================
# FILE INFORMATION
# ============================================
print("\n" + "="*60)
print("PARQUET FILE INFORMATION")
print("="*60)

print("""
Benefits of Parquet format:
1. Columnar storage - faster queries
2. Built-in compression - smaller file size
3. Stores schema - no need to inferSchema
4. Predicate pushdown - reads only needed columns

File location in container:
/opt/bitnami/spark/data/output/processed_orders.parquet

To read this file in future sessions:
--------------------------------------
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Read Parquet").getOrCreate()
df = spark.read.parquet("/opt/bitnami/spark/data/output/processed_orders.parquet")
df.show()
""")

print("="*60)
print("🎉 PIPELINE COMPLETED SUCCESSFULLY!")
print("="*60)

spark.stop()
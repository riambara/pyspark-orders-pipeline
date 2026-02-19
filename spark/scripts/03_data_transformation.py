"""
3. Data Transformation - Cleaning, Standardization & Feature Engineering
=========================================================================
Tasks performed:
1. Data Cleaning: drop() unnecessary columns, filter() invalid data
2. Data Standardization: cast() data types, withColumnRenamed()
3. Feature Engineering: Create GMV (Gross Merchandise Value) column
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round
from pyspark.sql.types import DecimalType, DateType

# Initialize Spark
spark = SparkSession.builder \
    .appName("03 - Data Transformation") \
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

print("="*50)
print("ORIGINAL DATA (Before Transformation)")
print("="*50)
print("Orders Schema:")
orders_df.printSchema()
print("\nOrder Items Schema:")
order_items_df.printSchema()

# ============================================
# TRANSFORMATION 1: Data Cleaning
# ============================================
print("\n" + "="*50)
print("TRANSFORMATION 1: Data Cleaning")
print("="*50)

# 1a. Drop unnecessary column (total_amount will be recalculated from items)
orders_clean_df = orders_df.drop("total_amount")
print("✓ Dropped 'total_amount' column from orders")

# 1b. Filter invalid data
orders_clean_df = orders_clean_df.filter(col("order_id") > 0)
order_items_clean_df = order_items_df.filter(
    (col("quantity") > 0) & (col("price") > 0)
)
print("✓ Filtered out invalid records (quantity <= 0 or price <= 0)")

# ============================================
# TRANSFORMATION 2: Data Standardization
# ============================================
print("\n" + "="*50)
print("TRANSFORMATION 2: Data Standardization")
print("="*50)

# 2a. Cast to proper data types
orders_std_df = orders_clean_df \
    .withColumn("order_date", col("order_date").cast(DateType())) \
    .withColumn("user_id", col("user_id").cast(DecimalType(10,0)))

order_items_std_df = order_items_clean_df \
    .withColumn("price", col("price").cast(DecimalType(10,2))) \
    .withColumn("quantity", col("quantity").cast(DecimalType(10,0)))

print("✓ Cast data types:")
print("  - order_date: string → date")
print("  - user_id: int → decimal")
print("  - price: int → decimal(10,2)")
print("  - quantity: int → decimal(10,0)")

# 2b. Rename column for clarity
order_items_std_df = order_items_std_df \
    .withColumnRenamed("price", "unit_price")
print("✓ Renamed 'price' → 'unit_price' for clarity")

# ============================================
# TRANSFORMATION 3: Feature Engineering (GMV)
# ============================================
print("\n" + "="*50)
print("TRANSFORMATION 3: Feature Engineering - GMV")
print("="*50)

# Calculate GMV (Gross Merchandise Value) = quantity * unit_price
order_items_with_gmv_df = order_items_std_df \
    .withColumn("gmv", 
                round(col("quantity") * col("unit_price"), 2))

print("✓ Created new column 'gmv' = quantity × unit_price")
print("✓ GMV rounded to 2 decimal places")

# ============================================
# Join transformed data
# ============================================
print("\n" + "="*50)
print("FINAL TRANSFORMED DATA")
print("="*50)

# Join the transformed datasets
final_df = orders_std_df.join(
    order_items_with_gmv_df,
    orders_std_df.order_id == order_items_with_gmv_df.order_id,
    "inner"
).select(
    orders_std_df.order_id,
    "user_id",
    "order_date",
    "product_id",
    "quantity",
    "unit_price",
    "gmv"
)

# Show results
print("Sample of transformed data (5 rows):")
final_df.show(5, truncate=False)

print(f"\nTotal records: {final_df.count()}")
print("\nFinal Schema:")
final_df.printSchema()

print("\n" + "="*50)
print("SUMMARY OF TRANSFORMATIONS:")
print("="*50)
print("✓ Data Cleaning:")
print("  - Dropped: total_amount")
print("  - Filtered: quantity > 0, price > 0")
print("\n✓ Data Standardization:")
print("  - Cast: order_date (date), user_id (decimal)")
print("  - Cast: unit_price (decimal), quantity (decimal)")
print("  - Renamed: price → unit_price")
print("\n✓ Feature Engineering:")
print("  - Created: gmv = quantity × unit_price")
print("="*50)

spark.stop()
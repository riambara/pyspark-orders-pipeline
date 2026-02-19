# PySpark Data Processing Pipeline

A complete data pipeline for orders analysis using PySpark (ingestion, join, transformation, and Parquet storage).

## 📁 File Structure
├── spark/
│   ├── data/
│   │   ├── orders.csv
│   │   └── order_items.csv
│   └── scripts/
│       ├── 01_data_ingestion.py
│       ├── 02_data_joining.py
│       ├── 03_data_transformation.py
│       └── 04_save_to_parquet.py
└── README.md

## 🔄 Pipeline Steps
| Script | Function | Description |
|--------|----------|-------------|
| 01_data_ingestion.py | Read CSV | Load orders and order_items |
| 02_data_joining.py | Join tables | Inner join on order_id |
| 03_data_transformation.py | Transform | 3 methods: clean (drop/filter), standardize (cast/rename), create GMV |
| 04_save_to_parquet.py | Save | Store as Parquet (overwrite) |

## 🚀 Quick Start
### 1. Start Spark
docker compose --profile spark up -d

### 2. Copy data to ALL workers
for worker in spark-master spark-worker-1 spark-worker-2; do
  docker cp spark/data/orders.csv $worker:/opt/bitnami/spark/data/
  docker cp spark/data/order_items.csv $worker:/opt/bitnami/spark/data/
done

### 3. Copy scripts
docker cp spark/scripts/. spark-master:/opt/bitnami/spark/scripts/

### 4. Run pipeline
for script in 01_data_ingestion 02_data_joining 03_data_transformation 04_save_to_parquet; do
  docker exec -it spark-master spark-submit \
    --master spark://spark-master:7077 \
    /opt/bitnami/spark/scripts/$script.py
done

## 📊 Transformations Applied
| Category | Method | Description |
|----------|--------|-------------|
| Clean | drop() | Remove total_amount |
| | filter() | quantity > 0, price > 0 |
| Standardize | cast() | String→Date, Int→Decimal |
| | withColumnRenamed() | price → unit_price |
| Feature | withColumn() | GMV = quantity × unit_price |

## 💾 Output
- Location: /opt/bitnami/spark/data/output/processed_orders.parquet
- Format: Parquet (Snappy compression)
- Records: 3,000 transactions
- Columns: order_id, user_id, order_date, product_id, quantity, unit_price, gmv

## 📖 Read saved data
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
df = spark.read.parquet("/opt/bitnami/spark/data/output/processed_orders.parquet")
df.show(5)
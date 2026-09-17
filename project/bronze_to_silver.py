import os
import glob
import pyarrow as pa
import pyarrow.parquet as pq

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, col, row_number
from pyspark.sql.window import Window


spark = (
    SparkSession.builder
    .appName("BronzeToSilver")
    .config("spark.hadoop.hadoop.home.dir", "C:/hadoop")
    .config("spark.hadoop.io.native.lib.available", "false")
    .config(
        "spark.sql.warehouse.dir",
        "D:/DataEngineering/spark-warehouse"
    )
    .getOrCreate()
)


BRONZE_DIR = "D:/DataEngineering/data/bronze/orders"
SILVER_DIR = "D:/DataEngineering/data/silver/orders"
SILVER_FILE = os.path.join(
    SILVER_DIR,
    "orders.parquet"
)


print("\n===================================")
print("BRONZE to SILVER")
print("===================================")


# --------------------------------------------------
# 1. Find Bronze files
# --------------------------------------------------

bronze_files = sorted(
    glob.glob(
        os.path.join(
            BRONZE_DIR,
            "kafka_batch_*.parquet"
        )
    )
)

print(f"Bronze files found: {len(bronze_files)}")


if not bronze_files:
    print("ERROR: No Bronze files found.")
    spark.stop()
    exit(1)


# --------------------------------------------------
# 2. Read Bronze using PyArrow
# --------------------------------------------------

tables = []

for file in bronze_files:
    print(f"Reading: {os.path.basename(file)}")
    tables.append(
        pq.read_table(file)
    )


combined_table = pa.concat_tables(
    tables,
    promote_options="default"
)

print(
    f"Bronze records: {combined_table.num_rows}"
)


# --------------------------------------------------
# 3. Create Spark DataFrame
# --------------------------------------------------

records = combined_table.to_pylist()

orders = spark.createDataFrame(records)

print(
    f"Spark records: {orders.count()}"
)


# --------------------------------------------------
# 4. Convert timestamp
# --------------------------------------------------

orders = orders.withColumn(
    "timestamp",
    to_timestamp("timestamp")
)


# --------------------------------------------------
# 5. Remove invalid timestamps
# --------------------------------------------------

orders = orders.filter(
    col("timestamp").isNotNull()
)

print(
    f"After timestamp validation: {orders.count()}"
)


# --------------------------------------------------
# 6. Remove duplicate orders
# --------------------------------------------------

window_spec = (
    Window
    .partitionBy("order_id")
    .orderBy(
        col("timestamp").desc()
    )
)


orders = (
    orders
    .withColumn(
        "row_number",
        row_number().over(window_spec)
    )
    .filter(
        col("row_number") == 1
    )
    .drop("row_number")
)


print(
    f"After deduplication: {orders.count()}"
)


# --------------------------------------------------
# 7. Write Silver
# --------------------------------------------------

rows = orders.collect()

silver_records = [
    row.asDict()
    for row in rows
]


os.makedirs(
    SILVER_DIR,
    exist_ok=True
)


silver_table = pa.Table.from_pylist(
    silver_records
)


pq.write_table(
    silver_table,
    SILVER_FILE,
    compression="snappy"
)


print("\n===================================")
print("Silver transformation completed!")
print("===================================")

print(
    f"Records written: {len(silver_records)}"
)

print(
    f"Silver file: {SILVER_FILE}"
)


spark.stop()
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when

spark = (
    SparkSession.builder
    .appName("DataQualityChecks")
    .getOrCreate()
)

# Read Silver
silver_path = "D:/DataEngineering/data/silver/orders/orders.parquet"

orders = spark.read.parquet(silver_path)

print("\n========== DATA QUALITY REPORT ==========\n")

total_records = orders.count()

print(f"Total records: {total_records}")

# 1. Missing order IDs
missing_order_id = orders.filter(
    col("order_id").isNull()
).count()

# 2. Missing customer IDs
missing_customer_id = orders.filter(
    col("customer_id").isNull()
).count()

# 3. Invalid amounts
invalid_amount = orders.filter(
    (col("amount").isNull()) | (col("amount") <= 0)
).count()

# 4. Missing cities
missing_city = orders.filter(
    col("city").isNull()
).count()

# 5. Missing payment methods
missing_payment = orders.filter(
    col("payment_method").isNull()
).count()

# 6. Missing timestamps
missing_timestamp = orders.filter(
    col("timestamp").isNull()
).count()

# 7. Duplicate order IDs
duplicate_order_ids = (
    orders.groupBy("order_id")
    .count()
    .filter(col("count") > 1)
    .count()
)

print(f"Missing order_id:       {missing_order_id}")
print(f"Missing customer_id:    {missing_customer_id}")
print(f"Invalid amount:         {invalid_amount}")
print(f"Missing city:           {missing_city}")
print(f"Missing payment_method: {missing_payment}")
print(f"Missing timestamp:      {missing_timestamp}")
print(f"Duplicate order IDs:    {duplicate_order_ids}")

# Overall status
if (
    missing_order_id == 0
    and missing_customer_id == 0
    and invalid_amount == 0
    and missing_city == 0
    and missing_payment == 0
    and missing_timestamp == 0
    and duplicate_order_ids == 0
):
    print("\nDATA QUALITY STATUS: PASSED")
else:
    print("\nDATA QUALITY STATUS: FAILED")

spark.stop()
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import pyarrow as pa
import pyarrow.parquet as pq
import os


spark = (
    SparkSession.builder
    .appName("EcommerceKafkaConsumer")
    .getOrCreate()
)


order_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("amount", IntegerType(), True),
    StructField("city", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("timestamp", StringType(), True)
])


df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "orders")
    .option("startingOffsets", "earliest")
    .load()
)


json_df = df.selectExpr("CAST(value AS STRING) AS json_string")


orders = (
    json_df
    .select(
        from_json(col("json_string"), order_schema).alias("data")
    )
    .select("data.*")
)


bronze_dir = "D:/DataEngineering/data/bronze/orders"

os.makedirs(bronze_dir, exist_ok=True)


def write_batch(batch_df, batch_id):

    if batch_df.isEmpty():
        return

    rows = batch_df.collect()

    records = [row.asDict() for row in rows]

    table = pa.Table.from_pylist(records)

    output_file = os.path.join(
        bronze_dir,
        f"batch_{batch_id}.parquet"
    )

    pq.write_table(
        table,
        output_file,
        compression="snappy"
    )

    print(
        f"Bronze batch {batch_id} written successfully: "
        f"{len(records)} records"
    )


query = (
    orders.writeStream
    .foreachBatch(write_batch)
    .outputMode("append")
    .trigger(once=True)
    .start()
)


query.awaitTermination()
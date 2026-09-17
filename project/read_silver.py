from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("ReadSilver")
    .getOrCreate()
)

df = spark.read.parquet(
    "D:/DataEngineering/data/silver/orders/orders.parquet"
)

df.show(20, truncate=False)

print("Total Silver records:", df.count())

spark.stop()
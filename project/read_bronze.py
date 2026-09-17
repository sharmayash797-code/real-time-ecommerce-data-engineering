from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("ReadBronze")
    .getOrCreate()
)

df = spark.read.parquet(
    "D:/DataEngineering/data/bronze/orders/part-00000-f1702674-a8ba-4af6-89e3-44de7a4acfbf-c000.snappy.parquet"
)

df.show(20, truncate=False)

spark.stop()
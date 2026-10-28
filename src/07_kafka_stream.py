from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType
)

spark = (
    SparkSession.builder
    .appName("Taxi-Realtime-Streaming")
    .config("spark.hadoop.dfs.replication", "3")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("pickup_datetime", StringType(), True),
    StructField("pickup_zone_id", IntegerType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("total_amount", DoubleType(), True)
])

print("===== CONNECTING TO KAFKA =====")

kafka_df = (
    spark.readStream
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        "localhost:9092"
    )
    .option(
        "subscribe",
        "taxi-events"
    )
    .option(
        "startingOffsets",
        "latest"
    )
    .load()
)

parsed = (
    kafka_df
    .select(
        F.from_json(
            F.col("value").cast("string"),
            schema
        ).alias("data")
    )
    .select("data.*")
    .filter(F.col("pickup_datetime").isNotNull())
    .withColumn(
        "pickup_datetime",
        F.to_timestamp("pickup_datetime")
    )
)

print("===== REALTIME STREAM STARTED =====")
print("Waiting for taxi events from Kafka...")

console_query = (
    parsed.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .trigger(processingTime="2 seconds")
    .start()
)

hdfs_query = (
    parsed.writeStream
    .format("parquet")
    .outputMode("append")
    .option(
        "path",
        "hdfs://localhost:9000/taxi-demand/stream/taxi_events"
    )
    .option(
        "checkpointLocation",
        "hdfs://localhost:9000/taxi-demand/checkpoints/taxi_events_realtime"
    )
    .trigger(processingTime="2 seconds")
    .start()
)

spark.streams.awaitAnyTermination()

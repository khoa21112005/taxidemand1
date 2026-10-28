from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import (
    StructType, StructField, LongType,
    StringType, DoubleType
)

spark = (
    SparkSession.builder
    .appName("TaxiRealtimeStreaming")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("event_id", LongType(), True),
    StructField("pickup_datetime", StringType(), True),
    StructField("pickup_zone_id", LongType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("total_amount", DoubleType(), True)
])

print("===== TAXI REALTIME STREAMING STARTED =====")

kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "taxi-events")
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

events = (
    kafka_df
    .selectExpr("CAST(value AS STRING) AS json")
    .select(from_json(col("json"), schema).alias("event"))
    .select("event.*")
    .withColumn("processed_at", current_timestamp())
)

console_query = (
    events.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .trigger(processingTime="2 seconds")
    .start()
)

hdfs_query = (
    events.writeStream
    .format("parquet")
    .outputMode("append")
    .option(
        "path",
        "hdfs://localhost:9000/taxi-demand/stream/realtime_events"
    )
    .option(
        "checkpointLocation",
        "hdfs://localhost:9000/taxi-demand/checkpoints/realtime_events"
    )
    .trigger(processingTime="5 seconds")
    .start()
)

spark.streams.awaitAnyTermination()

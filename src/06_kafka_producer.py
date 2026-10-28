import json
import time

from kafka import KafkaProducer
from pyspark.sql import SparkSession


spark = (
    SparkSession.builder
    .appName("Taxi-Kafka-Producer")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


INPUT = "hdfs://localhost:9000/taxi-demand/processed/taxi_cleaned"


df = (
    spark.read.parquet(INPUT)
    .select(
        "pickup_datetime",
        "pickup_zone_id",
        "trip_distance",
        "fare_amount",
        "total_amount"
    )
    .limit(1000)
)


producer = KafkaProducer(
    bootstrap_servers="localhost:9092",

    value_serializer=lambda value:
        json.dumps(value).encode("utf-8")
)


count = 0

for row in df.toLocalIterator():

    event = {
        "pickup_datetime":
            row.pickup_datetime.isoformat(),

        "pickup_zone_id":
            row.pickup_zone_id,

        "trip_distance":
            row.trip_distance,

        "fare_amount":
            row.fare_amount,

        "total_amount":
            row.total_amount
    }

    producer.send(
        "taxi-events",
        value=event
    )

    count += 1

    print(
        "SEND",
        count,
        event
    )

    time.sleep(0.02)


producer.flush()

print("")
print("===== PRODUCER DONE =====")
print("Total events:", count)

spark.stop()

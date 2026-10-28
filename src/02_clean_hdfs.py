from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("Taxi-Clean-2023-2024")
    .config("spark.sql.parquet.enableVectorizedReader", "false")
    .config("spark.hadoop.dfs.replication", "3")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

INPUT_BASE = "hdfs://localhost:9000/taxi-demand/raw"
OUTPUT_BASE = "hdfs://localhost:9000/taxi-demand/processed/taxi_cleaned"

raw_total = 0
clean_total = 0

for year in [2023, 2024]:
    for month in range(1, 13):

        input_path = (
            f"{INPUT_BASE}/{year}/"
            f"yellow_tripdata_{year}-{month:02d}.parquet"
        )

        output_path = (
            f"{OUTPUT_BASE}/year={year}/month={month:02d}"
        )

        print("=" * 60)
        print(f"PROCESSING {year}-{month:02d}")
        print("INPUT :", input_path)
        print("OUTPUT:", output_path)

        raw = spark.read.parquet(input_path)

        taxi = raw.select(
            F.col("tpep_pickup_datetime")
                .cast("timestamp")
                .alias("pickup_datetime"),

            F.col("tpep_dropoff_datetime")
                .cast("timestamp")
                .alias("dropoff_datetime"),

            F.col("PULocationID")
                .cast("int")
                .alias("pickup_zone_id"),

            F.col("DOLocationID")
                .cast("int")
                .alias("dropoff_zone_id"),

            F.col("trip_distance")
                .cast("double")
                .alias("trip_distance"),

            F.col("fare_amount")
                .cast("double")
                .alias("fare_amount"),

            F.col("total_amount")
                .cast("double")
                .alias("total_amount")
        )

        raw_count = taxi.count()

        cleaned = (
            taxi
            .filter(F.col("pickup_datetime").isNotNull())
            .filter(F.col("dropoff_datetime").isNotNull())
            .filter(F.col("pickup_zone_id").between(1, 265))
            .filter(F.col("dropoff_zone_id").between(1, 265))
            .filter(F.col("dropoff_datetime") >= F.col("pickup_datetime"))
            .filter(F.col("trip_distance") >= 0)
            .filter(F.col("fare_amount") >= 0)
            .filter(F.col("total_amount") >= 0)
            .filter(
                (F.col("pickup_datetime") >=
                 F.lit("2023-01-01 00:00:00").cast("timestamp"))
                &
                (F.col("pickup_datetime") <
                 F.lit("2025-01-01 00:00:00").cast("timestamp"))
            )
            .withColumn(
                "pickup_date",
                F.to_date("pickup_datetime")
            )
            .withColumn(
                "pickup_hour",
                F.hour("pickup_datetime")
            )
            .withColumn(
                "pickup_month",
                F.month("pickup_datetime")
            )
            .withColumn(
                "pickup_year",
                F.year("pickup_datetime")
            )
            .withColumn(
                "day_of_week",
                F.dayofweek("pickup_datetime")
            )
            .withColumn(
                "day_type",
                F.when(
                    F.dayofweek("pickup_datetime").isin(1, 7),
                    "Weekend"
                ).otherwise("Weekday")
            )
        )

        clean_count = cleaned.count()

        print("RAW   :", raw_count)
        print("CLEAN :", clean_count)
        print("REMOVED:", raw_count - clean_count)

        cleaned.write \
            .mode("overwrite") \
            .parquet(output_path)

        raw_total += raw_count
        clean_total += clean_count

        print(f"DONE {year}-{month:02d}")

print("")
print("===== FINAL CLEANING RESULT =====")
print("Raw records     :", raw_total)
print("Clean records   :", clean_total)
print("Removed records :", raw_total - clean_total)

spark.stop()

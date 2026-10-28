from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Taxi-Save-Results")
    .config("spark.hadoop.dfs.replication", "3")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

INPUT = "hdfs://localhost:9000/taxi-demand/processed/taxi_cleaned"
OUTPUT = "hdfs://localhost:9000/taxi-demand/output"

df = spark.read.parquet(INPUT)

df.createOrReplaceTempView("taxi")

hourly = spark.sql("""
SELECT
    pickup_hour,
    COUNT(*) AS total_trips
FROM taxi
GROUP BY pickup_hour
ORDER BY pickup_hour
""")

monthly = spark.sql("""
SELECT
    pickup_year,
    pickup_month,
    COUNT(*) AS total_trips
FROM taxi
GROUP BY pickup_year, pickup_month
ORDER BY pickup_year, pickup_month
""")

weekday_weekend = spark.sql("""
SELECT
    day_type,
    COUNT(*) AS total_trips
FROM taxi
GROUP BY day_type
ORDER BY total_trips DESC
""")

top_zones = spark.sql("""
SELECT
    pickup_zone_id AS zone_id,
    COUNT(*) AS total_trips
FROM taxi
GROUP BY pickup_zone_id
ORDER BY total_trips DESC
LIMIT 15
""")

hourly.write.mode("overwrite").parquet(
    f"{OUTPUT}/hourly_demand"
)

monthly.write.mode("overwrite").parquet(
    f"{OUTPUT}/monthly_demand"
)

weekday_weekend.write.mode("overwrite").parquet(
    f"{OUTPUT}/weekday_weekend"
)

top_zones.write.mode("overwrite").parquet(
    f"{OUTPUT}/top_zones"
)

print("===== DONE STEP 7 =====")
print("Saved:")
print("hourly_demand")
print("monthly_demand")
print("weekday_weekend")
print("top_zones")

spark.stop()

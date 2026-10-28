from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Taxi-Spark-SQL")
    .config("spark.hadoop.dfs.replication", "3")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

INPUT = "hdfs://localhost:9000/taxi-demand/processed/taxi_cleaned"
OUTPUT = "hdfs://localhost:9000/taxi-demand/output"

print("\n===== READ CLEANED DATA =====")

df = spark.read.parquet(INPUT)

df.printSchema()

df.createOrReplaceTempView("taxi")


print("\n===== 1. TONG SO CHUYEN =====")

total = spark.sql("""
SELECT COUNT(*) AS total_trips
FROM taxi
""")

total.show()


print("\n===== 2. SO CHUYEN THEO NAM =====")

yearly = spark.sql("""
SELECT
    pickup_year,
    COUNT(*) AS total_trips
FROM taxi
GROUP BY pickup_year
ORDER BY pickup_year
""")

yearly.show()


print("\n===== 3. NHU CAU THEO 24 GIO =====")

hourly = spark.sql("""
SELECT
    pickup_hour,
    COUNT(*) AS total_trips
FROM taxi
GROUP BY pickup_hour
ORDER BY pickup_hour
""")

hourly.show(24)


print("\n===== 4. NHU CAU THEO THANG =====")

monthly = spark.sql("""
SELECT
    pickup_year,
    pickup_month,
    COUNT(*) AS total_trips
FROM taxi
GROUP BY pickup_year, pickup_month
ORDER BY pickup_year, pickup_month
""")

monthly.show(24)


print("\n===== 5. TOP 15 PICKUP ZONE =====")

top_zones = spark.sql("""
SELECT
    pickup_zone_id AS zone_id,
    COUNT(*) AS total_trips
FROM taxi
GROUP BY pickup_zone_id
ORDER BY total_trips DESC
LIMIT 15
""")

top_zones.show(15)


print("\n===== 6. WEEKDAY VS WEEKEND =====")

weekday_weekend = spark.sql("""
SELECT
    day_type,
    COUNT(*) AS total_trips
FROM taxi
GROUP BY day_type
ORDER BY total_trips DESC
""")

weekday_weekend.show()


print("\n===== 7. KHOANG CACH TRUNG BINH =====")

spark.sql("""
SELECT
    ROUND(AVG(trip_distance), 2) AS avg_trip_distance
FROM taxi
""").show()


print("\n===== 8. GIA CUOC TRUNG BINH =====")

spark.sql("""
SELECT
    ROUND(AVG(fare_amount), 2) AS avg_fare,
    ROUND(AVG(total_amount), 2) AS avg_total
FROM taxi
""").show()


print("\n===== 9. TOP 10 GIO DONG KHACH =====")

spark.sql("""
SELECT
    pickup_hour,
    COUNT(*) AS total_trips
FROM taxi
GROUP BY pickup_hour
ORDER BY total_trips DESC
LIMIT 10
""").show(10)


print("\n===== 10. ZONE x HOUR x DEMAND =====")

demand = spark.sql("""
SELECT
    pickup_zone_id AS zone_id,
    DATE_TRUNC('hour', pickup_datetime) AS pickup_hour,
    COUNT(*) AS demand
FROM taxi
GROUP BY
    pickup_zone_id,
    DATE_TRUNC('hour', pickup_datetime)
""")

demand.orderBy(
    "pickup_hour",
    "zone_id"
).show(30, truncate=False)


print("\n===== DATA RANGE =====")

spark.sql("""
SELECT
    MIN(pickup_datetime) AS min_time,
    MAX(pickup_datetime) AS max_time,
    COUNT(DISTINCT pickup_zone_id) AS total_zones
FROM taxi
""").show(truncate=False)


print("\n===== SAVE OUTPUT TO HDFS =====")

hourly.write \
    .mode("overwrite") \
    .parquet(f"{OUTPUT}/hourly_demand")

monthly.write \
    .mode("overwrite") \
    .parquet(f"{OUTPUT}/monthly_demand")

top_zones.write \
    .mode("overwrite") \
    .parquet(f"{OUTPUT}/top_zones")

weekday_weekend.write \
    .mode("overwrite") \
    .parquet(f"{OUTPUT}/weekday_weekend")

demand.write \
    .mode("overwrite") \
    .parquet(f"{OUTPUT}/demand_by_zone_hour")


print("\n===== DONE STEP 5 =====")

spark.stop()

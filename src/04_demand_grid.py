from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("Taxi-Demand-Grid")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.hadoop.dfs.replication", "3")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

INPUT = "hdfs://localhost:9000/taxi-demand/processed/taxi_cleaned"
OUTPUT = "hdfs://localhost:9000/taxi-demand/processed/demand_hourly"

print("\n===== READ CLEANED DATA =====")

df = spark.read.parquet(INPUT)

print("\n===== CREATE OBSERVED DEMAND =====")

observed = (
    df
    .withColumn(
        "pickup_hour",
        F.date_trunc("hour", "pickup_datetime")
    )
    .groupBy(
        F.col("pickup_zone_id").alias("zone_id"),
        "pickup_hour"
    )
    .agg(
        F.count("*").alias("demand")
    )
)

print("\n===== CREATE 265 ZONES =====")

zones = (
    spark.range(1, 266)
    .select(
        F.col("id").cast("int").alias("zone_id")
    )
)

print("\n===== CREATE ALL HOURS 2023-2024 =====")

hours = spark.sql("""
SELECT explode(
    sequence(
        timestamp('2023-01-01 00:00:00'),
        timestamp('2024-12-31 23:00:00'),
        interval 1 hour
    )
) AS pickup_hour
""")

hours_count = hours.count()

print("Hours:", hours_count)

print("\n===== CREATE FULL GRID =====")

grid = zones.crossJoin(hours)

expected_rows = grid.count()

print("Expected rows:", expected_rows)

print("\n===== JOIN DEMAND =====")

demand_hourly = (
    grid
    .join(
        observed,
        ["zone_id", "pickup_hour"],
        "left"
    )
    .fillna({"demand": 0})
)

print("\n===== VALIDATION =====")

rows = demand_hourly.count()

zone_count = (
    demand_hourly
    .select("zone_id")
    .distinct()
    .count()
)

hour_count = (
    demand_hourly
    .select("pickup_hour")
    .distinct()
    .count()
)

duplicate_count = (
    demand_hourly
    .groupBy("zone_id", "pickup_hour")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

zero_count = (
    demand_hourly
    .filter(F.col("demand") == 0)
    .count()
)

print("")
print("===== DEMAND GRID RESULT =====")
print("Rows       :", rows)
print("Zones      :", zone_count)
print("Hours      :", hour_count)
print("Duplicates :", duplicate_count)
print("Zero demand:", zero_count)

print("\n===== SAMPLE =====")

demand_hourly.orderBy(
    "pickup_hour",
    "zone_id"
).show(30, truncate=False)

print("\n===== WRITE TO HDFS =====")

demand_hourly.write \
    .mode("overwrite") \
    .parquet(OUTPUT)

print("\n===== DONE STEP 6 =====")
print("Saved:", OUTPUT)

spark.stop()

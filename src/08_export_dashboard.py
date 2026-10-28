"""Export small, dashboard-ready datasets from HDFS.

Only aggregates and model outputs are copied locally. Raw taxi trips and the
full demand grid remain in HDFS and are never loaded into pandas.
"""

from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


spark = (
    SparkSession.builder.appName("Taxi-Dashboard-Export")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

BASE = "hdfs://localhost:9000/taxi-demand"
LOCAL = Path("/home/khoa2/taxi-demand/dashboard/data")
LOOKUP_LOCAL = LOCAL / "prediction_lookup"

LOCAL.mkdir(parents=True, exist_ok=True)


def write_csv(df, name: str, order_by=None):
    if order_by:
        df = df.orderBy(*order_by)
    output = LOCAL / name
    df.toPandas().to_csv(output, index=False)
    print(f"Exported: {output}")


def hdfs_exists(path: str) -> bool:
    jvm = spark._jvm
    uri = jvm.java.net.URI(path)
    filesystem = jvm.org.apache.hadoop.fs.FileSystem.get(uri, spark._jsc.hadoopConfiguration())
    return filesystem.exists(jvm.org.apache.hadoop.fs.Path(path))


print("===== EXPORT DASHBOARD DATA =====")

hourly = spark.read.parquet(f"{BASE}/output/hourly_demand")
monthly = spark.read.parquet(f"{BASE}/output/monthly_demand")
top_zones = spark.read.parquet(f"{BASE}/output/top_zones")
weekday = spark.read.parquet(f"{BASE}/output/weekday_weekend")
grid = spark.read.parquet(f"{BASE}/processed/demand_hourly")

write_csv(hourly, "hourly_demand.csv", ["pickup_hour"])
write_csv(monthly, "monthly_demand.csv", ["pickup_year", "pickup_month"])
write_csv(top_zones, "top_zones.csv", [F.desc("total_trips")])
write_csv(weekday, "weekday_weekend.csv")

total_trips = int(hourly.agg(F.sum("total_trips")).first()[0])
total_zones = grid.select("zone_id").distinct().count()
total_hours = grid.select("pickup_hour").distinct().count()
grid_rows = grid.count()

spark.createDataFrame(
    [{
        "total_trips": total_trips,
        "total_zones": total_zones,
        "total_hours": total_hours,
        "grid_rows": grid_rows,
    }]
).toPandas().to_csv(LOCAL / "kpi.csv", index=False)

prediction_paths = {
    "metrics": f"{BASE}/output/model_metrics",
    "metadata": f"{BASE}/output/model_metadata",
    "daily": f"{BASE}/output/prediction_daily",
    "zones": f"{BASE}/output/prediction_by_zone",
    "lookup": f"{BASE}/output/prediction_lookup",
}

missing_prediction_paths = [
    path for path in prediction_paths.values() if not hdfs_exists(path)
]

if missing_prediction_paths:
    print("Prediction export skipped. Missing HDFS paths:")
    for path in missing_prediction_paths:
        print(f"- {path}")
else:
    metrics = spark.read.parquet(prediction_paths["metrics"])
    metadata = spark.read.parquet(prediction_paths["metadata"])
    daily = spark.read.parquet(prediction_paths["daily"])
    zones = spark.read.parquet(prediction_paths["zones"])
    lookup = spark.read.parquet(prediction_paths["lookup"])

    write_csv(metrics, "model_metrics.csv", ["split"])
    write_csv(metadata, "model_metadata.csv")
    write_csv(daily, "prediction_daily.csv", ["date"])
    write_csv(zones, "prediction_by_zone.csv", [F.desc("predicted_demand")])

    (
        lookup.repartition("zone_id")
        .write.mode("overwrite")
        .partitionBy("zone_id")
        .parquet(f"file://{LOOKUP_LOCAL}")
    )
    print(f"Exported: {LOOKUP_LOCAL}")

stream_path = f"{BASE}/stream/taxi_events"
if hdfs_exists(stream_path):
    realtime = (
        spark.read.parquet(stream_path)
        .orderBy(F.desc("pickup_datetime"))
        .limit(200)
    )
    write_csv(realtime, "realtime_events.csv", [F.desc("pickup_datetime")])
else:
    print("Realtime snapshot skipped: Kafka stream output does not exist yet")

print("===== DASHBOARD DATA EXPORTED =====")
print(f"Total trips : {total_trips:,}")
print(f"Total zones : {total_zones:,}")
print(f"Total hours : {total_hours:,}")
print(f"Grid rows   : {grid_rows:,}")
print(f"Saved to    : {LOCAL}")

spark.stop()

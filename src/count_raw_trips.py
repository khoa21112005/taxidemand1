from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("CountRawTaxiTrips")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

total = 0

for year in [2023, 2024]:
    year_total = 0

    for month in range(1, 13):
        path = (
            f"hdfs://localhost:9000/taxi-demand/raw/{year}/"
            f"yellow_tripdata_{year}-{month:02d}.parquet"
        )

        count = spark.read.parquet(path).count()
        year_total += count
        print(f"Thang {month:02d}/{year}: {count:,} chuyen")

    print(f"TONG NAM {year}: {year_total:,} chuyen")
    total += year_total

print("=" * 45)
print(f"TONG SO CHUYEN RAW: {total:,}")

spark.stop()

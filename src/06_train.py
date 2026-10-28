"""Train and evaluate a Spark ML model for hourly taxi demand.

The split is chronological to avoid future leakage:

* train:      2023-01-01 through 2024-09-30
* validation: 2024-10-01 through 2024-10-31
* test:       2024-11-01 through 2024-12-31

The script reads the complete demand grid from HDFS, trains a
RandomForestRegressor and writes the model, metrics, test predictions and a
compact calendar lookup used by the Streamlit dashboard back to HDFS.
"""

import argparse
from datetime import datetime, timezone

from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


DEFAULT_INPUT = "hdfs://localhost:9000/taxi-demand/processed/demand_hourly"
DEFAULT_OUTPUT = "hdfs://localhost:9000/taxi-demand/output"
DEFAULT_MODEL = "hdfs://localhost:9000/taxi-demand/models/random_forest_demand"

TRAIN_END = "2024-10-01 00:00:00"
VALIDATION_END = "2024-11-01 00:00:00"
TEST_END = "2025-01-01 00:00:00"

CALENDAR_COLUMNS = [
    "zone_id",
    "hour_of_day",
    "day_of_week",
    "month",
    "is_weekend",
]
PROFILE_COLUMNS = ["zone_id", "hour_of_day", "day_of_week", "month"]
FEATURE_COLUMNS = [*CALENDAR_COLUMNS, "historical_mean"]


def parse_args():
    parser = argparse.ArgumentParser(
        description="Train Spark ML Random Forest for taxi demand"
    )
    parser.add_argument("--input", default=DEFAULT_INPUT)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--num-trees", type=int, default=20)
    parser.add_argument("--max-depth", type=int, default=8)
    parser.add_argument(
        "--smoke-test",
        action="store_true",
        help="Fit a tiny synthetic dataset without writing any output",
    )
    return parser.parse_args()


def add_calendar_features(df: DataFrame) -> DataFrame:
    """Create features known at prediction time."""
    return (
        df.withColumn("hour_of_day", F.hour("pickup_hour").cast("double"))
        .withColumn("day_of_week", F.dayofweek("pickup_hour").cast("double"))
        .withColumn("month", F.month("pickup_hour").cast("double"))
        .withColumn(
            "is_weekend",
            F.when(F.dayofweek("pickup_hour").isin(1, 7), 1.0).otherwise(0.0),
        )
        .withColumn("zone_id", F.col("zone_id").cast("double"))
        .withColumn("demand", F.col("demand").cast("double"))
        .select("pickup_hour", "demand", *CALENDAR_COLUMNS)
    )


def attach_historical_profile(
    df: DataFrame,
    profile: DataFrame,
    global_mean: float,
) -> DataFrame:
    """Join a train-only seasonal mean and provide a safe fallback."""
    return (
        df.join(profile, PROFILE_COLUMNS, "left")
        .fillna({"historical_mean": float(global_mean)})
    )


def create_pipeline(num_trees: int, max_depth: int) -> Pipeline:
    assembler = VectorAssembler(
        inputCols=FEATURE_COLUMNS,
        outputCol="features",
        handleInvalid="error",
    )
    regressor = RandomForestRegressor(
        labelCol="demand",
        featuresCol="features",
        predictionCol="prediction",
        numTrees=num_trees,
        maxDepth=max_depth,
        minInstancesPerNode=5,
        subsamplingRate=0.8,
        featureSubsetStrategy="auto",
        seed=42,
    )
    return Pipeline(stages=[assembler, regressor])


def evaluate_predictions(predictions: DataFrame, split_name: str) -> dict:
    values = {"split": split_name, "rows": predictions.count()}
    for metric_name in ("mae", "rmse", "r2"):
        evaluator = RegressionEvaluator(
            labelCol="demand",
            predictionCol="prediction",
            metricName=metric_name,
        )
        values[metric_name] = float(evaluator.evaluate(predictions))
    return values


def build_lookup(
    spark: SparkSession,
    model,
    profile: DataFrame,
    global_mean: float,
) -> DataFrame:
    """Predict all calendar combinations required by the web form."""
    zones = spark.range(1, 266).select(F.col("id").cast("double").alias("zone_id"))
    months = spark.range(1, 13).select(F.col("id").cast("double").alias("month"))
    weekdays = spark.range(1, 8).select(
        F.col("id").cast("double").alias("day_of_week")
    )
    hours = spark.range(0, 24).select(
        F.col("id").cast("double").alias("hour_of_day")
    )

    calendar = (
        zones.crossJoin(months)
        .crossJoin(weekdays)
        .crossJoin(hours)
        .withColumn(
            "is_weekend",
            F.when(F.col("day_of_week").isin(1.0, 7.0), 1.0).otherwise(0.0),
        )
    )

    calendar_with_history = attach_historical_profile(calendar, profile, global_mean)

    return (
        model.transform(calendar_with_history)
        .select(
            F.col("zone_id").cast("int").alias("zone_id"),
            F.col("month").cast("int").alias("month"),
            F.col("day_of_week").cast("int").alias("day_of_week"),
            F.col("hour_of_day").cast("int").alias("hour_of_day"),
            F.greatest(F.col("prediction"), F.lit(0.0)).alias("prediction"),
        )
    )


def create_smoke_data(spark: SparkSession) -> DataFrame:
    hours = spark.sql(
        """
        SELECT explode(sequence(
            timestamp('2024-09-20 00:00:00'),
            timestamp('2024-12-10 23:00:00'),
            interval 3 hours
        )) AS pickup_hour
        """
    )
    zones = spark.range(1, 5).select(F.col("id").cast("int").alias("zone_id"))
    return zones.crossJoin(hours).withColumn(
        "demand",
        (
            F.col("zone_id") * 3
            + F.hour("pickup_hour")
            + F.when(F.dayofweek("pickup_hour").isin(1, 7), 5).otherwise(0)
        ).cast("long"),
    )


def main():
    args = parse_args()
    spark = (
        SparkSession.builder.appName("Taxi-Demand-Random-Forest")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.hadoop.dfs.replication", "3")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print("===== READ DEMAND GRID =====")
    source = create_smoke_data(spark) if args.smoke_test else spark.read.parquet(args.input)
    data = add_calendar_features(source).cache()

    train_calendar = data.filter(
        F.col("pickup_hour") < F.lit(TRAIN_END).cast("timestamp")
    )
    validation_calendar = data.filter(
        (F.col("pickup_hour") >= F.lit(TRAIN_END).cast("timestamp"))
        & (F.col("pickup_hour") < F.lit(VALIDATION_END).cast("timestamp"))
    )
    test_calendar = data.filter(
        (F.col("pickup_hour") >= F.lit(VALIDATION_END).cast("timestamp"))
        & (F.col("pickup_hour") < F.lit(TEST_END).cast("timestamp"))
    )

    historical_profile = (
        train_calendar.groupBy(*PROFILE_COLUMNS)
        .agg(F.avg("demand").alias("historical_mean"))
        .cache()
    )
    global_mean = float(train_calendar.agg(F.avg("demand")).first()[0])

    train = attach_historical_profile(
        train_calendar, historical_profile, global_mean
    )
    validation = attach_historical_profile(
        validation_calendar, historical_profile, global_mean
    )
    test = attach_historical_profile(
        test_calendar, historical_profile, global_mean
    )

    train_rows = train.count()
    validation_rows = validation.count()
    test_rows = test.count()
    print(f"Train rows      : {train_rows:,}")
    print(f"Validation rows : {validation_rows:,}")
    print(f"Test rows       : {test_rows:,}")

    if min(train_rows, validation_rows, test_rows) == 0:
        raise ValueError("Chronological split produced an empty dataset")

    pipeline = create_pipeline(
        num_trees=3 if args.smoke_test else args.num_trees,
        max_depth=4 if args.smoke_test else args.max_depth,
    )

    print("===== TRAIN RANDOM FOREST =====")
    model = pipeline.fit(train)

    validation_predictions = model.transform(validation).cache()
    test_predictions = model.transform(test).cache()

    validation_metrics = evaluate_predictions(validation_predictions, "validation")
    test_metrics = evaluate_predictions(test_predictions, "test")

    print("===== MODEL METRICS =====")
    for metrics in (validation_metrics, test_metrics):
        print(
            f"{metrics['split']}: "
            f"MAE={metrics['mae']:.4f}, "
            f"RMSE={metrics['rmse']:.4f}, "
            f"R2={metrics['r2']:.4f}"
        )

    lookup = build_lookup(
        spark, model, historical_profile, global_mean
    ).cache()
    lookup_rows = lookup.count()
    positive_lookup = lookup.filter(F.col("prediction") > 0)
    quantiles = positive_lookup.approxQuantile("prediction", [0.50, 0.85], 0.001)
    low_max, medium_max = quantiles if len(quantiles) == 2 else (10.0, 50.0)

    if args.smoke_test:
        assert lookup_rows == 265 * 12 * 7 * 24
        print(f"Smoke lookup rows: {lookup_rows:,}")
        print("SMOKE TEST PASSED")
        spark.stop()
        return

    print("===== SAVE MODEL AND PREDICTIONS =====")
    model.write().overwrite().save(args.model)

    output_predictions = test_predictions.select(
        F.col("zone_id").cast("int").alias("zone_id"),
        "pickup_hour",
        F.col("demand").alias("actual_demand"),
        F.greatest(F.col("prediction"), F.lit(0.0)).alias("predicted_demand"),
        F.col("hour_of_day").cast("int").alias("hour_of_day"),
        F.col("day_of_week").cast("int").alias("day_of_week"),
        F.col("month").cast("int").alias("month"),
    )
    output_predictions.write.mode("overwrite").parquet(
        f"{args.output}/demand_predictions"
    )

    metrics_rows = []
    for values in (validation_metrics, test_metrics):
        metrics_rows.append(
            {
                "model": "RandomForestRegressor",
                "split": values["split"],
                "rows": int(values["rows"]),
                "mae": float(values["mae"]),
                "rmse": float(values["rmse"]),
                "r2": float(values["r2"]),
            }
        )
    spark.createDataFrame(metrics_rows).write.mode("overwrite").parquet(
        f"{args.output}/model_metrics"
    )

    daily = (
        output_predictions.withColumn("date", F.to_date("pickup_hour"))
        .groupBy("date")
        .agg(
            F.sum("actual_demand").alias("actual_demand"),
            F.sum("predicted_demand").alias("predicted_demand"),
        )
        .orderBy("date")
    )
    daily.write.mode("overwrite").parquet(f"{args.output}/prediction_daily")

    zone_summary = (
        output_predictions.withColumn(
            "absolute_error",
            F.abs(F.col("actual_demand") - F.col("predicted_demand")),
        )
        .groupBy("zone_id")
        .agg(
            F.sum("actual_demand").alias("actual_demand"),
            F.sum("predicted_demand").alias("predicted_demand"),
            F.avg("absolute_error").alias("mae"),
        )
        .orderBy(F.desc("predicted_demand"))
    )
    zone_summary.write.mode("overwrite").parquet(
        f"{args.output}/prediction_by_zone"
    )

    lookup.write.mode("overwrite").parquet(f"{args.output}/prediction_lookup")
    historical_profile.write.mode("overwrite").parquet(
        f"{args.output}/model_historical_profile"
    )

    metadata = [{
        "model": "RandomForestRegressor",
        "features": ",".join(FEATURE_COLUMNS),
        "train_period": "2023-01-01 to 2024-09-30",
        "validation_period": "2024-10-01 to 2024-10-31",
        "test_period": "2024-11-01 to 2024-12-31",
        "train_rows": train_rows,
        "validation_rows": validation_rows,
        "test_rows": test_rows,
        "lookup_rows": lookup_rows,
        "low_max": float(low_max),
        "medium_max": float(medium_max),
        "num_trees": args.num_trees,
        "max_depth": args.max_depth,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
    }]
    spark.createDataFrame(metadata).write.mode("overwrite").parquet(
        f"{args.output}/model_metadata"
    )

    print(f"Model saved       : {args.model}")
    print(f"Predictions saved : {args.output}/demand_predictions")
    print(f"Lookup rows       : {lookup_rows:,}")
    print(f"Demand thresholds : LOW < {low_max:.2f}, HIGH >= {medium_max:.2f}")
    print("===== TRAIN/PREDICT DONE =====")

    spark.stop()


if __name__ == "__main__":
    main()

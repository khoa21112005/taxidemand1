import os
import subprocess
import pendulum

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator


PROJECT = "/home/khoa2/taxi-demand"
HADOOP_HOME = "/home/khoa2/hadoop"
HDFS = "/home/khoa2/hadoop/bin/hdfs"
JAVA_HOME = "/usr/lib/jvm/java-17-openjdk-amd64"

PYSPARK_ENV = "/home/khoa2/pyspark-env"
SPARK = "/home/khoa2/pyspark-env/bin/spark-submit"


def get_env():
    env = os.environ.copy()

    env["JAVA_HOME"] = JAVA_HOME
    env["HADOOP_HOME"] = HADOOP_HOME
    env["PYSPARK_PYTHON"] = f"{PYSPARK_ENV}/bin/python"
    env["PYSPARK_DRIVER_PYTHON"] = f"{PYSPARK_ENV}/bin/python"

    env["PATH"] = (
        f"{PYSPARK_ENV}/bin:"
        f"{HADOOP_HOME}/bin:"
        + env.get("PATH", "")
    )

    return env


def check_hdfs_func():
    env = get_env()

    paths = [
        "hdfs://localhost:9000/taxi-demand/raw/2023",
        "hdfs://localhost:9000/taxi-demand/raw/2024"
    ]

    for path in paths:
        subprocess.run(
            [HDFS, "dfs", "-test", "-e", path],
            check=True,
            env=env
        )

        print(f"OK: {path}")

    print("HDFS RAW DATA OK")


def run_spark_func(script, spark_options=None):
    env = get_env()

    script_path = os.path.join(
        PROJECT,
        script
    )

    if not os.path.exists(script_path):
        raise FileNotFoundError(
            f"Khong tim thay: {script_path}"
        )

    print(f"RUNNING: {script_path}")

    command = [SPARK, *(spark_options or []), script_path]

    subprocess.run(
        command,
        cwd=PROJECT,
        check=True,
        env=env
    )

    print(f"DONE: {script}")


def check_output_func():
    env = get_env()

    hdfs_paths = [
        "hdfs://localhost:9000/taxi-demand/processed/demand_hourly",
        "hdfs://localhost:9000/taxi-demand/output/hourly_demand",
        "hdfs://localhost:9000/taxi-demand/output/demand_predictions",
        "hdfs://localhost:9000/taxi-demand/output/model_metrics",
        "hdfs://localhost:9000/taxi-demand/output/prediction_lookup",
        "hdfs://localhost:9000/taxi-demand/models/random_forest_demand",
    ]

    for path in hdfs_paths:
        subprocess.run(
            [HDFS, "dfs", "-test", "-e", path],
            check=True,
            env=env
        )

        print(f"OUTPUT OK: {path}")

    dashboard_outputs = [
        os.path.join(PROJECT, "dashboard", "data", "kpi.csv"),
        os.path.join(PROJECT, "dashboard", "data", "model_metrics.csv"),
        os.path.join(PROJECT, "dashboard", "data", "model_metadata.csv"),
        os.path.join(PROJECT, "dashboard", "data", "prediction_daily.csv"),
        os.path.join(PROJECT, "dashboard", "data", "prediction_lookup"),
    ]

    for output in dashboard_outputs:
        if not os.path.exists(output):
            raise FileNotFoundError(f"Khong tim thay: {output}")
        print(f"DASHBOARD OUTPUT OK: {output}")

    print("PIPELINE OUTPUT OK")


with DAG(
    dag_id="taxi_bigdata_pipeline",
    description="NYC Taxi Big Data Pipeline",
    start_date=pendulum.datetime(
        2026,
        1,
        1,
        tz="Asia/Ho_Chi_Minh"
    ),
    schedule=None,
    catchup=False,
    tags=["bigdata", "hdfs", "spark", "taxi"],
) as dag:

    start = EmptyOperator(
        task_id="start"
    )

    check_hdfs = PythonOperator(
        task_id="check_hdfs",
        python_callable=check_hdfs_func
    )

    clean_data = PythonOperator(
        task_id="clean_data",
        python_callable=run_spark_func,
        op_kwargs={
            "script": "src/02_clean_hdfs.py"
        }
    )

    spark_sql = PythonOperator(
        task_id="spark_sql",
        python_callable=run_spark_func,
        op_kwargs={
            "script": "src/03_spark_sql.py"
        }
    )

    demand_grid = PythonOperator(
        task_id="demand_grid",
        python_callable=run_spark_func,
        op_kwargs={
            "script": "src/04_demand_grid.py"
        }
    )

    save_results = PythonOperator(
        task_id="save_results",
        python_callable=run_spark_func,
        op_kwargs={
            "script": "src/05_save_results.py"
        }
    )

    train_predict = PythonOperator(
        task_id="train_predict",
        python_callable=run_spark_func,
        op_kwargs={
            "script": "src/06_train.py",
            "spark_options": ["--driver-memory", "2g"]
        }
    )

    export_dashboard = PythonOperator(
        task_id="export_dashboard",
        python_callable=run_spark_func,
        op_kwargs={
            "script": "src/08_export_dashboard.py"
        }
    )

    check_output = PythonOperator(
        task_id="check_output",
        python_callable=check_output_func
    )

    end = EmptyOperator(
        task_id="end"
    )

    (
        start
        >> check_hdfs
        >> clean_data
        >> spark_sql
        >> demand_grid
        >> save_results
        >> train_predict
        >> export_dashboard
        >> check_output
        >> end
    )

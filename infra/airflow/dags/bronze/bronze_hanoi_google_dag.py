"""
DAG: bronze_hanoi_google
Mô tả: Quy trình hợp nhất cho nguồn Google (Python Raw Ingestion + Spark Iceberg Ingestion)
Lịch chạy: Hàng ngày lúc 02:00 ICT
"""
import os
import sys
import json
import logging
import asyncio
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
# from airflow.utils.dates import days_ago

from _spark_common import (
    iceberg_dataset,
    iceberg_maintenance,
    spark_base_conf,
    spark_env_vars,
    spark_job_base,
    spark_packages,
    spark_utils_py_files,
)

logger = logging.getLogger(__name__)

# ─── Constants ────────────────────────────────────────────────────────────────
BRONZE_TABLE = "iceberg.bronze.hanoi_attractions"
PACKAGES     = spark_packages()
BASE_CONF    = spark_base_conf()
ENV_VARS     = spark_env_vars()
SPARK_JOB_BASE = spark_job_base()
SPARK_PY_FILES = spark_utils_py_files()

BRONZE_SPARK_APP = os.path.join(SPARK_JOB_BASE, "bronze", "bronze_hanoi_places.py")

default_args = {
    "owner": "tourism-team",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ─── Python Task Functions (Raw Ingestion) ────────────────────────────────────

def validate_env(**context):
    api_key = os.environ.get("GOOGLE_PLACES_API_KEY", "")
    if not api_key:
        raise ValueError("GOOGLE_PLACES_API_KEY is not set")
    logger.info("Environment validation passed for Google")
    return True

# Ingestion scripts
INGESTION_GOOGLE  = os.path.join(spark_job_base(), "ingestion", "fetch_hanoi_google.py")
INGESTION_WEATHER = os.path.join(spark_job_base(), "ingestion", "fetch_hanoi_weather.py")

# ─── DAG ──────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="bronze_hanoi_google",
    description="2-Stage Ingestion: API -> Raw JSON (Landing) -> Iceberg (Bronze)",
    schedule="0 19 * * *",  # 02:00 ICT
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=["bronze", "google", "hanoi", "iceberg", "landing"],
) as dag:

    start = EmptyOperator(task_id="start")

    t_validate = PythonOperator(
        task_id="validate_env",
        python_callable=validate_env,
    )

    t_fetch_weather = SparkSubmitOperator(
        task_id="fetch_weather",
        conn_id="spark_default",
        application=INGESTION_WEATHER,
        env_vars=ENV_VARS,
        conf=BASE_CONF,
        verbose=True,
    )

    t_fetch_google_raw = SparkSubmitOperator(
        task_id="fetch_google_places_raw",
        conn_id="spark_default",
        application=INGESTION_GOOGLE,
        env_vars=ENV_VARS,
        conf=BASE_CONF,
        verbose=True,
    )

    # Ingest from Raw JSON files using Spark
    t_ingest_spark = SparkSubmitOperator(
        task_id="ingest_to_iceberg_spark",
        conn_id="spark_default",
        application=BRONZE_SPARK_APP,
        py_files=SPARK_PY_FILES,
        packages=PACKAGES,
        env_vars=ENV_VARS,
        conf=BASE_CONF,
        application_args=[
            "--table", BRONZE_TABLE,
            "--input-path", "s3a://tourism-bronze/source=google_places/date={{ ds }}/*.json"
        ],
        verbose=True,
        outlets=[iceberg_dataset(BRONZE_TABLE)],
    )

    t_maintenance = PythonOperator(
        task_id="iceberg_maintenance",
        python_callable=iceberg_maintenance,
        op_kwargs={"table": BRONZE_TABLE, "expire_days": "7d"},
    )

    end = EmptyOperator(task_id="end")

    start >> t_validate
    t_validate >> [t_fetch_weather, t_fetch_google_raw]
    [t_fetch_weather, t_fetch_google_raw] >> t_ingest_spark >> t_maintenance >> end

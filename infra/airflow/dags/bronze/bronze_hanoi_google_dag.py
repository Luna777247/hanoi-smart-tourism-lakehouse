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
from airflow.utils.dates import days_ago

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

def fetch_weather(**context):
    """Thu thập dữ liệu thời tiết Hà Nội bổ sung."""
    import httpx
    api_key = os.environ.get("OPENWEATHER_API_KEY", "")
    if not api_key:
        return {}
    url = "https://api.openweathermap.org/data/2.5/weather"
    params = {"q": "Hanoi,VN", "appid": api_key, "units": "metric"}
    resp = httpx.get(url, params=params, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    weather = {
        "temperature_celsius": data["main"]["temp"],
        "weather_condition": data["weather"][0]["main"],
        "snapshot_time": datetime.utcnow().isoformat(),
    }
    context["ti"].xcom_push(key="weather_snapshot", value=weather)
    return weather

def load_raw_to_minio(**context):
    """Lưu dữ liệu thời tiết vào bucket raw (JSON)."""
    from io import BytesIO
    from minio import Minio
    ti = context["ti"]
    weather = ti.xcom_pull(key="weather_snapshot", task_ids="fetch_weather")
    
    minio_client = Minio(
        os.environ["MINIO_ENDPOINT"],
        access_key=os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
        secret_key=os.environ.get("MINIO_SECRET_KEY", "minioadmin123"),
        secure=False,
    )
    date_str = datetime.utcnow().strftime("%Y-%m-%d")
    ts = datetime.utcnow().strftime("%H%M%S")

    if weather:
        weather_json = json.dumps([weather], ensure_ascii=False).encode("utf-8")
        minio_client.put_object(
            bucket_name="tourism-bronze",
            object_name=f"source=openweather/date={date_str}/weather_{ts}.json",
            data=BytesIO(weather_json),
            length=len(weather_json),
            content_type="application/json",
        )
    return {"status": "success"}

def fetch_google_places_raw(**context):
    """Giai đoạn 1: Gọi Google Places API và lưu JSON thô vào MinIO."""
    from io import BytesIO
    from minio import Minio
    # backend/ dùng import dạng `app.*` (giống FastAPI); mount repo là /workspace
    backend_root = os.path.join(
        os.environ.get("WORKSPACE_ROOT", "/workspace"), "apps", "backend"
    )
    if backend_root not in sys.path:
        sys.path.insert(0, backend_root)

    from app.services.google_places_service import GooglePlacesService
    
    api_key = os.environ["GOOGLE_PLACES_API_KEY"]
    svc = GooglePlacesService(api_key=api_key)
    
    # Giả sử chúng ta thu thập mẫu 20 địa điểm mỗi loại để tránh tốn quota trong demo
    attractions = asyncio.run(svc.fetch_all_hanoi_attractions(limit_per_type=20))
    logger.info(f"Fetched {len(attractions)} attractions from Google Places API.")

    minio_client = Minio(
        os.environ["MINIO_ENDPOINT"],
        access_key=os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
        secret_key=os.environ.get("MINIO_SECRET_KEY", "minioadmin123"),
        secure=False,
    )
    date_str = datetime.utcnow().strftime("%Y-%m-%d")
    ts = datetime.utcnow().strftime("%H%M%S")

    # Upload JSON thô làm Landing Zone
    places_json = json.dumps(attractions, ensure_ascii=False).encode("utf-8")
    minio_client.put_object(
        bucket_name="tourism-bronze",
        object_name=f"source=google_places/date={date_str}/places_{ts}.json",
        data=BytesIO(places_json),
        length=len(places_json),
        content_type="application/json",
    )
    context["ti"].xcom_push(key="raw_path", value=f"source=google_places/date={date_str}/")
    return len(attractions)

# ─── DAG ──────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="bronze_hanoi_google",
    description="2-Stage Ingestion: API -> Raw JSON (Landing) -> Iceberg (Bronze)",
    schedule_interval="0 19 * * *",  # 02:00 ICT
    start_date=days_ago(1),
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

    t_fetch_weather = PythonOperator(
        task_id="fetch_weather",
        python_callable=fetch_weather,
    )

    t_fetch_google_raw = PythonOperator(
        task_id="fetch_google_places_raw",
        python_callable=fetch_google_places_raw,
    )

    t_load_weather_raw = PythonOperator(
        task_id="load_weather_raw_json",
        python_callable=load_raw_to_minio,
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
    t_fetch_weather >> t_load_weather_raw
    [t_load_weather_raw, t_fetch_google_raw] >> t_ingest_spark >> t_maintenance >> end

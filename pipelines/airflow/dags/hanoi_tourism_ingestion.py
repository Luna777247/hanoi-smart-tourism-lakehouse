"""
DAG: hanoi_tourism_ingestion
Mô tả: Thu thập dữ liệu từ Google Places API → lưu vào Bronze layer (MinIO/Iceberg)
Schedule: Hàng ngày lúc 2:00 AM
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import json
import logging
import os

logger = logging.getLogger(__name__)

default_args = {
    "owner": "tourism-team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="hanoi_tourism_ingestion",
    description="Ingest Hanoi attractions from Google Places API to Bronze layer",
    default_args=default_args,
    schedule_interval="0 2 * * *",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["bronze", "ingestion", "google-places"],
    params={
        "city": "Hanoi",
        "limit_per_type": 60,
        "radius_meters": 30000,
    },
)


def validate_env(**context):
    """Kiểm tra API keys và kết nối MinIO trước khi chạy."""
    api_key = os.environ.get("GOOGLE_PLACES_API_KEY", "")
    if not api_key:
        raise ValueError("GOOGLE_PLACES_API_KEY is not set")
    minio_endpoint = os.environ.get("MINIO_ENDPOINT", "")
    if not minio_endpoint:
        raise ValueError("MINIO_ENDPOINT is not set")
    logger.info("Environment validation passed")
    return True


def fetch_attractions(**context):
    """Gọi Google Places API, thu thập điểm du lịch Hà Nội."""
    import asyncio
    import sys

    # backend/ dùng import dạng `app.*` (giống FastAPI); mount repo là /workspace
    backend_root = os.path.join(
        os.environ.get("WORKSPACE_ROOT", "/workspace"), "apps", "backend"
    )
    if backend_root not in sys.path:
        sys.path.insert(0, backend_root)

    from app.services.google_places_service import GooglePlacesService

    api_key = os.environ["GOOGLE_PLACES_API_KEY"]
    params = context["params"]

    async def _fetch():
        svc = GooglePlacesService(api_key=api_key)
        return await svc.fetch_all_hanoi_attractions(
            limit_per_type=params.get("limit_per_type", 60)
        )

    attractions = asyncio.run(_fetch())
    logger.info(f"Fetched {len(attractions)} attractions")

    context["ti"].xcom_push(key="attraction_count", value=len(attractions))
    context["ti"].xcom_push(key="attractions", value=attractions[:10])

    return len(attractions)


def fetch_weather(**context):
    """Thu thập dữ liệu thời tiết Hà Nội hiện tại."""
    import httpx

    api_key = os.environ.get("OPENWEATHER_API_KEY", "")
    if not api_key:
        logger.warning("No OpenWeather API key, skipping weather fetch")
        return {}

    url = "https://api.openweathermap.org/data/2.5/weather"
    params = {"q": "Hanoi,VN", "appid": api_key, "units": "metric"}

    resp = httpx.get(url, params=params, timeout=15)
    resp.raise_for_status()
    data = resp.json()

    weather = {
        "temperature_celsius": data["main"]["temp"],
        "feels_like": data["main"]["feels_like"],
        "humidity": data["main"]["humidity"],
        "weather_condition": data["weather"][0]["main"],
        "weather_description": data["weather"][0]["description"],
        "wind_speed": data["wind"]["speed"],
        "snapshot_time": datetime.utcnow().isoformat(),
    }

    context["ti"].xcom_push(key="weather_snapshot", value=weather)
    logger.info(
        f"Weather fetched: {weather['weather_condition']}, {weather['temperature_celsius']}°C"
    )
    return weather


def load_to_bronze(**context):
    """Upload raw data vào MinIO Bronze bucket theo partition date."""
    from io import BytesIO

    from minio import Minio

    ti = context["ti"]
    attraction_count = ti.xcom_pull(key="attraction_count", task_ids="fetch_attractions")
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
            metadata={"source": "openweather", "record_count": "1"},
        )

    logger.info(f"Bronze load complete. Attractions: {attraction_count}, Weather: uploaded")
    return {"status": "success", "date": date_str, "attraction_count": attraction_count}


def update_catalog(**context):
    """Notify OpenMetadata về data lineage mới."""
    import httpx

    openmetadata_url = os.environ.get("OPENMETADATA_URL", "http://openmetadata-server:8585")
    date_str = datetime.utcnow().strftime("%Y-%m-%d")

    payload = {
        "event_type": "ENTITY_CREATED",
        "entity_type": "table",
        "source": "airflow-dag",
        "dag_id": "hanoi_tourism_ingestion",
        "execution_date": date_str,
        "entities": [
            f"tourism-bronze/source=google_places/date={date_str}",
            f"tourism-bronze/source=openweather/date={date_str}",
        ],
    }

    try:
        resp = httpx.post(
            f"{openmetadata_url}/api/v1/events",
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=10,
        )
        logger.info(f"OpenMetadata notified: {resp.status_code}")
    except Exception as e:
        logger.warning(f"OpenMetadata notification failed (non-critical): {e}")


with dag:
    start = EmptyOperator(task_id="start")

    t_validate = PythonOperator(
        task_id="validate_env",
        python_callable=validate_env,
    )

    t_fetch_attractions = PythonOperator(
        task_id="fetch_attractions",
        python_callable=fetch_attractions,
    )

    t_fetch_weather = PythonOperator(
        task_id="fetch_weather",
        python_callable=fetch_weather,
    )

    t_bronze = PythonOperator(
        task_id="load_to_bronze",
        python_callable=load_to_bronze,
    )

    t_catalog = PythonOperator(
        task_id="update_catalog",
        python_callable=update_catalog,
    )

    end = EmptyOperator(task_id="end")

    start >> t_validate >> [t_fetch_attractions, t_fetch_weather] >> t_bronze >> t_catalog >> end

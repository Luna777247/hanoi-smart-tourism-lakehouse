#!/usr/bin/env python3
import os
import json
import logging
import httpx
from datetime import datetime, timezone
from io import BytesIO
from minio import Minio

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

def run_weather_ingestion():
    """Thu thập dữ liệu thô (raw) từ OpenWeather API và lưu vào MinIO."""
    api_key = os.environ.get("OPENWEATHER_API_KEY", "")
    if not api_key:
        logger.warning("OPENWEATHER_API_KEY not found. Skipping weather fetch.")
        return 0
        
    url = "https://api.openweathermap.org/data/2.5/weather"
    params = {"q": "Hanoi,VN", "appid": api_key, "units": "metric"}
    
    logger.info("Fetching raw weather data from OpenWeather...")
    try:
        resp = httpx.get(url, params=params, timeout=15)
        resp.raise_for_status()
        raw_data = resp.json() # Trả về bản gốc từ OpenWeather
    except Exception as e:
        logger.error(f"Error fetching weather: {e}")
        return 0

    minio_client = Minio(
        os.environ["MINIO_ENDPOINT"].replace("http://", "").replace("https://", ""),
        access_key=os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
        secret_key=os.environ.get("MINIO_SECRET_KEY", "minioadmin123"),
        secure=False,
    )
    
    bucket_name = "tourism-bronze"
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    ts = datetime.now(timezone.utc).strftime("%H%M%S")

    # Lưu nguyên xi response (bao gồm coord, weather, main, wind, clouds, sys, name, cod, etc.)
    json_data = json.dumps(raw_data, ensure_ascii=False).encode("utf-8")
    object_name = f"source=openweather/date={date_str}/raw_weather_{ts}.json"
    
    minio_client.put_object(
        bucket_name=bucket_name,
        object_name=object_name,
        data=BytesIO(json_data),
        length=len(json_data),
        content_type="application/json",
    )
    logger.info(f"Successfully saved RAW weather data to s3://{bucket_name}/{object_name}")
    return 1

if __name__ == "__main__":
    run_weather_ingestion()

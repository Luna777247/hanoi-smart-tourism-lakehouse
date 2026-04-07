#!/usr/bin/env python3
import os
import sys
import json
import logging
import asyncio
import httpx
from datetime import datetime, timezone
from io import BytesIO
from minio import Minio

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

OVERPASS_URL = "http://overpass-api.de/api/interpreter"

async def fetch_osm_raw_data():
    """Lấy dữ liệu thô (raw) trực tiếp từ Overpass API."""
    query = """
    [out:json][timeout:60];
    area["name"="Thành phố Hà Nội"]->.a;
    (
      node["tourism"="attraction"](area.a);
      way["tourism"="attraction"](area.a);
      node["historic"](area.a);
      way["historic"](area.a);
    );
    out body;
    >;
    out skel qt;
    """
    
    async with httpx.AsyncClient(timeout=60.0) as client:
        try:
            response = await client.post(OVERPASS_URL, data={"data": query})
            response.raise_for_status()
            return response.json() # Trả về toàn bộ OSM JSON envelope
        except Exception as e:
            logger.error(f"Error fetching from Overpass API: {e}")
            return None

async def run_ingestion():
    """Giai đoạn 1 (Landing Zone): Lưu nguyên xi JSON thô từ OSM vào MinIO."""
    raw_data = await fetch_osm_raw_data()
    if not raw_data:
        sys.exit(1)
        
    minio_client = Minio(
        os.environ["MINIO_ENDPOINT"].replace("http://", "").replace("https://", ""),
        access_key=os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
        secret_key=os.environ.get("MINIO_SECRET_KEY", "minioadmin123"),
        secure=False,
    )
    
    bucket_name = "tourism-bronze"
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    ts = datetime.now(timezone.utc).strftime("%H%M%S")

    # Lưu nguyên kết quả OSM (bao gồm cả generator, osm3s và elements)
    json_data = json.dumps(raw_data, ensure_ascii=False).encode("utf-8")
    object_name = f"source=osm/date={date_str}/raw_osm_{ts}.json"
    
    minio_client.put_object(
        bucket_name=bucket_name,
        object_name=object_name,
        data=BytesIO(json_data),
        length=len(json_data),
        content_type="application/json",
    )
    logger.info(f"Successfully saved TRUE RAW OSM data to s3://{bucket_name}/{object_name}")
    return 1

if __name__ == "__main__":
    asyncio.run(run_ingestion())

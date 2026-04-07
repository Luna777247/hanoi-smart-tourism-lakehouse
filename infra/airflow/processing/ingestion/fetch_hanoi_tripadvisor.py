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

SERPAPI_URL = "https://serpapi.com/search"

async def fetch_tripadvisor_serpapi(api_key: str, query: str):
    """Lấy dữ liệu thô (raw) từ SerpApi engine 'tripadvisor'."""
    params = {
        "engine": "tripadvisor",
        "q": query,
        "ssrc": "A",
        "hl": "vi",
        "api_key": api_key
    }
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            response = await client.get(SERPAPI_URL, params=params)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error fetching from SerpApi (tripadvisor) for '{query}': {e}")
            return None

async def run_ingestion():
    """Giai đoạn 1 (Landing Zone): Lưu nguyên xi JSON thô từ SerpApi vào MinIO."""
    api_key = os.environ.get("SERPAPI_KEY")
    if not api_key:
        logger.error("SERPAPI_KEY environment variable not found.")
        sys.exit(1)
        
    queries = ["Hanoi attractions", "Things to do in Hanoi"]
    all_raw_responses = []
    
    logger.info(f"Starting TRUE RAW Tripadvisor fetch for {len(queries)} queries...")
    for q in queries:
        raw_data = await fetch_tripadvisor_serpapi(api_key, q)
        if raw_data:
            all_raw_responses.append(raw_data)
    
    minio_client = Minio(
        os.environ["MINIO_ENDPOINT"].replace("http://", "").replace("https://", ""),
        access_key=os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
        secret_key=os.environ.get("MINIO_SECRET_KEY", "minioadmin123"),
        secure=False,
    )
    
    bucket_name = "tourism-bronze"
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    ts = datetime.now(timezone.utc).strftime("%H%M%S")

    # Lưu nguyên envelope (metadata + results) cho Landing Zone
    json_data = json.dumps(all_raw_responses, ensure_ascii=False).encode("utf-8")
    object_name = f"source=tripadvisor/date={date_str}/raw_responses_{ts}.json"
    
    minio_client.put_object(
        bucket_name=bucket_name,
        object_name=object_name,
        data=BytesIO(json_data),
        length=len(json_data),
        content_type="application/json",
    )
    logger.info(f"Successfully saved {len(all_raw_responses)} RAW responses to s3://{bucket_name}/{object_name}")
    return len(all_raw_responses)

if __name__ == "__main__":
    asyncio.run(run_ingestion())

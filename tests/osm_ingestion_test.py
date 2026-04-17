#!/usr/bin/env python3
import os
import sys
import json
import logging
import asyncio
import httpx

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

OVERPASS_URL = "http://overpass-api.de/api/interpreter"

async def fetch_osm_raw_data():
    """Lấy dữ liệu thô (raw) trực tiếp từ Overpass API."""
    query = """
    [out:json][timeout:25];
    node(21.0278,105.8342,21.0288,105.8352);
    out;
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
    """Giai đoạn 1 (Bronze Zone): Lưu nguyên xi JSON thô từ OSM vào MinIO."""
    raw_data = await fetch_osm_raw_data()
    if not raw_data:
        sys.exit(1)

    # For now, just print the data instead of saving to MinIO
    logger.info(f"Fetched {len(raw_data.get('elements', []))} elements from OSM")
    logger.info("Sample data (first 3 elements):")
    for i, element in enumerate(raw_data.get('elements', [])[:3]):
        logger.info(f"Element {i}: {json.dumps(element, indent=2, ensure_ascii=False)}")

    logger.info("OSM data collection successful!")
    return 1

if __name__ == "__main__":
    asyncio.run(run_ingestion())
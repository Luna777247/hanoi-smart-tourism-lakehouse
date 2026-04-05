#!/usr/bin/env python3
"""Bronze ingestion: Overpass API + Nominatim → Iceberg bronze.hanoi_osm.

Follows the 'insert + update by ID' requirement.
- OSM ID is the unique identifier.
- Uses Overpass API to fetch list of attractions.
- (Optional) Fetches metadata from Nominatim.
- Performs MERGE INTO to handle upserts.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import asyncio
from datetime import datetime
from typing import List, Dict, Any

from pyspark.sql import SparkSession, DataFrame, types as T, functions as F

# Shared helpers
from spark_utils import build_spark, ensure_iceberg_table, ensure_schema

APP_NAME = "bronze_hanoi_osm"
logger = logging.getLogger(__name__)

# ─── Iceberg table DDL ────────────────────────────────────────────────────────
BRONZE_TABLE = "iceberg.bronze.hanoi_osm"
BRONZE_COLUMNS_SQL = """
    osm_id            STRING   COMMENT 'OpenStreetMap ID',
    osm_type          STRING   COMMENT 'node / way / relation',
    name              STRING   COMMENT 'Tên địa điểm (OSM name)',
    name_en           STRING   COMMENT 'Tên tiếng Anh (OSM name:en)',
    lat               DOUBLE   COMMENT 'Vĩ độ',
    lon               DOUBLE   COMMENT 'Kinh độ',
    tourism           STRING   COMMENT 'Giá trị tag tourism (attraction, museum...)',
    historic          STRING   COMMENT 'Giá trị tag historic',
    amenity           STRING   COMMENT 'Giá trị tag amenity',
    tags_json         STRING   COMMENT 'Toàn bộ tags dạng JSON string',
    address_json      STRING   COMMENT 'Địa chỉ chi tiết từ Nominatim (JSON)',
    display_name      STRING   COMMENT 'Tên hiển thị đầy đủ từ Nominatim',
    collected_at      TIMESTAMP COMMENT 'Thời điểm thu thập',
    ingestion_date    DATE     COMMENT 'Partition key'
"""

# ─── Spark Schema ─────────────────────────────────────────────────────────────
SPARK_SCHEMA = T.StructType([
    T.StructField("osm_id",         T.StringType()),
    T.StructField("osm_type",       T.StringType()),
    T.StructField("name",           T.StringType()),
    T.StructField("name_en",        T.StringType()),
    T.StructField("lat",            T.DoubleType()),
    T.StructField("lon",            T.DoubleType()),
    T.StructField("tourism",        T.StringType()),
    T.StructField("historic",       T.StringType()),
    T.StructField("amenity",        T.StringType()),
    T.StructField("tags_json",      T.StringType()),
    T.StructField("address_json",   T.StringType()),
    T.StructField("display_name",   T.StringType()),
    T.StructField("collected_at",   T.TimestampType()),
    T.StructField("ingestion_date", T.DateType()),
])

async def fetch_data() -> List[Dict]:
    """Fetch from Overpass and Nominatim on the Spark driver."""
    # backend root path for imports
    import sys
    workspace_root = os.environ.get("WORKSPACE_ROOT", "/workspace")
    backend_root = os.path.join(workspace_root, "apps", "backend")
    if backend_root not in sys.path:
        sys.path.insert(0, backend_root)
        
    from app.services.osm_service import OSMService
    
    svc = OSMService()
    logger.info("Fetching attractions from Overpass API...")
    attractions = await svc.fetch_hanoi_attractions_from_overpass()
    logger.info(f"Retrieved {len(attractions)} elements from Overpass.")
    
    # Nominatim detail fetching (limited to top 20 new/important for demo to avoid rate limit)
    # In production, we'd only fetch for IDs we haven't seen or that need refresh.
    detailed_results = []
    ingested_at = datetime.utcnow()
    
    # We take a sample to avoid hitting Nominatim too hard during every run
    # REAL WORLD: You would check your internal DB for missing addresses first.
    sample_limit = 10 
    for i, attr in enumerate(attractions):
        osm_id = attr["osm_id"]
        osm_type = attr["osm_type"]
        
        address_json = "{}"
        display_name = ""
        
        if i < sample_limit:
            logger.info(f"[{i+1}/{sample_limit}] Fetching Nominatim details for {osm_type}/{osm_id}...")
            details = await svc.get_details_from_nominatim(osm_id, osm_type)
            address_json = json.dumps(details.get("address", {}), ensure_ascii=False)
            display_name = details.get("display_name", "")
            
        detailed_results.append({
            "osm_id":         osm_id,
            "osm_type":       osm_type,
            "name":           attr.get("name"),
            "name_en":        attr.get("name_en"),
            "lat":            float(attr.get("lat")) if attr.get("lat") else None,
            "lon":            float(attr.get("lon")) if attr.get("lon") else None,
            "tourism":        attr.get("tourism"),
            "historic":       attr.get("historic"),
            "amenity":        attr.get("amenity"),
            "tags_json":      json.dumps(attr.get("tags", {}), ensure_ascii=False),
            "address_json":   address_json,
            "display_name":   display_name,
            "collected_at":   ingested_at,
            "ingestion_date": ingested_at.date()
        })
        
    return detailed_results

def ingest(spark: SparkSession, input_path: str, table: str = BRONZE_TABLE) -> int:
    """Read from Landing Zone (MinIO) and write to Bronze Iceberg table."""
    
    # 1. Read JSON from MinIO
    logger.info(f"READ_START | path={input_path}")
    df_raw = spark.read.option("multiline", "true").json(input_path)
    
    if df_raw.count() == 0:
        logger.warning("No records found to ingest.")
        return 0
        
    # 2. Process for Iceberg
    # Fields: osm_id, osm_type, name, name_en, lat, lon, tourism, historic, amenity, tags_json, address_json, display_name, collected_at, ingestion_date
    df_processed = df_raw.select(
        F.col("osm_id"),
        F.col("osm_type"),
        F.col("name"),
        F.col("name_en"),
        F.col("lat").cast("double"),
        F.col("lon").cast("double"),
        F.col("tourism"),
        F.col("historic"),
        F.col("amenity"),
        F.col("tags_json"),
        F.col("address_json"),
        F.col("display_name"),
        F.to_timestamp(F.col("collected_at")).alias("collected_at"),
        F.to_date(F.col("collected_at")).alias("ingestion_date")
    )
    df_raw.createOrReplaceTempView("stg_osm")
    
    # 3. Ensure table exists
    ensure_schema(spark, "iceberg.bronze")
    ensure_iceberg_table(
        spark, 
        table, 
        BRONZE_COLUMNS_SQL, 
        partition_field="ingestion_date"
    )
    
    # 4. MERGE INTO
    merge_sql = f"""
        MERGE INTO {table} t
        USING stg_osm s
        ON t.osm_id = s.osm_id
        WHEN MATCHED THEN
          UPDATE SET 
            t.name = s.name,
            t.name_en = s.name_en,
            t.lat = s.lat,
            t.lon = s.lon,
            t.tourism = s.tourism,
            t.historic = s.historic,
            t.amenity = s.amenity,
            t.tags_json = s.tags_json,
            t.address_json = s.address_json,
            t.display_name = s.display_name,
            t.collected_at = s.collected_at,
            t.ingestion_date = s.ingestion_date
        WHEN NOT MATCHED THEN
          INSERT *
    """
    
    logger.info(f"Executing MERGE INTO for {table}...")
    spark.sql(merge_sql)
    
    count = df_processed.count()
    logger.info(f"Ingestion complete. Processed {count} records.")
    return count

def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", default=BRONZE_TABLE)
    parser.add_argument("--input-path", required=True)
    args = parser.parse_args()
    
    spark = build_spark(APP_NAME)
    try:
        count = ingest(spark, input_path=args.input_path, table=args.table)
        logger.info(f"JOB SUCCESS | records={count}")
    except Exception as e:
        logger.error(f"JOB FAILED | error={e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()

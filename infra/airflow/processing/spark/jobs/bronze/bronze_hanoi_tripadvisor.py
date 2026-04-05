#!/usr/bin/env python3
"""Bronze ingestion: Tripadvisor API (SerpApi) → Iceberg bronze.hanoi_tripadvisor.

Follows the 'insert + update by ID' requirement.
- Sử dụng place_id từ SerpApi làm định danh duy nhất.
- Thực hiện MERGE INTO để cập nhật dữ liệu nếu đã tồn tại.
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

APP_NAME = "bronze_hanoi_tripadvisor"
logger = logging.getLogger(__name__)

# ─── Iceberg table DDL ────────────────────────────────────────────────────────
BRONZE_TABLE = "iceberg.bronze.hanoi_tripadvisor"
BRONZE_COLUMNS_SQL = """
    place_id          STRING   COMMENT 'Stable unique ID from Tripadvisor/SerpApi',
    title             STRING   COMMENT 'Tên địa điểm',
    rating            DOUBLE   COMMENT 'Rating trung bình',
    reviews_count     INT      COMMENT 'Tổng số lượt review',
    location_string   STRING   COMMENT 'Địa chỉ mô tả',
    tripadvisor_url   STRING   COMMENT 'Link đến Tripadvisor',
    thumbnail         STRING   COMMENT 'Ảnh đại diện',
    description       STRING   COMMENT 'Mô tả ngắn',
    source            STRING   COMMENT 'Nguồn dữ liệu',
    query_used        STRING   COMMENT 'Câu truy vấn sử dụng',
    collected_at      TIMESTAMP COMMENT 'Thời điểm thu thập',
    ingestion_date    DATE     COMMENT 'Partition key'
"""

# ─── Spark Schema ─────────────────────────────────────────────────────────────
SPARK_SCHEMA = T.StructType([
    T.StructField("place_id",        T.StringType()),
    T.StructField("title",           T.StringType()),
    T.StructField("rating",          T.DoubleType()),
    T.StructField("reviews_count",   T.IntegerType()),
    T.StructField("location_string", T.StringType()),
    T.StructField("tripadvisor_url", T.StringType()),
    T.StructField("thumbnail",       T.StringType()),
    T.StructField("description",     T.StringType()),
    T.StructField("source",          T.StringType()),
    T.StructField("query_used",      T.StringType()),
    T.StructField("collected_at",    T.TimestampType()),
    T.StructField("ingestion_date",  T.DateType()),
])

async def fetch_data(api_key: str, queries: List[str]) -> List[Dict]:
    """Fetch from Tripadvisor via SerpApi on the Spark driver."""
    import sys
    workspace_root = os.environ.get("WORKSPACE_ROOT", "/workspace")
    backend_root = os.path.join(workspace_root, "apps", "backend")
    if backend_root not in sys.path:
        sys.path.insert(0, backend_root)
        
    from app.services.tripadvisor_service import TripadvisorService
    
    svc = TripadvisorService(api_key=api_key)
    logger.info(f"Fetching Tripadvisor data for {len(queries)} queries...")
    results = await svc.fetch_all_hanoi_attractions(queries=queries)
    
    # Process for Spark (convert ISO string to datetime object)
    for r in results:
        r["collected_at"] = datetime.fromisoformat(r["collected_at"])
        r["ingestion_date"] = r["collected_at"].date()
        # Convert reviews_count parsing if needed (handle strings like "1,234")
        if isinstance(r["reviews_count"], str):
            try:
                r["reviews_count"] = int(r["reviews_count"].replace(",", ""))
            except:
                 r["reviews_count"] = 0
                 
    return results

def ingest(spark: SparkSession, input_path: str, table: str = BRONZE_TABLE) -> int:
    """Read from Landing Zone and write to Bronze Iceberg table."""
    
    # 1. Read JSON from MinIO
    logger.info(f"READ_START | path={input_path}")
    df_raw = spark.read.option("multiline", "true").json(input_path)
    
    if df_raw.count() == 0:
        logger.warning("No records found to ingest.")
        return 0
        
    # 2. Process for Iceberg
    # Fields: place_id, title, rating, reviews_count, location_string, tripadvisor_url, thumbnail, description, source, query_used, collected_at, ingestion_date
    ingested_at = datetime.utcnow()
    df_processed = df_raw.select(
        F.col("place_id"),
        F.col("title"),
        F.col("rating").cast("double"),
        F.col("reviews_count").cast("int"),
        F.col("location_string"),
        F.col("tripadvisor_url"),
        F.col("thumbnail"),
        F.col("description"),
        F.col("source"),
        F.col("query_used"),
        F.to_timestamp(F.col("collected_at")).alias("collected_at"),
        F.to_date(F.col("collected_at")).alias("ingestion_date")
    )
    df_processed.createOrReplaceTempView("stg_tripadvisor")
    
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
        USING stg_tripadvisor s
        ON t.place_id = s.place_id
        WHEN MATCHED THEN
          UPDATE SET 
            t.title = s.title,
            t.rating = s.rating,
            t.reviews_count = s.reviews_count,
            t.location_string = s.location_string,
            t.tripadvisor_url = s.tripadvisor_url,
            t.thumbnail = s.thumbnail,
            t.description = s.description,
            t.query_used = s.query_used,
            t.collected_at = s.collected_at,
            t.ingestion_date = s.ingestion_date
        WHEN NOT MATCHED THEN
          INSERT *
    """
    
    logger.info(f"Executing MERGE INTO for {table}...")
    spark.sql(merge_sql)
    
    count = df_processed.count()
    logger.info(f"Ingestion complete for Tripadvisor. Processed {count} records.")
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

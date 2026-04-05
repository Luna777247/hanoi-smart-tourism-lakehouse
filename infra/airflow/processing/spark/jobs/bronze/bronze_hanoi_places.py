#!/usr/bin/env python3
"""Bronze ingestion: Google Places API → Iceberg bronze.hanoi_attractions.

Follows the same pattern as bronze_events_kafka_stream.py from vuong.ngo.
- Calls Google Places Text Search API (paginated via next_page_token)
- Flattens raw JSON into Iceberg table partitioned by ingestion_date
- Idempotent: overwrites the current day's partition on re-run
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import time
from datetime import date, datetime
from typing import Any, Iterator

import requests
from pyspark.sql import DataFrame, SparkSession, functions as F, types as T

# Reuse anh Vương's shared helpers
from spark_utils import build_spark, ensure_iceberg_table, ensure_schema

APP_NAME = "bronze_hanoi_attractions"
logger = logging.getLogger(__name__)

# ─── Iceberg table DDL ────────────────────────────────────────────────────────
BRONZE_TABLE = "iceberg.bronze.hanoi_attractions"
BRONZE_COLUMNS_SQL = """
    place_id          STRING   COMMENT 'Google Place ID (stable unique key)',
    name              STRING   COMMENT 'Tên điểm du lịch',
    formatted_address STRING   COMMENT 'Địa chỉ đầy đủ',
    lat               DOUBLE   COMMENT 'Vĩ độ GPS',
    lng               DOUBLE   COMMENT 'Kinh độ GPS',
    rating            DOUBLE   COMMENT 'Rating Google (0.0–5.0)',
    user_ratings_total LONG    COMMENT 'Tổng số lượt đánh giá',
    price_level       INT      COMMENT 'Mức giá (0–4), null nếu không có',
    business_status   STRING   COMMENT 'OPERATIONAL / CLOSED_TEMPORARILY / CLOSED_PERMANENTLY',
    types             STRING   COMMENT 'JSON array các loại hình (tourist_attraction, museum...)',
    opening_hours_open_now BOOLEAN COMMENT 'Đang mở cửa hay không tại thời điểm ingest',
    icon              STRING   COMMENT 'URL icon Google Maps',
    vicinity          STRING   COMMENT 'Tên khu vực gần nhất',
    raw_json          STRING   COMMENT 'Toàn bộ JSON gốc từ API (lineage)',
    source_query      STRING   COMMENT 'Query string dùng để tìm kiếm',
    page_token        STRING   COMMENT 'next_page_token của trang kết quả',
    ingested_at       TIMESTAMP COMMENT 'Timestamp ingest (UTC)',
    ingestion_date    DATE     COMMENT 'Partition key – ngày ingest'
"""

# ─── Google Places helper ─────────────────────────────────────────────────────

BASE_URL = "https://maps.googleapis.com/maps/api/place/textsearch/json"

# Các query để đảm bảo coverage đầy đủ các loại điểm tham quan ở Hà Nội
SEARCH_QUERIES = [
    "tourist attractions in Hanoi Vietnam",
    "museums in Hanoi Vietnam",
    "temples pagodas in Hanoi Vietnam",
    "historical sites in Hanoi Vietnam",
    "parks gardens in Hanoi Vietnam",
    "entertainment Hanoi Vietnam",
]


def _call_places_api(
    query: str,
    api_key: str,
    *,
    language: str = "vi",
    page_token: str | None = None,
) -> dict[str, Any]:
    """Single API call – returns raw response dict."""
    params: dict[str, Any] = {
        "query": query,
        "key": api_key,
        "language": language,
        "region": "vn",
    }
    if page_token:
        params["pagetoken"] = page_token

    resp = requests.get(BASE_URL, params=params, timeout=15)
    resp.raise_for_status()
    data = resp.json()

    status = data.get("status", "UNKNOWN")
    if status not in {"OK", "ZERO_RESULTS"}:
        raise RuntimeError(f"Places API error: status={status} | query={query}")

    return data


def fetch_all_pages(query: str, api_key: str) -> Iterator[dict[str, Any]]:
    """Yield every result record across all pages for a query."""
    page_token: str | None = None
    page = 0

    while True:
        # Google requires a small delay before using next_page_token
        if page_token:
            time.sleep(2)

        data = _call_places_api(query, api_key, page_token=page_token)
        results = data.get("results", [])
        next_token = data.get("next_page_token")

        logger.info("PLACES_API | query=%r | page=%d | results=%d | has_next=%s",
                    query, page, len(results), bool(next_token))

        for result in results:
            yield result, query, page_token or ""

        page_token = next_token
        page += 1

        if not page_token:
            break


def _flatten_result(result: dict, query: str, page_token: str, ingested_at: datetime) -> dict:
    """Flatten one Google Places result into a flat dict matching the schema."""
    geometry = result.get("geometry", {})
    location = geometry.get("location", {})
    hours = result.get("opening_hours", {})

    return {
        "place_id":            result.get("place_id"),
        "name":                result.get("name"),
        "formatted_address":   result.get("formatted_address"),
        "lat":                 location.get("lat"),
        "lng":                 location.get("lng"),
        "rating":              result.get("rating"),
        "user_ratings_total":  result.get("user_ratings_total"),
        "price_level":         result.get("price_level"),
        "business_status":     result.get("business_status"),
        "types":               json.dumps(result.get("types", []), ensure_ascii=False),
        "opening_hours_open_now": hours.get("open_now"),
        "icon":                result.get("icon"),
        "vicinity":            result.get("vicinity"),
        "raw_json":            json.dumps(result, ensure_ascii=False, default=str),
        "source_query":        query,
        "page_token":          page_token,
        "ingested_at":         ingested_at,
        "ingestion_date":      ingested_at.date(),
    }


# ─── Spark schema ─────────────────────────────────────────────────────────────

SPARK_SCHEMA = T.StructType([
    T.StructField("place_id",               T.StringType()),
    T.StructField("name",                   T.StringType()),
    T.StructField("formatted_address",      T.StringType()),
    T.StructField("lat",                    T.DoubleType()),
    T.StructField("lng",                    T.DoubleType()),
    T.StructField("rating",                 T.DoubleType()),
    T.StructField("user_ratings_total",     T.LongType()),
    T.StructField("price_level",            T.IntegerType()),
    T.StructField("business_status",        T.StringType()),
    T.StructField("types",                  T.StringType()),
    T.StructField("opening_hours_open_now", T.BooleanType()),
    T.StructField("icon",                   T.StringType()),
    T.StructField("vicinity",               T.StringType()),
    T.StructField("raw_json",               T.StringType()),
    T.StructField("source_query",           T.StringType()),
    T.StructField("page_token",             T.StringType()),
    T.StructField("ingested_at",            T.TimestampType()),
    T.StructField("ingestion_date",         T.DateType()),
])


# ─── Main logic ──────────────────────────────────────────────────────────────

def ingest(
    spark: SparkSession,
    input_path: str,
    *,
    table: str = BRONZE_TABLE,
) -> int:
    """Read raw JSON from Landing Zone and write to Bronze Iceberg table.

    Returns the number of records written.
    """
    ingested_at = datetime.utcnow()
    
    # ── Read from MinIO (Landing Zone)
    logger.info(f"READ_START | path={input_path}")
    df_raw = spark.read.option("multiline", "true").json(input_path)
    
    if df_raw.count() == 0:
        logger.warning("NO_RECORDS | skipping write")
        return 0

    # ── Flatten (Transformation logic)
    # We use a UDF or direct Spark SQL to flatten if needed, 
    # but here we can just map the fields if the schema matches.
    
    # Let's map the fields precisely as defined in SPARK_SCHEMA
    # Note: _flatten_result was used earlier in the driver, now we do it in Spark
    
    df_flattened = df_raw.select(
        F.col("place_id"),
        F.col("name"),
        F.col("formatted_address"),
        F.col("geometry.location.lat").alias("lat"),
        F.col("geometry.location.lng").alias("lng"),
        F.col("rating"),
        F.col("user_ratings_total"),
        F.col("price_level"),
        F.col("business_status"),
        F.to_json(F.col("types")).alias("types"),
        F.col("opening_hours.open_now").alias("opening_hours_open_now"),
        F.col("icon"),
        F.col("vicinity"),
        F.to_json(F.struct("*")).alias("raw_json"), # original record as JSON string
        F.lit("json_landing").alias("source_query"),
        F.lit(None).cast("string").alias("page_token"),
        F.lit(ingested_at).alias("ingested_at"),
        F.lit(ingested_at.date()).alias("ingestion_date")
    )

    # ── Create Iceberg table if not exists
    ensure_schema(spark, "iceberg.bronze")
    ensure_iceberg_table(
        spark,
        table,
        BRONZE_COLUMNS_SQL,
        partition_field="ingestion_date",
        table_properties={"write.target-file-size-bytes": "67108864"},
    )

    # ── Build DataFrame and create temp view
    df_flattened.createOrReplaceTempView("stg_places")

    # ── Build DataFrame and create temp view
    df: DataFrame = spark.createDataFrame(records, schema=SPARK_SCHEMA)
    df.createOrReplaceTempView("stg_places")

    # ── MERGE INTO (Insert + Update by place_id)
    merge_sql = f"""
        MERGE INTO {table} t
        USING stg_places s
        ON t.place_id = s.place_id
        WHEN MATCHED THEN
          UPDATE SET 
            t.name = s.name,
            t.formatted_address = s.formatted_address,
            t.lat = s.lat,
            t.lng = s.lng,
            t.rating = s.rating,
            t.user_ratings_total = s.user_ratings_total,
            t.price_level = s.price_level,
            t.business_status = s.business_status,
            t.types = s.types,
            t.opening_hours_open_now = s.opening_hours_open_now,
            t.icon = s.icon,
            t.vicinity = s.vicinity,
            t.raw_json = s.raw_json,
            t.source_query = s.source_query,
            t.page_token = s.page_token,
            t.ingested_at = s.ingested_at,
            t.ingestion_date = s.ingestion_date
        WHEN NOT MATCHED THEN
          INSERT *
    """
    
    logger.info(f"Executing MERGE INTO for {table}...")
    spark.sql(merge_sql)

    count = df_flattened.count()
    logger.info("WRITE_COMPLETE | table=%s | records=%d | status=upserted", table, count)
    return count


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Bronze ingestion: Google Places JSON -> Iceberg")
    parser.add_argument("--table", default=BRONZE_TABLE, help="Target Iceberg table")
    parser.add_argument("--input-path", required=True, help="Input path for raw JSON files")
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s – %(message)s",
    )
    args = parse_args()

    spark = build_spark(APP_NAME)
    spark.sparkContext.setLogLevel("WARN")

    try:
        count = ingest(spark, input_path=args.input_path, table=args.table)
        logger.info("JOB_SUCCESS | records=%d", count)
    except Exception as exc:
        logger.error("JOB_FAILED | error=%s", exc)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

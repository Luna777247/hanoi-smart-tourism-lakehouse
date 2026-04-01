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
    api_key: str,
    *,
    queries: list[str] | None = None,
    table: str = BRONZE_TABLE,
) -> int:
    """Collect all API results and write to Bronze Iceberg table.

    Returns the number of records written.
    """
    ingested_at = datetime.utcnow()
    today = ingested_at.date()
    active_queries = queries or SEARCH_QUERIES

    # ── Collect from API (runs on driver – API rate limits prevent parallelism)
    records: list[dict] = []
    seen_place_ids: set[str] = set()  # deduplicate across queries

    for query in active_queries:
        for result, q, token in fetch_all_pages(query, api_key):
            pid = result.get("place_id")
            if pid and pid in seen_place_ids:
                continue  # skip cross-query duplicates
            if pid:
                seen_place_ids.add(pid)
            records.append(_flatten_result(result, q, token, ingested_at))

    logger.info("COLLECTION_COMPLETE | total_unique=%d | date=%s", len(records), today)

    if not records:
        logger.warning("NO_RECORDS | skipping write")
        return 0

    # ── Create Iceberg table if not exists
    ensure_schema(spark, "iceberg.bronze")
    ensure_iceberg_table(
        spark,
        table,
        BRONZE_COLUMNS_SQL,
        partition_field="ingestion_date",
        table_properties={"write.target-file-size-bytes": "67108864"},  # 64 MB
    )

    # ── Build DataFrame and write
    df: DataFrame = spark.createDataFrame(records, schema=SPARK_SCHEMA)

    # Overwrite today's partition (idempotent re-runs)
    (
        df.writeTo(table)
        .option("partition-spec", f"ingestion_date='{today}'")
        .overwritePartitions()
    )

    count = df.count()
    logger.info("WRITE_COMPLETE | table=%s | records=%d | partition=%s", table, count, today)
    return count


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Bronze ingestion: Google Places → Iceberg")
    parser.add_argument("--table", default=BRONZE_TABLE, help="Target Iceberg table")
    parser.add_argument(
        "--queries",
        default=",".join(SEARCH_QUERIES),
        help="Comma-separated search queries (default: predefined Hanoi queries)",
    )
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s – %(message)s",
    )
    args = parse_args()

    api_key = os.environ.get("GOOGLE_PLACES_API_KEY", "")
    if not api_key:
        raise EnvironmentError(
            "GOOGLE_PLACES_API_KEY not set. "
            "Add it to .env or inject from HashiCorp Vault (secret/platform/google_places)."
        )

    queries = [q.strip() for q in args.queries.split(",") if q.strip()]
    spark = build_spark(APP_NAME)
    spark.sparkContext.setLogLevel("WARN")

    try:
        count = ingest(spark, api_key, queries=queries, table=args.table)
        logger.info("JOB_SUCCESS | records=%d", count)
    except Exception as exc:
        logger.error("JOB_FAILED | error=%s", exc)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

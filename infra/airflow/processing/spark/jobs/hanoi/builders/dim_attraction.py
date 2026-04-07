"""Silver builder: dim_attraction.

Reads from bronze.hanoi_attractions, deduplicates by place_id (latest snapshot),
enriches with district and attraction_type, generates surrogate key.

Pattern mirrors dim_customer_profile.py from vuong.ngo:
- Use get_json_object on raw_json for forward-compatible parsing
- Surrogate key via xxhash64
- No SCD2 here (attractions are slowly-changing, treat as SCD1 / Type-1 overwrite)
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.window import Window

from hanoi.common import classify_type_udf, extract_district_udf, surrogate_key


def build_dim_attraction(spark: SparkSession, _: DataFrame | None) -> DataFrame:
    """Build the attraction dimension from the latest Bronze snapshot."""

    bronze = spark.table("iceberg.bronze.hanoi_attractions")

    # ── Take the latest ingested record per place_id ──────────────────────────
    w_latest = Window.partitionBy("place_id").orderBy(F.col("ingested_at").desc())

    latest = (
        bronze
        .filter(F.col("place_id").isNotNull())
        # --- Data Quality Checks ---
        .filter((F.col("lat") >= 20.5) & (F.col("lat") <= 21.5))
        .filter((F.col("lng") >= 105.3) & (F.col("lng") <= 106.0))
        .filter((F.col("rating").isNull()) | ((F.col("rating") >= 0) & (F.col("rating") <= 5)))
        # ---------------------------
        .withColumn("_rn", F.row_number().over(w_latest))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    # ── Enrich ────────────────────────────────────────────────────────────────
    enriched = (
        latest
        .withColumn("district",        extract_district_udf(F.col("formatted_address")))
        .withColumn("attraction_type", classify_type_udf(F.col("types")))
        .withColumn(
            "is_operational",
            F.col("business_status") == F.lit("OPERATIONAL"),
        )
        .withColumn(
            "attraction_sk",
            surrogate_key(F.col("place_id"), F.col("ingested_at")),
        )
        .withColumn("processed_at", F.current_timestamp())
    )

    return enriched.select(
        "attraction_sk",
        F.col("place_id").alias("attraction_nk"),   # natural key
        "name",
        "formatted_address",
        "district",
        "attraction_type",
        "lat",
        "lng",
        "rating",
        "user_ratings_total",
        "price_level",
        "is_operational",
        "business_status",
        "opening_hours_open_now",
        "vicinity",
        "types",
        "icon",
        "ingested_at",
        "ingestion_date",
        "processed_at",
    )

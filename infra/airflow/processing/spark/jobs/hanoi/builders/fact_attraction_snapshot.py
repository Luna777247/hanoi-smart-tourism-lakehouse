"""Silver builder: fact_attraction_snapshot.

One row per (place_id, ingestion_date) – captures the daily snapshot of
rating and review count for time-series trend analysis.

This is what powers:
  - Rating trend charts in Superset
  - Overcrowding alert logic (review growth rate)
  - District performance over time

Pattern mirrors fact_order_service.py from vuong.ngo.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.window import Window

from hanoi.common import surrogate_key


def build_fact_attraction_snapshot(spark: SparkSession, _: DataFrame | None) -> DataFrame:
    """Build daily snapshot fact from Bronze table."""

    bronze = spark.table("iceberg.bronze.hanoi_attractions")
    dim = spark.table("iceberg.silver.dim_attraction")

    # ── One snapshot per (place_id, ingestion_date) – use latest ingest of the day
    w_day = Window.partitionBy("place_id", "ingestion_date").orderBy(
        F.col("ingested_at").desc()
    )

    daily = (
        bronze
        .filter(F.col("place_id").isNotNull())
        .filter(F.col("rating").isNotNull())
        .withColumn("_rn", F.row_number().over(w_day))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    # ── Join to dim for surrogate key ─────────────────────────────────────────
    # Left join: fact rows exist even if dim hasn't been refreshed yet
    joined = daily.alias("f").join(
        dim.select("attraction_nk", "attraction_sk", "district", "attraction_type").alias("d"),
        F.col("f.place_id") == F.col("d.attraction_nk"),
        how="left",
    )

    # ── Calculate review growth vs previous day ───────────────────────────────
    w_trend = Window.partitionBy("place_id").orderBy("ingestion_date")

    result = (
        joined
        .withColumn(
            "prev_user_ratings_total",
            F.lag("f.user_ratings_total").over(w_trend),
        )
        .withColumn(
            "review_delta",
            F.col("f.user_ratings_total") - F.col("prev_user_ratings_total"),
        )
        .withColumn(
            "review_growth_rate",
            F.when(
                F.col("prev_user_ratings_total").isNotNull()
                & (F.col("prev_user_ratings_total") > 0),
                (F.col("review_delta") / F.col("prev_user_ratings_total")),
            ).otherwise(F.lit(None).cast("double")),
        )
        .withColumn(
            "snapshot_sk",
            surrogate_key(F.col("f.place_id"), F.col("f.ingestion_date")),
        )
        .withColumn("processed_at", F.current_timestamp())
    )

    return result.select(
        "snapshot_sk",
        F.col("d.attraction_sk"),
        F.col("f.place_id").alias("attraction_nk"),
        F.col("d.district"),
        F.col("d.attraction_type"),
        F.col("f.rating").alias("rating"),
        F.col("f.user_ratings_total"),
        "review_delta",
        "review_growth_rate",
        F.col("f.ingestion_date").alias("snapshot_date"),
        F.col("f.ingested_at"),
        "processed_at",
    )

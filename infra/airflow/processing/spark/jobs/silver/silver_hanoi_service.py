#!/usr/bin/env python3
"""Silver layer service for Hanoi Tourism domain.

Mirrors silver_retail_service.py from vuong.ngo – same CLI interface,
same write_snapshot / enforce_primary_key pattern.

Usage:
    spark-submit silver_hanoi_service.py --tables all
    spark-submit silver_hanoi_service.py --tables dim_attraction
    spark-submit silver_hanoi_service.py --tables dim_attraction,fact_attraction_snapshot
"""

from __future__ import annotations

import argparse
import logging
from typing import Iterable, Sequence

from pyspark.sql import DataFrame, SparkSession, functions as F

from spark_utils import build_spark, ensure_schema
from hanoi.registry import BUILDER_MAP, TableBuilder

logger = logging.getLogger(__name__)


def write_snapshot(df: DataFrame, builder: TableBuilder) -> None:
    """Create or replace Iceberg table with partitioning.
    Identical to silver_retail_service.write_snapshot.
    """
    writer = df.writeTo(builder.table).using("iceberg")
    for column in builder.partition_cols:
        writer = writer.partitionedBy(F.col(column))
    writer.createOrReplace()
    logger.info("TABLE_WRITTEN | table=%s", builder.table)


def enforce_primary_key(df: DataFrame, keys: Sequence[str], table_name: str) -> None:
    """Validate primary key uniqueness before write."""
    dup_count = (
        df.groupBy(*[F.col(k) for k in keys])
        .count()
        .where(F.col("count") > 1)
        .count()
    )
    if dup_count > 0:
        raise ValueError(
            f"PRIMARY_KEY_VIOLATION | table={table_name} | duplicates={dup_count}"
        )
    logger.info("PK_CHECK_PASSED | table=%s", table_name)


def materialise_tables(
    spark: SparkSession,
    builders: Iterable[TableBuilder],
) -> None:
    """Build and persist each Silver table in dependency order."""
    ensure_schema(spark, "iceberg.silver")

    for builder in builders:
        logger.info("BUILDING | table=%s", builder.identifier)
        df = builder.build_fn(spark, None)

        if builder.primary_key:
            enforce_primary_key(df, list(builder.primary_key), builder.identifier)

        write_snapshot(df, builder)
        logger.info("COMPLETED | table=%s", builder.identifier)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build Hanoi Tourism Silver tables")
    parser.add_argument(
        "--tables",
        default="all",
        help="Comma-separated table identifiers, or 'all' (default)",
    )
    return parser.parse_args()


def resolve_selection(selection: str) -> list[TableBuilder]:
    """Return builders in dependency order for the requested selection."""
    if selection.lower() == "all":
        return list(BUILDER_MAP.values())  # already in dependency order

    requested = {s.strip() for s in selection.split(",") if s.strip()}
    unknown = requested - set(BUILDER_MAP)
    if unknown:
        raise ValueError(f"Unknown identifiers: {', '.join(sorted(unknown))}")

    # Preserve insertion order from BUILDER_MAP (dependency order)
    return [b for b in BUILDER_MAP.values() if b.identifier in requested]


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s – %(message)s",
    )
    args = parse_args()
    builders = resolve_selection(args.tables)

    if not builders:
        logger.info("NO_TABLES_SELECTED | exiting")
        return

    app_name = f"silver_hanoi:{':'.join(b.identifier for b in builders)}"
    logger.info("JOB_START | app=%s | tables=%d", app_name, len(builders))

    spark = build_spark(app_name)
    spark.sparkContext.setLogLevel("WARN")

    try:
        materialise_tables(spark, builders)
        logger.info("JOB_SUCCESS")
    except Exception as exc:
        logger.error("JOB_FAILED | error=%s", exc)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

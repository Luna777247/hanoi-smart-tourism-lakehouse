"""Registry of Hanoi Tourism Silver table builders.

Mirrors registry.py from vuong.ngo's retail domain.
Import order matters: dim_attraction must exist before fact_attraction_snapshot.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Sequence

from pyspark.sql import DataFrame, SparkSession

from hanoi.builders.dim_attraction import build_dim_attraction
from hanoi.builders.fact_attraction_snapshot import build_fact_attraction_snapshot


@dataclass(frozen=True)
class TableBuilder:
    identifier: str
    table: str
    build_fn: Callable[[SparkSession, DataFrame | None], DataFrame]
    primary_key: Sequence[str]
    partition_cols: Sequence[str] = ()
    requires_raw_events: bool = False


TABLE_BUILDERS: Sequence[TableBuilder] = (
    # Dimension – build first
    TableBuilder(
        identifier="dim_attraction",
        table="iceberg.silver.dim_attraction",
        build_fn=build_dim_attraction,
        primary_key=("attraction_sk",),
    ),
    # Fact – depends on dim_attraction
    TableBuilder(
        identifier="fact_attraction_snapshot",
        table="iceberg.silver.fact_attraction_snapshot",
        build_fn=build_fact_attraction_snapshot,
        primary_key=("snapshot_sk",),
        partition_cols=("snapshot_date",),
    ),
)

BUILDER_MAP = {b.identifier: b for b in TABLE_BUILDERS}

__all__ = ["TableBuilder", "TABLE_BUILDERS", "BUILDER_MAP"]

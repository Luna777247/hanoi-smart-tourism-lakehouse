"""Airflow DAG: dbt Gold layer orchestration for Hanoi Tourism.

Mirrors dbt_orchestration_dag_v1.py from vuong.ngo.
Uses astronomer-cosmos for native dbt-Airflow integration.

Trigger: Dataset-based – fires when Silver fact_attraction_snapshot is updated.
Models run in dependency order as resolved by dbt DAG:
  gold_attractions_summary → gold_district_performance
                           → gold_rating_trend
                           → gold_overcrowding_alert
"""

import os

from airflow import DAG
from airflow.utils.dates import days_ago

try:
    # Cosmos provides native dbt task integration (already in requirements.txt)
    from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
    from cosmos.profiles import TrinoTokenProfileMapping
    _COSMOS_AVAILABLE = True
except ImportError:
    _COSMOS_AVAILABLE = False

from airflow.providers.standard.operators.bash import BashOperator
from _spark_common import iceberg_dataset

# ─── Config ───────────────────────────────────────────────────────────────────

DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR", "/workspace/pipelines/dbt")
DBT_PROFILES_DIR = os.getenv("DBT_PROFILES_DIR", "/workspace/pipelines/dbt")

TRIGGER_DATASETS = [
    iceberg_dataset("iceberg.silver.fact_attraction_snapshot"),
]

default_args = {
    "owner":           "DataForge",
    "depends_on_past": False,
    "retries":         1,
    "retry_delay":     120,
}

# ─── DAG ──────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="gold_hanoi_dbt",
    description="dbt Gold models: attractions_summary, district_performance, rating_trend, overcrowding_alert",
    doc_md="""\
#### Gold Hanoi – dbt Orchestration

Runs dbt models for the Hanoi Tourism Gold layer via astronomer-cosmos:

| Model | Depends on |
|---|---|
| `gold_attractions_summary`  | silver.dim_attraction + silver.fact_attraction_snapshot |
| `gold_district_performance` | gold_attractions_summary |
| `gold_rating_trend`         | silver.fact_attraction_snapshot |
| `gold_overcrowding_alert`   | gold_attractions_summary |

**Trigger**: When `iceberg.silver.fact_attraction_snapshot` is updated
    """,
    start_date=days_ago(1),
    schedule=TRIGGER_DATASETS,
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=["gold", "hanoi", "dbt", "iceberg"],
) as dag:

    if _COSMOS_AVAILABLE:
        # ── Preferred: Cosmos renders each dbt model as a native Airflow task ──
        profile_config = ProfileConfig(
            profile_name="hanoi_tourism",
            target_name="dev",
            profiles_yml_filepath=f"{DBT_PROFILES_DIR}/profiles.yml",
        )
        dbt_gold = DbtTaskGroup(
            group_id="dbt_gold",
            project_config=ProjectConfig(DBT_PROJECT_DIR),
            profile_config=profile_config,
            execution_config=ExecutionConfig(
                dbt_executable_path=os.getenv("DBT_EXECUTABLE", "dbt"),
            ),
            operator_args={
                "select":      "gold",          # only run gold/* models
                "no_version_check": True,
            },
        )
    else:
        # ── Fallback: plain BashOperator when Cosmos not installed ────────────
        dbt_gold = BashOperator(
            task_id="dbt_run_gold",
            bash_command=(
                f"cd {DBT_PROJECT_DIR} && "
                f"dbt run --profiles-dir {DBT_PROFILES_DIR} --select gold && "
                f"dbt test --profiles-dir {DBT_PROFILES_DIR} --select gold --store-failures"
            ),
        )

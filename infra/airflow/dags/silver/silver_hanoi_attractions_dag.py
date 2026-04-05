"""Airflow DAG: Silver layer – Bronze → dim_attraction + fact_attraction_snapshot.

Mirrors silver_retail_star_schema_dag.py from vuong.ngo:
  - SparkSubmitOperator per table (--tables <identifier>)
  - Dataset-based trigger: fires when Bronze table is updated
  - Dependency: dim_attraction must complete before fact_attraction_snapshot
  - Iceberg maintenance after each build
"""

import os
from typing import Dict

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from _spark_common import (
    iceberg_dataset,
    iceberg_maintenance,
    spark_base_conf,
    spark_env_vars,
    spark_job_base,
    spark_packages,
    spark_utils_py_files,
)

# ─── Constants ────────────────────────────────────────────────────────────────

PACKAGES       = spark_packages()
BASE_CONF      = spark_base_conf()
ENV_VARS       = spark_env_vars()
SPARK_JOB_BASE = spark_job_base()
SPARK_PY_FILES = spark_utils_py_files()

SILVER_APPLICATION = os.path.join(SPARK_JOB_BASE, "silver_hanoi_service.py")

_hanoi_pkg = os.path.join(SPARK_JOB_BASE, "hanoi")
PY_FILES = f"{SPARK_PY_FILES},{_hanoi_pkg}" if os.path.isdir(_hanoi_pkg) else SPARK_PY_FILES

# Trigger when Bronze attractions table is updated (by bronze DAG)
TRIGGER_DATASETS = [
    iceberg_dataset("iceberg.bronze.hanoi_attractions"),
]

SILVER_TABLES = {
    "dim_attraction":          "iceberg.silver.dim_attraction",
    "fact_attraction_snapshot": "iceberg.silver.fact_attraction_snapshot",
}

default_args = {
    "owner":           "DataForge",
    "depends_on_past": False,
    "retries":         1,
    "retry_delay":     180,
}

# ─── Task factory (same pattern as silver_retail_star_schema_dag) ─────────────

def _make_tasks(identifier: str, table: str, dag):
    """Return (build_task, maintenance_task) for a Silver table."""
    build = SparkSubmitOperator(
        task_id=f"build_{identifier}",
        conn_id="spark_default",
        application=SILVER_APPLICATION,
        py_files=PY_FILES,
        packages=PACKAGES,
        env_vars=ENV_VARS,
        conf=BASE_CONF,
        application_args=["--tables", identifier],
        verbose=True,
        outlets=[iceberg_dataset(table)],
        dag=dag,
    )
    maint = PythonOperator(
        task_id=f"iceberg_maintenance_{identifier}",
        python_callable=iceberg_maintenance,
        op_kwargs={"table": table, "expire_days": "30d"},
        dag=dag,
    )
    build >> maint
    return build, maint

# ─── DAG ──────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="silver_hanoi_attractions",
    description="Build Silver dim_attraction + fact_attraction_snapshot from Bronze",
    doc_md="""\
#### Silver Hanoi Attractions

Transforms Bronze Google Places data into Silver Kimball-style tables:

| Table | Type | Description |
|---|---|---|
| `dim_attraction` | Dimension (SCD1) | One row per unique place_id, latest attributes |
| `fact_attraction_snapshot` | Fact | Daily snapshot: rating, review count, growth rate |

**Trigger**: When `iceberg.bronze.hanoi_attractions` is updated
**Dependency order**: `dim_attraction` → `fact_attraction_snapshot`
    """,
    start_date=days_ago(1),
    schedule=TRIGGER_DATASETS,
    catchup=False,
    default_args=default_args,
    max_active_tasks=2,
    tags=["silver", "hanoi", "iceberg", "kimball"],
) as dag:

    build_tasks: Dict[str, SparkSubmitOperator] = {}

    for identifier, table in SILVER_TABLES.items():
        build_task, _ = _make_tasks(identifier, table, dag)
        build_tasks[identifier] = build_task

    # Dependency: fact needs dim to exist first
    build_tasks["dim_attraction"] >> build_tasks["fact_attraction_snapshot"]

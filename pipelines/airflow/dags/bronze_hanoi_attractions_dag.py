"""Airflow DAG: Bronze ingestion – Google Places API → Iceberg.

Follows the exact same structure as bronze_events_kafka_stream_dag.py
from vuong.ngo:
  - Uses _spark_common helpers (spark_base_conf, spark_packages, etc.)
  - SparkSubmitOperator with same conn_id='spark_default'
  - iceberg_maintenance post-ingest
  - Dataset outlets for downstream DAG triggering
  - Daily schedule at 02:00 ICT (UTC+7 → 19:00 UTC)
"""

import os

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

BRONZE_TABLE = "iceberg.bronze.hanoi_attractions"
CHECKPOINT   = "s3a://iceberg/checkpoints/hanoi/bronze/attractions"

PACKAGES     = spark_packages()
BASE_CONF    = spark_base_conf()
ENV_VARS     = spark_env_vars()
SPARK_JOB_BASE = spark_job_base()
SPARK_PY_FILES = spark_utils_py_files()

# Add Google Places API key from environment (set in .env or via Vault)
ENV_VARS["GOOGLE_PLACES_API_KEY"] = os.getenv("GOOGLE_PLACES_API_KEY", "")

BRONZE_APPLICATION = os.path.join(SPARK_JOB_BASE, "bronze_hanoi_places.py")

# Extra py_files: ship hanoi package alongside spark_utils
_hanoi_pkg = os.path.join(SPARK_JOB_BASE, "hanoi")
PY_FILES = f"{SPARK_PY_FILES},{_hanoi_pkg}" if os.path.isdir(_hanoi_pkg) else SPARK_PY_FILES

default_args = {
    "owner":           "DataForge",
    "depends_on_past": False,
    "retries":         1,
    "retry_delay":     300,  # 5 minutes
}

# ─── DAG ──────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="bronze_hanoi_attractions",
    description="Daily ingestion: Google Places API → Bronze Iceberg (hanoi_attractions)",
    doc_md="""\
#### Bronze Hanoi Attractions

Ingests tourist attraction data for Hanoi from the Google Places Text Search API.

**Schedule**: Daily at 02:00 ICT (19:00 UTC previous day)
**Target table**: `iceberg.bronze.hanoi_attractions`
**Partition**: `ingestion_date` (overwrites today's partition on re-run)
**Downstream**: Triggers `silver_hanoi_attractions` DAG via Dataset
    """,
    start_date=days_ago(1),
    schedule_interval="0 19 * * *",   # 02:00 ICT
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=["bronze", "hanoi", "google-places", "iceberg"],
) as dag:

    # ── Task 1: Ingest from Google Places API → Bronze Iceberg ────────────────
    ingest = SparkSubmitOperator(
        task_id="ingest_google_places",
        conn_id="spark_default",
        application=BRONZE_APPLICATION,
        py_files=PY_FILES,
        packages=PACKAGES,
        env_vars=ENV_VARS,
        conf=BASE_CONF,
        application_args=[
            "--table", BRONZE_TABLE,
        ],
        verbose=True,
        outlets=[iceberg_dataset(BRONZE_TABLE)],
    )

    # ── Task 2: Iceberg maintenance (OPTIMIZE + EXPIRE_SNAPSHOTS + REMOVE_ORPHANS)
    maintenance = PythonOperator(
        task_id="iceberg_maintenance_bronze_attractions",
        python_callable=iceberg_maintenance,
        op_kwargs={
            "table":       BRONZE_TABLE,
            "expire_days": "14d",   # keep 2 weeks of snapshots
        },
    )

    ingest >> maintenance

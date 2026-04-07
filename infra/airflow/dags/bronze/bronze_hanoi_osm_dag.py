"""
DAG: bronze_hanoi_osm
Mô tả: Thu thập dữ liệu từ Overpass API + Nominatim → Bronze Iceberg (hanoi_osm)
Sử dụng logic MERGE INTO để thực hiện Insert + Update theo OSM ID.
"""

import os
import sys
import json
import logging
import asyncio
from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.standard.operators.python import PythonOperator
# from airflow.utils.dates import days_ago

logger = logging.getLogger(__name__)

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

BRONZE_TABLE = "iceberg.bronze.hanoi_osm"
PACKAGES     = spark_packages()
BASE_CONF    = spark_base_conf()
ENV_VARS     = spark_env_vars()
SPARK_JOB_BASE = spark_job_base()
SPARK_PY_FILES = spark_utils_py_files()

BRONZE_APPLICATION = os.path.join(SPARK_JOB_BASE, "bronze", "bronze_hanoi_osm.py")

default_args = {
    "owner":           "DataForge",
    "depends_on_past": False,
    "retries":         1,
    "retry_delay":     300,  # 5 minutes
}

# Ingestion scripts
INGESTION_OSM = os.path.join(spark_job_base(), "ingestion", "fetch_hanoi_osm.py")

# ─── DAG ──────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="bronze_hanoi_osm",
    description="2-Stage Ingestion: Overpass API -> Raw JSON -> Iceberg with Upsert",
    doc_md="""\
#### Bronze Hanoi OSM Ingestion

1. **Landing**: Python fetches from Overpass API and saves to MinIO as JSON.
2. **Bronze**: Spark reads JSON from MinIO and upserts into `iceberg.bronze.hanoi_osm`.
    """,
    start_date=datetime(2025, 1, 1),
    schedule="0 20 * * 5",   # 03:00 ICT on Saturday
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=["bronze", "hanoi", "osm", "iceberg", "landing"],
) as dag:

    # ── Task 1: API to Landing JSON ──────────────
    fetch_raw = SparkSubmitOperator(
        task_id="fetch_osm_raw",
        conn_id="spark_default",
        application=INGESTION_OSM,
        env_vars=ENV_VARS,
        conf=BASE_CONF,
        verbose=True,
    )

    # ── Task 2: Ingest from Landing JSON → Bronze Iceberg ────────────────
    ingest = SparkSubmitOperator(
        task_id="ingest_osm_data",
        conn_id="spark_default",
        application=BRONZE_APPLICATION,
        py_files=SPARK_PY_FILES,
        packages=PACKAGES,
        env_vars=ENV_VARS,
        conf=BASE_CONF,
        application_args=[
            "--table", BRONZE_TABLE,
            "--input-path", "s3a://tourism-bronze/source=osm/date={{ ds }}/*.json",
        ],
        verbose=True,
        outlets=[iceberg_dataset(BRONZE_TABLE)],
    )

    # ── Task 2: Iceberg maintenance (OPTIMIZE + EXPIRE_SNAPSHOTS + REMOVE_ORPHANS)
    maintenance = PythonOperator(
        task_id="iceberg_maintenance_bronze_osm",
        python_callable=iceberg_maintenance,
        op_kwargs={
            "table":       BRONZE_TABLE,
            "expire_days": "14d",   # keep 2 weeks of snapshots
        },
    )

    ingest >> maintenance
    fetch_raw >> ingest

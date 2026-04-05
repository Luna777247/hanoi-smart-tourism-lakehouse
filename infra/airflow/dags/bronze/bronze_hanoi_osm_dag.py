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
from airflow.utils.dates import days_ago

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

def fetch_osm_raw(**context):
    """Giai đoạn 1: Gọi Overpass API và lưu JSON thô vào MinIO."""
    from io import BytesIO
    from minio import Minio
    backend_root = os.path.join(os.environ.get("WORKSPACE_ROOT", "/workspace"), "apps", "backend")
    if backend_root not in sys.path:
        sys.path.insert(0, backend_root)

    from app.services.osm_service import OSMService
    
    svc = OSMService()
    results = asyncio.run(svc.fetch_hanoi_attractions_from_overpass())
    logger.info(f"Fetched {len(results)} attractions from Overpass API.")

    minio_client = Minio(
        os.environ["MINIO_ENDPOINT"],
        access_key=os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
        secret_key=os.environ.get("MINIO_SECRET_KEY", "minioadmin123"),
        secure=False,
    )
    date_str = datetime.utcnow().strftime("%Y-%m-%d")
    ts = datetime.utcnow().strftime("%H%M%S")

    json_data = json.dumps(results, ensure_ascii=False).encode("utf-8")
    minio_client.put_object(
        bucket_name="tourism-bronze",
        object_name=f"source=osm/date={date_str}/attractions_{ts}.json",
        data=BytesIO(json_data),
        length=len(json_data),
        content_type="application/json",
    )
    return len(results)

# ─── DAG ──────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="bronze_hanoi_osm",
    description="2-Stage Ingestion: Overpass API -> Raw JSON -> Iceberg with Upsert",
    doc_md="""\
#### Bronze Hanoi OSM Ingestion

1. **Landing**: Python fetches from Overpass API and saves to MinIO as JSON.
2. **Bronze**: Spark reads JSON from MinIO and upserts into `iceberg.bronze.hanoi_osm`.
    """,
    start_date=days_ago(1),
    schedule_interval="0 20 * * 5",   # 03:00 ICT on Saturday
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=["bronze", "hanoi", "osm", "iceberg", "landing"],
) as dag:

    # ── Task 1: API to Landing JSON ──────────────
    fetch_raw = PythonOperator(
        task_id="fetch_osm_raw",
        python_callable=fetch_osm_raw,
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

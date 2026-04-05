"""
DAG: hanoi_tripadvisor_ingestion
Mô tả: Thu thập dữ liệu từ Tripadvisor API (via SerpApi) → Bronze Iceberg (hanoi_tripadvisor)
Sử dụng logic MERGE INTO để thực hiện Insert + Update theo IDs.
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

BRONZE_TABLE = "iceberg.bronze.hanoi_tripadvisor"
PACKAGES     = spark_packages()
BASE_CONF    = spark_base_conf()
ENV_VARS     = spark_env_vars()
SPARK_JOB_BASE = spark_job_base()
SPARK_PY_FILES = spark_utils_py_files()

BRONZE_APPLICATION = os.path.join(SPARK_JOB_BASE, "bronze", "bronze_hanoi_tripadvisor.py")

default_args = {
    "owner":           "DataForge",
    "depends_on_past": False,
    "retries":         1,
    "retry_delay":     300,  # 5 minutes
}

# ─── DAG ──────────────────────────────────────────────────────────────────────

def fetch_tripadvisor_raw(**context):
    """Giai đoạn 1: Gọi Tripadvisor API (SerpApi) và lưu JSON thô vào MinIO."""
    from io import BytesIO
    from minio import Minio
    backend_root = os.path.join(os.environ.get("WORKSPACE_ROOT", "/workspace"), "apps", "backend")
    if backend_root not in sys.path:
        sys.path.insert(0, backend_root)

    from app.services.tripadvisor_service import TripadvisorService
    
    api_key = os.environ.get("SERPAPI_KEY")
    svc = TripadvisorService(api_key=api_key)
    
    queries = ["Hanoi attractions", "Things to do in Hanoi", "Hanoi landmarks"]
    results = asyncio.run(svc.fetch_all_hanoi_attractions(queries=queries))
    logger.info(f"Fetched {len(results)} attractions from Tripadvisor API.")

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
        object_name=f"source=tripadvisor/date={date_str}/attractions_{ts}.json",
        data=BytesIO(json_data),
        length=len(json_data),
        content_type="application/json",
    )
    return len(results)

# ─── DAG ──────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="bronze_hanoi_tripadvisor",
    description="2-Stage Ingestion: Tripadvisor (SerpApi) -> Raw JSON -> Iceberg with Upsert",
    doc_md="""\
#### Tripadvisor Ingestion

1. **Landing**: Python fetches from SerpApi and saves to MinIO as JSON.
2. **Bronze**: Spark reads JSON from MinIO and upserts into `iceberg.bronze.hanoi_tripadvisor`.
    """,
    start_date=days_ago(1),
    schedule_interval="0 21 * * 0",   # 04:00 ICT on Sunday
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=["bronze", "hanoi", "tripadvisor", "iceberg", "landing"],
) as dag:

    # ── Task 1: API to Landing JSON ──────────────
    fetch_raw = PythonOperator(
        task_id="fetch_tripadvisor_raw",
        python_callable=fetch_tripadvisor_raw,
    )

    # ── Task 2: Ingest from Landing JSON → Bronze Iceberg ────────────────
    ingest = SparkSubmitOperator(
        task_id="ingest_tripadvisor_data",
        conn_id="spark_default",
        application=BRONZE_APPLICATION,
        py_files=SPARK_PY_FILES,
        packages=PACKAGES,
        env_vars=ENV_VARS,
        conf=BASE_CONF,
        application_args=[
            "--table", BRONZE_TABLE,
            "--input-path", "s3a://tourism-bronze/source=tripadvisor/date={{ ds }}/*.json",
        ],
        verbose=True,
        outlets=[iceberg_dataset(BRONZE_TABLE)],
    )

    # ── Task 2: Iceberg maintenance (OPTIMIZE + EXPIRE_SNAPSHOTS + REMOVE_ORPHANS)
    maintenance = PythonOperator(
        task_id="iceberg_maintenance_bronze_tripadvisor",
        python_callable=iceberg_maintenance,
        op_kwargs={
            "table":       BRONZE_TABLE,
            "expire_days": "14d",
        },
    )

    ingest >> maintenance
    fetch_raw >> ingest

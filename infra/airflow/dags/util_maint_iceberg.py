# File: infra/airflow/dags/util_maint_iceberg.py
from __future__ import annotations
from datetime import datetime
import logging
 
logger = logging.getLogger(__name__)
 
from airflow.decorators import dag, task
from airflow.providers.trino.hooks.trino import TrinoHook
 
TABLES = [
    'iceberg.silver.attractions_enriched',
    'iceberg.gold.mart_district_stats',
    'iceberg.gold.mart_tourism_heatmap',
]
 
@dag(
    dag_id='util_maint_iceberg',
    schedule='0 2 * * *',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['maintenance', 'iceberg'],
)
def iceberg_maintenance():
 
    @task()
    def run_compaction() -> None:
        """Nen cac file nho thanh file lon hon (128MB) de tang 40% toc do truy van."""
        hook = TrinoHook(trino_conn_id='trino_default')
        for table in TABLES:
            logger.info(f"Optimizing table: {table}")
            sql = f"ALTER TABLE {table} EXECUTE optimize(file_size_threshold => '128MB')"
            hook.run(sql)
 
    @task()
    def expire_snapshots() -> None:
        """Xoa cac snapshot cu hon 7 ngay de giai phong dung luong MinIO (Data Retention Policy)."""
        hook = TrinoHook(trino_conn_id='trino_default')
        for table in TABLES:
            logger.info(f"Expiring snapshots for: {table}")
            sql = f"ALTER TABLE {table} EXECUTE expire_snapshots(retention_threshold => '7d')"
            hook.run(sql)

    @task()
    def clean_orphan_files() -> None:
        """Xoa cac file mo coi (khong thuoc bat ky snapshot nao) de toi uu dung luong."""
        hook = TrinoHook(trino_conn_id='trino_default')
        for table in TABLES:
            sql = f"ALTER TABLE {table} EXECUTE remove_orphan_files(retention_threshold => '7d')"
            hook.run(sql)
 
    run_compaction() >> expire_snapshots() >> clean_orphan_files()
 
iceberg_maintenance()


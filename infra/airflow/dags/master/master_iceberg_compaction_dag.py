"""
DAG: master_iceberg_compaction
Mô tả: Tối ưu hoá file nhỏ (Small Files Problem) cho Iceberg bằng cách gọi REWRITE_DATA_FILES.
"""
from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_sql import SparkSqlOperator
# from airflow.utils.dates import days_ago

default_args = {
    "owner": "DataForge",
    "depends_on_past": False,
}

with DAG(
    dag_id="master_iceberg_compaction",
    schedule="0 0 * * 0",  # Chạy hàng tuần vào CN
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["maintenance", "iceberg", "compaction"],
    default_args=default_args,
) as dag:
    
    compact_tripadvisor = SparkSqlOperator(
        task_id="compact_bronze_tripadvisor",
        conn_id="spark_default",
        sql="CALL catalog.system.rewrite_data_files('iceberg.bronze.hanoi_tripadvisor');",
    )
    
    compact_google = SparkSqlOperator(
        task_id="compact_bronze_google",
        conn_id="spark_default",
        sql="CALL catalog.system.rewrite_data_files('iceberg.bronze.hanoi_attractions');",
    )
    
    compact_silver = SparkSqlOperator(
        task_id="compact_silver_fact",
        conn_id="spark_default",
        sql="CALL catalog.system.rewrite_data_files('iceberg.silver.fact_attraction_snapshot');",
    )

    [compact_tripadvisor, compact_google] >> compact_silver

"""
DAG: hanoi_tourism_ingestion
Mô tả: Master Ingestion DAG – Kích hoạt các luồng thu thập dữ liệu Bronze (Google, OSM, Tripadvisor)
"""
from datetime import timedelta
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "tourism-team",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="hanoi_tourism_ingestion",
    description="Orchestrator for all Bronze Ingestion DAGs",
    schedule_interval="0 0 * * *",  # Hàng ngày lúc 00:00 UTC (07:00 ICT)
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
    tags=["master", "ingestion", "bronze"],
) as dag:

    trigger_google = TriggerDagRunOperator(
        task_id="trigger_google_ingestion",
        trigger_dag_id="bronze_hanoi_google",
        wait_for_completion=True,
    )

    trigger_osm = TriggerDagRunOperator(
        task_id="trigger_osm_ingestion",
        trigger_dag_id="bronze_hanoi_osm",
        wait_for_completion=True,
    )

    trigger_tripadvisor = TriggerDagRunOperator(
        task_id="trigger_tripadvisor_ingestion",
        trigger_dag_id="bronze_hanoi_tripadvisor",
        wait_for_completion=True,
    )

    [trigger_google, trigger_osm, trigger_tripadvisor]

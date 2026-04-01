"""
DAG: hanoi_full_pipeline
Mô tả: Chạy toàn bộ pipeline: Ingestion → Bronze → Silver → Gold → Quality Check
Schedule: Hàng tuần (Chủ nhật 3:00 AM)

- dbt: chạy trong container Airflow (venv /opt/airflow/dbt_venv + DBT_PROJECT_DIR).
- Spark: docker exec spark-master — cần Docker CLI + socket trên worker (xem README).
"""
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "tourism-team",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

dag = DAG(
    dag_id="hanoi_full_pipeline",
    description="End-to-end: Ingestion + Bronze + Silver + Gold + Quality",
    default_args=default_args,
    schedule_interval="0 3 * * 0",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["full-pipeline", "weekly"],
)

_DBT_SHELL_PREFIX = r"""
    set -e
    source /opt/airflow/dbt_venv/bin/activate
    cd "${DBT_PROJECT_DIR:-/workspace/pipelines/dbt}"
"""

with dag:
    trigger_ingestion = TriggerDagRunOperator(
        task_id="trigger_ingestion",
        trigger_dag_id="hanoi_tourism_ingestion",
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command="""
            docker exec spark-master spark-submit \
                --master local[2] \
                --driver-memory 2g \
                --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
                --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog \
                --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,\
org.apache.hadoop:hadoop-aws:3.3.4 \
                /opt/spark/jobs/bronze_to_silver.py \
                --date {{ ds }}
        """,
        execution_timeout=timedelta(minutes=30),
    )

    silver_to_gold = BashOperator(
        task_id="silver_to_gold_dbt",
        bash_command=_DBT_SHELL_PREFIX
        + """
    dbt run --project-dir . --profiles-dir . --models tag:gold \
        --vars '{"execution_date": "{{ ds }}"}'
""",
        execution_timeout=timedelta(minutes=20),
    )

    quality_checks = BashOperator(
        task_id="data_quality_checks",
        bash_command=_DBT_SHELL_PREFIX
        + """
    dbt test --project-dir . --profiles-dir . --models tag:gold
""",
        execution_timeout=timedelta(minutes=10),
    )

    great_expectations = BashOperator(
        task_id="great_expectations_validation",
        bash_command="""
            if [ -d /opt/airflow/great_expectations ] && command -v great_expectations >/dev/null 2>&1; then
              great_expectations checkpoint run tourism_gold_checkpoint \
                --directory /opt/airflow/great_expectations
            else
              echo "SKIP: Great Expectations chưa cài hoặc thư mục /opt/airflow/great_expectations chưa có"
            fi
        """,
        execution_timeout=timedelta(minutes=10),
    )

    trigger_ingestion >> bronze_to_silver >> silver_to_gold >> quality_checks >> great_expectations

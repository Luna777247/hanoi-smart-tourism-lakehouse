# File: infra/airflow/dags/gold_transform_tourism_marts.py
from datetime import datetime, timedelta
from airflow import DAG
import logging
import os

# Import Cosmos to orchestrate dbt
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.profiles import TrinoLDAPProfileMapping

from airflow.operators.python import PythonOperator
from libs.gold_quality_check import run_gold_quality_check

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'lakehouse_admin',
    'depends_on_past': False,
    'start_date': datetime(2026, 4, 1),
    'retries': 2, # Core Pillar 4: Tăng khả năng tự phục hồi
    'retry_delay': timedelta(minutes=5),
}

# dbt configuration
DBT_PROJECT_PATH = "/opt/airflow/dbt_project"
DBT_EXECUTABLE_PATH = "/usr/local/bin/dbt"

with DAG(
    'gold_transform_tourism_marts',
    default_args=default_args,
    description='Generate Business/Analytics Marts in Gold layer using dbt',
    schedule='0 6 * * *', # Runs at 06:00 AM, after the Silver transformation
    catchup=False,
    tags=['gold', 'dbt', 'trino', 'analytics', 'quality'],
) as dag:

    # Use Cosmos to run the Gold Marts
    generate_gold_marts = DbtTaskGroup(
        group_id="dbt_gold_marts",
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=ProfileConfig(
            profile_name="lakehouse",
            target_name="trino",
            profile_mapping=TrinoLDAPProfileMapping(
                conn_id="trino_default",
                profile_args={"schema": "gold"},
            ),
        ),
        render_config=RenderConfig(
            select=["path:models/marts"],
            test_behavior="after_each" # Core Pillar 3: Kiểm tra chất lượng sau mỗi bước
        ),
        execution_config=ExecutionConfig(
            dbt_executable_path=DBT_EXECUTABLE_PATH,
        ),
        operator_args={
            "install_deps": True,
        },
    )

    # Automated Quality Check with Great Expectations
    quality_check_gold = PythonOperator(
        task_id='quality_check_gold_attractions',
        python_callable=run_gold_quality_check,
        op_kwargs={'table_name': 'gold_attractions'}
    )

    generate_gold_marts >> quality_check_gold

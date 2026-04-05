"""
### Run a dbt Core project as a task group with Cosmos
Orchestrate dbt models for Spark Thrift Server with Iceberg
"""

from airflow.sdk import dag
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import SparkThriftProfileMapping
import os

CONNECTION_ID = "spark_default"
SCHEMA_NAME = "iceberg.data_source"

# Path to dbt project (mounted via docker-compose)
DBT_PROJECT_PATH = "/workspace/pipelines/dbt"

# Path to dbt executable in virtual environment
DBT_EXECUTABLE_PATH = "/opt/airflow/dbt_venv/bin/dbt"

profile_config = ProfileConfig(
    profile_name="dbt_vib",
    target_name="iceberg",
    profile_mapping=SparkThriftProfileMapping(
        conn_id=CONNECTION_ID,
        profile_args={"schema": SCHEMA_NAME},
    ),
)

# Use dbt from virtual environment where dbt-spark[PyHive] is installed
execution_config = ExecutionConfig(
    dbt_executable_path=DBT_EXECUTABLE_PATH,
)

@dag(
    dag_id="gold_dbt_full_orchestration",
    schedule=None,
    catchup=False,
    tags=['dbt', 'iceberg', 'spark'],
    params={"ds_dt": "2026-01-28"},
)
def dbt_full_project_orchestration():
    """DAG to orchestrate all dbt models via Cosmos"""
    
    dbt_run_all = DbtTaskGroup(
        group_id="dbt_models",
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config,
        execution_config=execution_config,
        operator_args={
            "vars": '{"ds_dt": "{{ params.ds_dt }}"}',
            "install_deps": True,
        },
        default_args={"retries": 1},
    )


dbt_full_project_orchestration()

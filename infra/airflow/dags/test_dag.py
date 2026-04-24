from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import time

def test_func():
    print("Airflow 3 Dispatch Check: SUCCESS")
    time.sleep(5)

with DAG(
    "test_dag_v1",
    start_date=datetime(2024,1,1),
    schedule=None,
    catchup=False,
    tags=['debug']
) as dag:
    PythonOperator(
        task_id="test_task",
        python_callable=test_func
    )

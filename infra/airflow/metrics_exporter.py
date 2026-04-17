#!/usr/bin/env python3

# Airflow Metrics Exporter Script
# Exports Prometheus metrics for Airflow

import time
import sys
import os

# Add current directory to path for imports
sys.path.insert(0, '/opt/airflow')

try:
    from prometheus_client import start_http_server, Gauge, Counter
    print("Prometheus client imported successfully")
except ImportError as e:
    print(f"Failed to import prometheus_client: {e}")
    sys.exit(1)

try:
    from airflow import settings
    from airflow.models import DagRun, TaskInstance
    from sqlalchemy import func
    print("Airflow imports successful")
except ImportError as e:
    print(f"Failed to import Airflow modules: {e}")
    sys.exit(1)

# Metrics
dag_runs_total = Counter('airflow_dag_runs_total', 'Total number of DAG runs', ['dag_id', 'state'])
tasks_total = Counter('airflow_tasks_total', 'Total number of tasks', ['dag_id', 'task_id', 'state'])
scheduler_heartbeat = Gauge('airflow_scheduler_heartbeat', 'Scheduler heartbeat timestamp')

if __name__ == '__main__':
    print("Starting HTTP server on port 8000...")
    start_http_server(8000)
    print("HTTP server started successfully")

    while True:
        try:
            print("Collecting metrics...")
            session = settings.Session()

            # DAG runs metrics
            dag_runs = session.query(DagRun.dag_id, DagRun.state, func.count(DagRun.run_id)).group_by(DagRun.dag_id, DagRun.state).all()
            for dag_id, state, count in dag_runs:
                dag_runs_total.labels(dag_id=dag_id, state=state).inc(count)
                print(f"DAG {dag_id} state {state}: {count}")

            # Task instances metrics
            tasks = session.query(TaskInstance.dag_id, TaskInstance.task_id, TaskInstance.state, func.count(TaskInstance.task_id)).group_by(TaskInstance.dag_id, TaskInstance.task_id, TaskInstance.state).all()
            for dag_id, task_id, state, count in tasks:
                tasks_total.labels(dag_id=dag_id, task_id=task_id, state=state).inc(count)
                print(f"Task {dag_id}.{task_id} state {state}: {count}")

            session.close()
            print("Metrics collected successfully")
        except Exception as e:
            print(f'Metrics collection error: {e}')
            import traceback
            traceback.print_exc()

        print("Sleeping for 30 seconds...")
        time.sleep(30)
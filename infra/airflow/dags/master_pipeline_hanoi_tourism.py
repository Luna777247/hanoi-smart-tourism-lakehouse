# File: infra/airflow/dags/master_pipeline_hanoi_tourism.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator
import os
import pandas as pd
import folium
from folium.plugins import HeatMap
from trino.dbapi import connect
from libs.governance_sync import trigger_openmetadata_ingestion

default_args = {
    'owner': 'lakehouse_admin',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def generate_heatmap_logic():
    # Ket noi den Trino tu ben trong mang Docker
    conn = connect(
        host='lakehouse-trino',
        port=8080,
        user='admin',
        catalog='iceberg',
        schema='gold',
    )
    
    query = """
        SELECT longitude AS lon, latitude AS lat, popularity_score AS avg_rating, name
        FROM mart_tourism_heatmap
        WHERE latitude IS NOT NULL AND longitude IS NOT NULL
    """
    df = pd.read_sql_query(query, conn)
    
    if df.empty:
        return

    hanoi_map = folium.Map(location=[21.0285, 105.8542], zoom_start=13)
    heat_data = [[row['lat'], row['lon'], row['avg_rating']] for index, row in df.iterrows()]
    HeatMap(heat_data, radius=15, blur=10, min_opacity=0.5).add_to(hanoi_map)
    
    # Luu vao thu muc data de co the xem tu Host
    output_dir = '/opt/airflow/data/reports'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        
    hanoi_map.save(f"{output_dir}/tourism_heatmap.html")
    print(f"Heatmap generated successfully in {output_dir}")

with DAG(
    'master_pipeline_hanoi_tourism',
    default_args=default_args,
    description='Master orchestration for the main OSM -> Google enrichment pipeline',
    schedule=None,
    catchup=False,
    tags=['master', 'strategy', 'lakehouse'],
) as dag:

    # Main pipeline: OSM -> Google enrichment -> Silver -> Gold.
    trigger_enriched_bronze = TriggerDagRunOperator(
        task_id='trigger_bronze_osm_google_enriched',
        trigger_dag_id='bronze_ingest_osm_google_enriched',
        wait_for_completion=True
    )

    trigger_silver = TriggerDagRunOperator(
        task_id='trigger_silver_enriched',
        trigger_dag_id='silver_transform_enriched_data',
        wait_for_completion=True
    )

    trigger_gold = TriggerDagRunOperator(
        task_id='trigger_gold_tourism_marts',
        trigger_dag_id='gold_transform_tourism_marts',
        wait_for_completion=True
    )

    # 4. GENERATE HEATMAP (Visualization)
    create_heatmap = PythonOperator(
        task_id='generate_tourism_heatmap',
        python_callable=generate_heatmap_logic
    )

    # 5. TRIGGER GOVERNANCE (OpenMetadata Sync)
    sync_metadata = PythonOperator(
        task_id='sync_lakehouse_metadata',
        python_callable=trigger_openmetadata_ingestion,
        op_kwargs={'pipeline_name': 'hanoi_lakehouse_metadata_sync'}
    )

    trigger_enriched_bronze >> trigger_silver >> trigger_gold >> create_heatmap >> sync_metadata

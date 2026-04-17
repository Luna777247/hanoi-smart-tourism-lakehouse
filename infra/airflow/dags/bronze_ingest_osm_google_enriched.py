# File: infra/airflow/dags/bronze_ingest_osm_google_enriched.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging

from libs.osm_google_enrichor import OSMGoogleEnrichor

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'lakehouse_admin',
    'depends_on_past': False,
    'start_date': datetime(2026, 4, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

def run_enrichment_flow(**kwargs):
    """
    Main ingestion task: OSM -> Google -> Bronze MinIO
    """
    logical_date = kwargs.get('ds', datetime.now().strftime('%Y-%m-%d'))
    run_id = kwargs.get('run_id', 'manual')
    force = kwargs.get('dag_run').conf.get('force', False) if kwargs.get('dag_run') and kwargs.get('dag_run').conf else False

    enrichor = OSMGoogleEnrichor()
    
    # We set a limit for the batch (e.g., 50 per run)
    # The 'run' method in BaseLakehouseIngestor handles calling collect() 
    # and saving the result to the 'tourism-bronze' bucket in MinIO.
    result = enrichor.run(force=force, logical_date=logical_date, run_id=run_id)
    
    if result == "SKIPPED":
        logger.info("Ingestion skipped for today.")
    elif result:
        logger.info(f"Ingestion successful! Data saved to: {result}")
    else:
        logger.error("Ingestion failed or returned no data.")

with DAG(
    'bronze_ingest_osm_google_enriched',
    default_args=default_args,
    description='Enrich OSM data with Google Places details for Hanoi Lakehouse',
    schedule='0 2 * * *',
    catchup=False,
    tags=['bronze', 'osm', 'google', 'enrichment'],
) as dag:

    enrich_task = PythonOperator(
        task_id='enrich_attractions_osm_google',
        python_callable=run_enrichment_flow,
    )

    enrich_task

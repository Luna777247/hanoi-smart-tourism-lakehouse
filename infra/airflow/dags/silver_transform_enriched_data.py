# File: infra/airflow/dags/silver_transform_enriched_data.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import logging

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'lakehouse_admin',
    'depends_on_past': False,
    'start_date': datetime(2026, 4, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=15),
}

with DAG(
    'silver_transform_enriched_data',
    default_args=default_args,
    description='Process enriched tourism data from Bronze to Silver layer using Spark',
    schedule='0 5 * * *', # Runs at 05:00 AM, after the 04:00 AM Unified ingestion
    catchup=False,
    tags=['silver', 'spark', 'iceberg', 'transformation'],
) as dag:

    # Run Spark job to transform Enriched Data
    transform_to_silver = SparkSubmitOperator(
        task_id='spark_transform_bronze_to_silver',
        application='/opt/spark-jobs/silver/silver_process_enriched_data.py',
        conn_id='spark_default',
        conf={
            'spark.master': 'spark://spark-master:7077',
            'spark.sql.catalog.iceberg': 'org.apache.iceberg.spark.SparkCatalog',
            'spark.sql.catalog.iceberg.catalog-impl': 'org.apache.iceberg.jdbc.JdbcCatalog',
            'spark.sql.catalog.iceberg.uri': 'jdbc:postgresql://postgres:5432/lakehouse_meta',
            'spark.sql.catalog.iceberg.jdbc.user': 'lakehouse_admin',
            'spark.sql.catalog.iceberg.jdbc.password': 'ChangeMe_Strong123!',
            'spark.sql.catalog.iceberg.warehouse': 's3a://tourism-silver/warehouse',
            'spark.jars.packages': ','.join([
                'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2',
                'org.apache.hadoop:hadoop-aws:3.3.4',
                'com.amazonaws:aws-java-sdk-bundle:1.12.261',
                'org.postgresql:postgresql:42.5.0',
            ]),
        },
        name='silver_enrichment_transformation',
    )

    transform_to_silver

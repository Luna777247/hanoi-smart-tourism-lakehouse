# File: infra/airflow/dags/gold_transform_tourism_marts.py
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
    'gold_transform_tourism_marts',
    default_args=default_args,
    description='Generate Business/Analytics Marts in Gold layer',
    schedule='0 6 * * *', # Runs at 06:00 AM, after the Silver transformation
    catchup=False,
    tags=['gold', 'spark', 'iceberg', 'analytics'],
) as dag:

    # Run Spark job to generate Gold Marts
    generate_marts = SparkSubmitOperator(
        task_id='spark_generate_gold_marts',
        application='/opt/spark-jobs/gold/gold_process_tourism_marts.py',
        conn_id='spark_default',
        conf={
            'spark.master': 'spark://spark-master:7077',
            'spark.sql.catalog.iceberg': 'org.apache.iceberg.spark.SparkCatalog',
            'spark.sql.catalog.iceberg.catalog-impl': 'org.apache.iceberg.jdbc.JdbcCatalog',
            'spark.sql.catalog.iceberg.uri': 'jdbc:postgresql://postgres:5432/lakehouse_meta',
            'spark.sql.catalog.iceberg.jdbc.user': 'lakehouse_admin',
            'spark.sql.catalog.iceberg.jdbc.password': 'ChangeMe_Strong123!',
            'spark.sql.catalog.iceberg.warehouse': 's3a://tourism-gold/warehouse',
            'spark.jars.packages': ','.join([
                'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2',
                'org.apache.hadoop:hadoop-aws:3.3.4',
                'com.amazonaws:aws-java-sdk-bundle:1.12.261',
                'org.postgresql:postgresql:42.5.0',
            ]),
        },
        name='gold_marts_generation',
    )

    generate_marts

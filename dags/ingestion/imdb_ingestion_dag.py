from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'imdb_data_ingestion',
    default_args=default_args,
    description='Ingest IMDb datasets to GCS',
    schedule_interval=None,  # Manually triggered initially
    start_date=datetime(2025, 6, 1),
    catchup=False,
    tags=['imdb', 'ingestion'],
) as dag:
    # DAG tasks will be implemented in Phase 1
    pass
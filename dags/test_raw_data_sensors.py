from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.operators.python import PythonOperator
from dotenv import load_dotenv
from google.cloud import storage

# Load env vars
load_dotenv()

# Defaults
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

RAW_BUCKET = os.getenv('RAW_DATA_BUCKET')

# Function to log the files
def list_gcs_files():
    client = storage.Client()
    blobs = client.list_blobs(RAW_BUCKET, prefix='IMDB/')
    files = [blob.name for blob in blobs]
    
    if files:
        print(f"✅ Raw data found! Files: {files}")
    else:
        print("⚠️ No raw data found in IMDB/ prefix.")

# Test DAG
with DAG(
    'test_check_raw_data_sensor',
    default_args=default_args,
    description='Test GCS sensor for IMDB raw data',
    schedule_interval=None,
    start_date=datetime(2025, 6, 1),
    catchup=False,
    tags=['test', 'sensor', 'gcs'],
) as dag:

    check_raw_data = GCSObjectsWithPrefixExistenceSensor(
        task_id='check_raw_data',
        bucket=RAW_BUCKET,
        prefix='IMDB/',
        google_cloud_conn_id='google_cloud_default',
        poke_interval=30,
        timeout=300,
    )

    log_files = PythonOperator(
        task_id='log_raw_data_files',
        python_callable=list_gcs_files,
    )

    check_raw_data >> log_files

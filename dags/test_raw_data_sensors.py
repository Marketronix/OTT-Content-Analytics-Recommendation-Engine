from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator
from google.cloud import storage
from dotenv import load_dotenv

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

# Environment variables
PROJECT_ID = os.getenv('GCP_PROJECT_ID')
REGION = os.getenv('GCP_REGION')
RAW_BUCKET = os.getenv('RAW_DATA_BUCKET')

# Cluster configuration
CLUSTER_NAME = "imdb-processing-cluster"
CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-4",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 100},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n1-standard-4",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 100},
    },
    "software_config": {
        "image_version": "2.0-debian10",
        "properties": {
            "spark:spark.executor.memory": "4g",
            "spark:spark.driver.memory": "4g",
        },
    },
}

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
    'test_check_raw_data_and_create_cluster',
    default_args=default_args,
    description='Test GCS sensor and create Dataproc cluster',
    schedule_interval=None,
    start_date=datetime(2025, 6, 1),
    catchup=False,
    tags=['test', 'sensor', 'gcs', 'dataproc'],
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

    create_cluster = DataprocCreateClusterOperator(
        task_id='create_dataproc_cluster',
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        gcp_conn_id='google_cloud_default',
    )

    # Task dependencies
    check_raw_data >> log_files >> create_cluster

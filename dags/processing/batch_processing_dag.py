# dags/processing/batch_processing_dag.py
from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitPySparkJobOperator,
)
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Environment variables
PROJECT_ID = os.getenv('GCP_PROJECT_ID')
REGION = os.getenv('GCP_REGION')
RAW_BUCKET = os.getenv('RAW_DATA_BUCKET')
PROCESSED_BUCKET = os.getenv('PROCESSED_DATA_BUCKET')
BQ_DATASET = os.getenv('BIGQUERY_DATASET')
PYSPARK_SCRIPTS_BUCKET = f"{os.getenv('PROCESSED_DATA_BUCKET')}/pyspark_scripts"
TEMP_BUCKET = f"{os.getenv('PROCESSED_DATA_BUCKET')}/temp"

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

# List of tables to process
TABLES = [
    "title_basics",
    "title_ratings",
    "title_crew",
    "title_episode",
    "title_principals",
    "title_akas", 
    "name_basics"
]

# Create the DAG
with DAG(
    'imdb_batch_processing',
    default_args=default_args,
    description='Process IMDb datasets with Dataproc',
    schedule_interval=None,  # Manually triggered after ingestion
    start_date=datetime(2025, 8, 6),
    catchup=False,
    tags=['imdb', 'processing', 'dataproc'],
) as dag:

    # Check if raw data exists in GCS
    check_raw_data = GCSObjectsWithPrefixExistenceSensor(
        task_id='check_raw_data',
        bucket=RAW_BUCKET,
        prefix='IMDB/',
        google_cloud_conn_id='google_cloud_default',
        poke_interval=120,  # check every minute
        timeout=600,  # timeout after 10 minutes
    )

    # Create Dataproc cluster
    create_cluster = DataprocCreateClusterOperator(
        task_id='create_dataproc_cluster',
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        gcp_conn_id='google_cloud_default',
    )

    # Delete Dataproc cluster
    delete_cluster = DataprocDeleteClusterOperator(
        task_id='delete_dataproc_cluster',
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
        trigger_rule='all_done',  # Run even if upstream tasks fail
        gcp_conn_id='google_cloud_default',
    )

    # Create PySpark job tasks for each table
    transform_tasks = []
    for table in TABLES:
        transform_task = DataprocSubmitPySparkJobOperator(
            task_id=f'transform_{table}',
            main=f'gs://{PYSPARK_SCRIPTS_BUCKET}/transform_{table}.py',
            arguments=[
                f'--project_id={PROJECT_ID}',
                f'--input_file=gs://{RAW_BUCKET}/IMDB/{table}.tsv',
                f'--output_table={BQ_DATASET}.{table}',
                f'--temp_bucket={TEMP_BUCKET}'
            ],
            cluster_name=CLUSTER_NAME,
            region=REGION,
            dataproc_jars=['gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar'],
            gcp_conn_id='google_cloud_default',
        )
        transform_tasks.append(transform_task)

    # Set up task dependencies
    check_raw_data >> create_cluster
    
    for task in transform_tasks:
        create_cluster >> task >> delete_cluster
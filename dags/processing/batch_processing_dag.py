# dags/processing/batch_processing_dag.py
from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
)
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule
from google.cloud import storage
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

# Cluster name
CLUSTER_NAME = "imdb-processing-cluster"

# Fallback zones
FALLBACK_ZONES = [
    f"https://www.googleapis.com/compute/v1/projects/{PROJECT_ID}/zones/{REGION}-a",
    f"https://www.googleapis.com/compute/v1/projects/{PROJECT_ID}/zones/{REGION}-b",
    f"https://www.googleapis.com/compute/v1/projects/{PROJECT_ID}/zones/{REGION}-c"
]

# List of tables to process
TABLES = [
    "title_basics",
    # "title_ratings",
    # "title_crew",
    # "title_episode",
    # "title_principals",
    # "title_akas", 
    # "name_basics"
]

# Mapping of table names to file names
FILE_NAME_MAPPING = {
    "title_basics": "title.basics.tsv",
    "title_ratings": "title.ratings.tsv",
    "title_crew": "title.crew.tsv",
    "title_episode": "title.episode.tsv",
    "title_principals": "title.principals.tsv",
    "title_akas": "title.akas.tsv",
    "name_basics": "name.basics.tsv"
}

# Function to log the files in GCS
def list_gcs_files():
    """List all files in the IMDB/ prefix in the raw data bucket."""
    client = storage.Client()
    blobs = client.list_blobs(RAW_BUCKET, prefix='IMDB/')
    files = [blob.name for blob in blobs]

    if files:
        print(f"✅ Raw data found! Files: {files}")
        for file in files:
            print(f"  - {file}")
    else:
        print("⚠️ No raw data found in IMDB/ prefix.")
    
    return files

# Function to dynamically build cluster config for given zone
def get_cluster_config(zone_uri):
    return {
        "gce_cluster_config": {
            "zone_uri": zone_uri
        },
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

# Create the DAG
with DAG(
    'imdb_batch_processing',
    default_args=default_args,
    description='Process IMDb datasets with Dataproc',
    schedule_interval=None,  # Manually triggered after ingestion
    start_date=datetime(2025, 6, 1),
    catchup=False,
    tags=['imdb', 'processing', 'dataproc'],
) as dag:

    # Check if raw data exists in GCS
    check_raw_data = GCSObjectsWithPrefixExistenceSensor(
        task_id='check_raw_data',
        bucket=RAW_BUCKET,
        prefix='IMDB/',
        google_cloud_conn_id='google_cloud_default',
        poke_interval=30,  # check every 30 seconds
        timeout=300,  # timeout after 5 minutes
    )

    # Log the available files
    log_files = PythonOperator(
        task_id='log_raw_data_files',
        python_callable=list_gcs_files,
    )

    # Create fallback create_cluster tasks
    previous_create_task = None
    create_cluster_tasks = []

    for index, zone_uri in enumerate(FALLBACK_ZONES):
        create_task = DataprocCreateClusterOperator(
            task_id=f'create_dataproc_cluster_zone_{index+1}',
            project_id=PROJECT_ID,
            cluster_config=get_cluster_config(zone_uri),
            region=REGION,
            cluster_name=CLUSTER_NAME,
            gcp_conn_id='google_cloud_default',
            trigger_rule=TriggerRule.ALL_FAILED if index > 0 else TriggerRule.ALL_SUCCESS,  # fallback if previous failed
        )
        create_cluster_tasks.append(create_task)
        
        if index == 0:
            # First create task depends on log_files
            log_files >> create_task
        else:
            # Fallback: trigger this if previous create task failed
            previous_create_task >> create_task
        
        previous_create_task = create_task

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
        file_name = FILE_NAME_MAPPING[table]
        
        pyspark_job = {
            "reference": {"project_id": PROJECT_ID},
            "placement": {"cluster_name": CLUSTER_NAME},
            "pyspark_job": {
                "main_python_file_uri": f"gs://{PYSPARK_SCRIPTS_BUCKET}/transform_{table}.py",
                "args": [
                    f"--project_id={PROJECT_ID}",
                    f"--input_file=gs://{RAW_BUCKET}/IMDB/{file_name}",
                    f"--output_table={BQ_DATASET}.{table}",
                    f"--temp_bucket={TEMP_BUCKET}"
                ],
                "jar_file_uris": ["gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar"]
            }
        }
        
        transform_task = DataprocSubmitJobOperator(
            task_id=f'transform_{table}',
            job=pyspark_job,
            region=REGION,
            project_id=PROJECT_ID,
            gcp_conn_id='google_cloud_default',
        )
        transform_tasks.append(transform_task)

    # Set up task dependencies
    check_raw_data >> log_files

    for task in transform_tasks:
        previous_create_task >> task >> delete_cluster

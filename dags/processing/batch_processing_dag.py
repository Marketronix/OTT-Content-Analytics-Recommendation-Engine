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
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from google.cloud import storage
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Default args for DAG
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
REGION = os.getenv('GCP_REGION')  # e.g. 'us-east4'
RAW_BUCKET = os.getenv('RAW_DATA_BUCKET')
PROCESSED_BUCKET = os.getenv('PROCESSED_DATA_BUCKET')
BQ_DATASET = os.getenv('BIGQUERY_DATASET')
PYSPARK_SCRIPTS_BUCKET = f"{PROCESSED_BUCKET}/pyspark_scripts"
TEMP_BUCKET = f"{PROCESSED_BUCKET}/temp"

# Zones fallback list - order of zones to try cluster creation in
FALLBACK_ZONES = [
    f"{REGION}-a",
    f"{REGION}-b",
    f"{REGION}-c",
]

# Function to create cluster config with specified zone
def get_cluster_config(zone_uri):
    return {
        "gce_cluster_config": {
            "zone_uri": zone_uri,
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


# Tables to process
TABLES = [
    "title_basics",
    # "title_ratings",
    # "title_crew",
    # "title_episode",
    # "title_principals",
    # "title_akas",
    # "name_basics"
]

# File name mapping
FILE_NAME_MAPPING = {
    "title_basics": "title.basics.tsv",
    "title_ratings": "title.ratings.tsv",
    "title_crew": "title.crew.tsv",
    "title_episode": "title.episode.tsv",
    "title_principals": "title.principals.tsv",
    "title_akas": "title.akas.tsv",
    "name_basics": "name.basics.tsv"
}

# Function to list files in GCS
def list_gcs_files():
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

# Define the DAG
with DAG(
    'imdb_batch_processing',
    default_args=default_args,
    description='Process IMDb datasets with Dataproc with zone fallback',
    schedule_interval=None,  # Manually triggered
    start_date=datetime(2025, 6, 1),
    catchup=False,
    tags=['imdb', 'processing', 'dataproc'],
) as dag:

    # Sensor to check raw data existence
    check_raw_data = GCSObjectsWithPrefixExistenceSensor(
        task_id='check_raw_data',
        bucket=RAW_BUCKET,
        prefix='IMDB/',
        google_cloud_conn_id='google_cloud_default',
        poke_interval=30,
        timeout=300,
    )

    # Log raw files found
    log_files = PythonOperator(
        task_id='log_raw_data_files',
        python_callable=list_gcs_files,
    )

    # Create Dataproc cluster fallback tasks (try zones in order)
    create_cluster_tasks = []
    for index, zone_uri in enumerate(FALLBACK_ZONES):
        create_task = DataprocCreateClusterOperator(
            task_id=f'create_dataproc_cluster_zone_{index+1}',
            project_id=PROJECT_ID,
            cluster_config=get_cluster_config(zone_uri),
            region=REGION,
            cluster_name="imdb-processing-cluster",
            gcp_conn_id='google_cloud_default',
            trigger_rule=TriggerRule.ALL_FAILED if index > 0 else TriggerRule.ALL_SUCCESS,
        )
        create_cluster_tasks.append(create_task)

        if index == 0:
            log_files >> create_task
        else:
            create_cluster_tasks[index - 1] >> create_task

    # Dummy task to continue if any cluster creation succeeded
    cluster_created = EmptyOperator(
        task_id='cluster_created',
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )
    create_cluster_tasks >> cluster_created

    # Create PySpark transform jobs for each table
    transform_tasks = []
    for table in TABLES:
        file_name = FILE_NAME_MAPPING[table]
        pyspark_job = {
            "reference": {"project_id": PROJECT_ID},
            "placement": {"cluster_name": "imdb-processing-cluster"},
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

    cluster_created >> transform_tasks

    # Delete the cluster after all transforms (or if any fail)
    delete_cluster = DataprocDeleteClusterOperator(
        task_id='delete_dataproc_cluster',
        project_id=PROJECT_ID,
        cluster_name="imdb-processing-cluster",
        region=REGION,
        trigger_rule=TriggerRule.ALL_DONE,
        gcp_conn_id='google_cloud_default',
    )

    transform_tasks >> delete_cluster

    # Connect sensor to log_files
    check_raw_data >> log_files

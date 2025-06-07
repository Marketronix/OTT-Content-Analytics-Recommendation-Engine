# dags/ingestion/imdb_ingestion_dag.py
from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Import helper functions
import sys
sys.path.append('/home/airflow/gcs/dags')
from ingestion.helpers import download_to_gcs, check_dataset_changes, extract_metadata

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,  # Increased retries
    'retry_delay': timedelta(minutes=15),
    'retry_exponential_backoff': True,
    }

# Define constants from environment variables with defaults
RAW_DATA_BUCKET = os.getenv('RAW_DATA_BUCKET')
PROCESSED_DATA_BUCKET = os.getenv('PROCESSED_DATA_BUCKET')
ARCHIVE_DATA_BUCKET = os.getenv('ARCHIVE_DATA_BUCKET')
PROJECT_ID = os.getenv('GCP_PROJECT_ID')
BIGQUERY_DATASET = os.getenv('BIGQUERY_DATASET')
DATASETS = os.getenv('IMDB_DATASETS')

with DAG(
    'imdb_data_ingestion',
    default_args=default_args,
    description='Ingest IMDb datasets to GCS',
    schedule_interval='@weekly',  # Weekly refresh
    start_date=datetime(2025, 6, 1),
    catchup=False,
    tags=['imdb', 'ingestion'],
) as dag:
    
    # Task 1: Download IMDb datasets directly to GCS
    download_datasets = PythonOperator(
        task_id='download_imdb_datasets',
        python_callable=download_to_gcs,
        op_kwargs={
            'datasets': DATASETS,
            'bucket_name': RAW_DATA_BUCKET,
            'prefix': 'imdb/'
        },
        execution_timeout=timedelta(hours=1)  # Extended timeout
    )
   
    # Task 2: Check for dataset changes
    check_changes = PythonOperator(
        task_id='check_dataset_changes',
        python_callable=check_dataset_changes,
        op_kwargs={
            'datasets': DATASETS,
            'bucket_name': RAW_DATA_BUCKET,
            'prefix': 'imdb/'
        },
        provide_context=True
    )
    
    # Task 3: Extract dataset metadata
    extract_dataset_metadata = PythonOperator(
        task_id='extract_dataset_metadata',
        python_callable=extract_metadata,
        op_kwargs={
            'datasets': DATASETS,
            'bucket_name': RAW_DATA_BUCKET,
            'prefix': 'imdb/'
        },
        provide_context=True
    )
    
    # Task to log ingestion metadata to BigQuery
    log_ingestion_metadata = BigQueryInsertJobOperator(
        task_id='log_ingestion_metadata',
        configuration={
            'query': {
                'query': """
                INSERT INTO `{{ params.project_id }}.{{ params.dataset }}.ingestion_log`
                (dataset_name, ingestion_date, file_size_bytes, record_count, md5_hash, has_changed)
                VALUES
                {% for dataset in task_instance.xcom_pull(task_ids='extract_dataset_metadata') %}
                ('{{ dataset.name }}', 
                 CURRENT_TIMESTAMP(), 
                 {{ dataset.size }}, 
                 {{ dataset.records }}, 
                 '{{ dataset.hash }}', 
                 {{ dataset.changed|lower }})
                 {% if not loop.last %},{% endif %}
                {% endfor %}
                """,
                'useLegacySql': False,
                'priority': 'BATCH',
            }
        },
        gcp_conn_id='google_cloud_default',
        params={
            'project_id': PROJECT_ID,
            'dataset': BIGQUERY_DATASET
        }
    )
    
    # Define the task dependencies
    download_datasets >> check_changes >> extract_dataset_metadata >> log_ingestion_metadata
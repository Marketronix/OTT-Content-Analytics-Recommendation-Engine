# dags/ingestion/imdb_ingestion_dag.py
from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Import helper functions
import sys
sys.path.append('/home/airflow/gcs/dags')
from ingestion.helpers import download_imdb_datasets, check_dataset_changes, extract_metadata, verify_downloads

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=15),
    'retry_exponential_backoff': True,  # Use exponential backoff
    }

# Define constants from environment variables
RAW_DATA_BUCKET = os.getenv('RAW_DATA_BUCKET')
PROCESSED_DATA_BUCKET = os.getenv('PROCESSED_DATA_BUCKET')
ARCHIVE_DATA_BUCKET = os.getenv('ARCHIVE_DATA_BUCKET')
PROJECT_ID = os.getenv('GCP_PROJECT_ID')
BIGQUERY_DATASET = os.getenv('BIGQUERY_DATASET')
DATASETS = os.getenv('IMDB_DATASETS').split(',')
TEMP_DOWNLOAD_PATH = '/home/airflow/gcs/data/tmp_imdb'

with DAG(
    'imdb_data_ingestion',
    default_args=default_args,
    description='Ingest IMDb datasets to GCS',
    schedule_interval='@weekly',  # Weekly refresh
    start_date=datetime(2025, 6, 1),
    catchup=False,
    tags=['imdb', 'ingestion'],
) as dag:
    
    # Task 1: Download IMDb datasets
    download_datasets = PythonOperator(
        task_id='download_imdb_datasets',
        python_callable=download_imdb_datasets,
        op_kwargs={
            'datasets': DATASETS,
            'download_path': TEMP_DOWNLOAD_PATH
        },
        execution_timeout=timedelta(seconds=1800)
    )

    # 
    verify_downloads_task = PythonOperator(
        task_id='verify_downloads',
        python_callable=verify_downloads,
        op_kwargs={
            'datasets': DATASETS,
            'download_path': TEMP_DOWNLOAD_PATH
        }
    )
    
    # Task 2: Check for dataset changes
    check_changes = PythonOperator(
        task_id='check_dataset_changes',
        python_callable=check_dataset_changes,
        op_kwargs={
            'datasets': DATASETS,
            'download_path': TEMP_DOWNLOAD_PATH,
            'bucket_name': RAW_DATA_BUCKET
        },
        provide_context=True
    )
    
    # Task 3: Extract dataset metadata
    extract_dataset_metadata = PythonOperator(
        task_id='extract_dataset_metadata',
        python_callable=extract_metadata,
        op_kwargs={
            'datasets': DATASETS,
            'download_path': TEMP_DOWNLOAD_PATH
        },
        provide_context=True
    )
    
    # Create a dynamic list of upload tasks
    upload_tasks = []
    for dataset in DATASETS:
        upload_to_gcs = LocalFilesystemToGCSOperator(
            task_id=f'upload_{dataset.replace(".", "_")}',
            src=os.path.join(TEMP_DOWNLOAD_PATH, dataset),
            dst=f'imdb/{dataset}',
            bucket=RAW_DATA_BUCKET,
            gcp_conn_id='google_cloud_default',
            trigger_rule=TriggerRule.ONE_SUCCESS  # Continue even if check_changes says no changes
        )
        upload_tasks.append(upload_to_gcs)
    
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
        'dataset': 'ott-analytics-engine'
    }
)
    
    # Define the task dependencies
    download_datasets >> verify_downloads_task >> check_changes >> extract_dataset_metadata
    
    # Add dynamic upload task dependencies
    for upload_task in upload_tasks:
        extract_dataset_metadata >> upload_task >> log_ingestion_metadata
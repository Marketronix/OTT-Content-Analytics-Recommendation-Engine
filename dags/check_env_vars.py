# dags/check_env_vars.py
from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def check_environment_variables():
    """Print environment variables to debug."""
    # Check all environment variables
    print("All environment variables:")
    for key, value in os.environ.items():
        if 'PASSWORD' not in key.upper() and 'SECRET' not in key.upper():
            print(f"  {key}: {value}")
    
    # Check specific variables
    print("\nSpecific variables:")
    imdb_datasets = os.getenv('IMDB_DATASETS')
    print(f"IMDB_DATASETS: {imdb_datasets}")
    
    if imdb_datasets:
        print("Split result:")
        for i, dataset in enumerate(imdb_datasets.split(',')):
            print(f"  {i}: '{dataset}'")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'check_env_vars',
    default_args=default_args,
    description='Check environment variables',
    schedule_interval=None,
    start_date=datetime(2025, 6, 1),
    catchup=False,
    tags=['debug'],
) as dag:
    
    check_env = PythonOperator(
        task_id='check_environment_variables',
        python_callable=check_environment_variables,
    )
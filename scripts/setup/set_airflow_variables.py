# scripts/setup/set_airflow_variables.py
from airflow.models import Variable
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def set_airflow_variables():
    """Set Airflow variables needed for the DAGs."""
    variables = {
        'project_id': os.getenv('GCP_PROJECT_ID'),
        'raw_data_bucket': os.getenv('RAW_DATA_BUCKET'),
        'processed_data_bucket': os.getenv('PROCESSED_DATA_BUCKET'),
        'archive_data_bucket': os.getenv('ARCHIVE_DATA_BUCKET'),
        'imdb_datasets': os.getenv('IMDB_DATASETS')
    }
    
    for key, value in variables.items():
        Variable.set(key, value)
        print(f"Set variable: {key} = {value}")

if __name__ == "__main__":
    set_airflow_variables()
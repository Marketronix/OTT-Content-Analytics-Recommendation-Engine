# scripts/setup/create_ingestion_log_table.py
from google.cloud import bigquery
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def create_ingestion_log_table():
    """Create the ingestion_log table in BigQuery."""
    client = bigquery.Client(project=os.getenv('GCP_PROJECT_ID'))
    
    schema = [
        bigquery.SchemaField("dataset_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("ingestion_date", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("file_size_bytes", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("md5_hash", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("has_changed", "BOOLEAN", mode="REQUIRED"),
    ]
    
    table_id = f"{os.getenv('BIGQUERY_DATASET_ID')}.ingestion_log"
    
    table = bigquery.Table(table_id, schema=schema)
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="ingestion_date"
    )
    
    table = client.create_table(table, exists_ok=True)
    print(f"Created table {table.project}.{table.dataset_id}.{table.table_id}")

if __name__ == "__main__":
    create_ingestion_log_table()
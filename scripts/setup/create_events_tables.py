# scripts/setup/create_events_tables.py
from google.cloud import bigquery
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

PROJECT_ID = os.getenv('GCP_PROJECT_ID')
DATASET_ID = os.getenv('BIGQUERY_DATASET')

# Initialize client
client = bigquery.Client(project=PROJECT_ID)

# Create events schema
events_schema = [
    bigquery.SchemaField("event_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("user_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("content_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("event_type", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("session_id", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("duration", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("position", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("device", "RECORD", mode="NULLABLE", fields=[
        bigquery.SchemaField("type", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("os", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("browser", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("model", "STRING", mode="NULLABLE"),
    ]),
    bigquery.SchemaField("location", "RECORD", mode="NULLABLE", fields=[
        bigquery.SchemaField("country", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("region", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("city", "STRING", mode="NULLABLE"),
    ]),
    bigquery.SchemaField("quality", "RECORD", mode="NULLABLE", fields=[
        bigquery.SchemaField("resolution", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("bitrate", "INTEGER", mode="NULLABLE"),
    ]),
    bigquery.SchemaField("rating", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("extra_attributes", "JSON", mode="NULLABLE"),
]

# Create the table
table_id = f"{PROJECT_ID}.{DATASET_ID}.user_events"
table = bigquery.Table(table_id, schema=events_schema)

# Set table partitioning by timestamp
table.time_partitioning = bigquery.TimePartitioning(
    type_=bigquery.TimePartitioningType.DAY,
    field="timestamp"
)

# Set table clustering by user_id and content_id
table.clustering_fields = ["user_id", "content_id", "event_type"]

# Create the table
table = client.create_table(table, exists_ok=True)
print(f"Created table {table_id}, partitioned by day on timestamp field and clustered by user_id, content_id, and event_type")
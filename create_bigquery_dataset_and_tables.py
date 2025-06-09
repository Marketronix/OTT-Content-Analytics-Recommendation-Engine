# setup_bigquery_env.py
import os
from dotenv import load_dotenv
from google.cloud import bigquery

# Load .env variables
load_dotenv()
project_id = os.getenv("GCP_PROJECT_ID")
region = os.getenv("GCP_REGION")
dataset_name = os.getenv("BQ_DATASET_ID")

# Initialize BigQuery client
client = bigquery.Client(project=project_id)

# Create dataset
dataset_id = f"{project_id}.{dataset_name}"
dataset = bigquery.Dataset(dataset_id)
dataset.location = region
dataset = client.create_dataset(dataset, exists_ok=True)
print(f"Created dataset {dataset_id} in {region}")

# Define table schemas
tables = {
    "title_akas": [
        bigquery.SchemaField("titleId", "STRING"),
        bigquery.SchemaField("ordering", "INTEGER"),
        bigquery.SchemaField("title", "STRING"),
        bigquery.SchemaField("region", "STRING"),
        bigquery.SchemaField("language", "STRING"),
        bigquery.SchemaField("types", "STRING", mode="REPEATED"),
        bigquery.SchemaField("attributes", "STRING", mode="REPEATED"),
        bigquery.SchemaField("isOriginalTitle", "BOOLEAN"),
    ],
    "title_basics": [
        bigquery.SchemaField("tconst", "STRING"),
        bigquery.SchemaField("titleType", "STRING"),
        bigquery.SchemaField("primaryTitle", "STRING"),
        bigquery.SchemaField("originalTitle", "STRING"),
        bigquery.SchemaField("isAdult", "BOOLEAN"),
        bigquery.SchemaField("startYear", "STRING"),
        bigquery.SchemaField("endYear", "STRING"),
        bigquery.SchemaField("runtimeMinutes", "INTEGER"),
        bigquery.SchemaField("genres", "STRING", mode="REPEATED"),
    ],
    "title_crew": [
        bigquery.SchemaField("tconst", "STRING"),
        bigquery.SchemaField("directors", "STRING", mode="REPEATED"),
        bigquery.SchemaField("writers", "STRING", mode="REPEATED"),
    ],
    "title_episode": [
        bigquery.SchemaField("tconst", "STRING"),
        bigquery.SchemaField("parentTconst", "STRING"),
        bigquery.SchemaField("seasonNumber", "INTEGER"),
        bigquery.SchemaField("episodeNumber", "INTEGER"),
    ],
    "title_principals": [
        bigquery.SchemaField("tconst", "STRING"),
        bigquery.SchemaField("ordering", "INTEGER"),
        bigquery.SchemaField("nconst", "STRING"),
        bigquery.SchemaField("category", "STRING"),
        bigquery.SchemaField("job", "STRING"),
        bigquery.SchemaField("characters", "STRING"),
    ],
    "title_ratings": [
        bigquery.SchemaField("tconst", "STRING"),
        bigquery.SchemaField("averageRating", "FLOAT"),
        bigquery.SchemaField("numVotes", "INTEGER"),
    ],
    "name_basics": [
        bigquery.SchemaField("nconst", "STRING"),
        bigquery.SchemaField("primaryName", "STRING"),
        bigquery.SchemaField("birthYear", "INTEGER"),
        bigquery.SchemaField("deathYear", "INTEGER"),
        bigquery.SchemaField("primaryProfession", "STRING", mode="REPEATED"),
        bigquery.SchemaField("knownForTitles", "STRING", mode="REPEATED"),
    ],
}

# Create tables
for table_name, schema in tables.items():
    table_id = f"{dataset_id}.{table_name}"
    table = bigquery.Table(table_id, schema=schema)

    # Example of adding partitioning/clustering — optional!
    if table_name == "title_ratings":
        # Ratings — could partition if you had a timestamp — but no date column in your schema.
        # We'll skip partitioning unless you add a "loadDate" field later.
        table.clustering_fields = ["numVotes"]  # Example clustering on numVotes

    if table_name == "title_basics":
        table.clustering_fields = ["titleType", "startYear"]

    if table_name == "dim_content":  # future use — year partitioning
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.YEAR,
            field="startYear",  # If you add such a column
        )
        table.clustering_fields = ["genres"]

    table = client.create_table(table, exists_ok=True)
    print(f"Created table {table_id}")

print("BigQuery environment setup complete.")

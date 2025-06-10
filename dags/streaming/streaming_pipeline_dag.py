# Updated streaming_pipeline_dag.py
from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.providers.google.cloud.operators.pubsub import PubSubCreateTopicOperator, PubSubCreateSubscriptionOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Default args
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Environment variables
PROJECT_ID = os.getenv('GCP_PROJECT_ID')
REGION = os.getenv('GCP_REGION')
DATASET_ID = os.getenv('BIGQUERY_DATASET')
GCS_BUCKET = os.getenv('PROCESSED_DATA_BUCKET')
TEMP_LOCATION = f"gs://{GCS_BUCKET}/temp"

# Topics and subscriptions
RAW_TOPIC = 'ott-raw-events'
RAW_SUB = 'ott-raw-events-sub'
PROCESSED_TOPIC = 'ott-processed-events'
PROCESSED_SUB = 'ott-processed-events-sub'

# Dataflow job name
JOB_NAME = 'ott-events-processing'

# Function to ensure temp directory exists
def ensure_temp_directory():
    """Make sure the temp directory exists in GCS."""
    from google.cloud import storage
    
    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(GCS_BUCKET)
    
    # Create an empty file to ensure the directory exists
    blob = bucket.blob('temp/.directory_marker')
    blob.upload_from_string('')
    
    print(f"Created temp directory marker at gs://{GCS_BUCKET}/temp/")
    return True

# Function to create a basic pipeline script
def create_pipeline_script():
    """Create and upload a simple Dataflow pipeline script."""
    from google.cloud import storage
    
    pipeline_code = """
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import json
import logging

class ParsePubSubMessage(beam.DoFn):
    def process(self, element):
        try:
            message = json.loads(element.decode('utf-8'))
            yield message
        except Exception as e:
            logging.error(f"Error parsing message: {e}")
            return

def run(argv=None):
    import argparse
    
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_subscription')
    parser.add_argument('--output_table')
    known_args, pipeline_args = parser.parse_known_args(argv)
    
    pipeline_options = PipelineOptions(pipeline_args)
    
    with beam.Pipeline(options=pipeline_options) as p:
        (p 
         | 'Read from PubSub' >> beam.io.ReadFromPubSub(subscription=known_args.input_subscription)
         | 'Parse JSON' >> beam.ParDo(ParsePubSubMessage())
         | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
             known_args.output_table,
             create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
             write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
    """
    
    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(GCS_BUCKET)
    
    # Upload the pipeline code
    blob = bucket.blob('dataflow/scripts/simple_pipeline.py')
    blob.upload_from_string(pipeline_code)
    
    print(f"Uploaded pipeline script to gs://{GCS_BUCKET}/dataflow/scripts/simple_pipeline.py")
    return f"gs://{GCS_BUCKET}/dataflow/scripts/simple_pipeline.py"

# DAG definition
with DAG(
    'ott_streaming_pipeline',
    default_args=default_args,
    description='OTT streaming events processing pipeline',
    schedule_interval=None,  # Manually triggered
    start_date=datetime(2025, 6, 1),
    catchup=False,
    tags=['ott', 'streaming', 'events'],
) as dag:
    
    # Create raw events topic
    create_raw_topic = PubSubCreateTopicOperator(
        task_id='create_raw_topic',
        project_id=PROJECT_ID,
        topic=RAW_TOPIC,
        fail_if_exists=False,
    )
    
    # Create subscription for raw events
    create_raw_subscription = PubSubCreateSubscriptionOperator(
        task_id='create_raw_subscription',
        project_id=PROJECT_ID,
        topic=RAW_TOPIC,
        subscription=RAW_SUB,
        ack_deadline_secs=60,
        fail_if_exists=False,
    )
    
    # Create processed events topic
    create_processed_topic = PubSubCreateTopicOperator(
        task_id='create_processed_topic',
        project_id=PROJECT_ID,
        topic=PROCESSED_TOPIC,
        fail_if_exists=False,
    )
    
    # Create subscription for processed events
    create_processed_subscription = PubSubCreateSubscriptionOperator(
        task_id='create_processed_subscription',
        project_id=PROJECT_ID,
        topic=PROCESSED_TOPIC,
        subscription=PROCESSED_SUB,
        ack_deadline_secs=60,
        fail_if_exists=False,
    )
    
    # Ensure temp directory exists
    check_temp_dir = PythonOperator(
        task_id='ensure_temp_directory',
        python_callable=ensure_temp_directory,
    )
    
    # Create pipeline script
    create_script = PythonOperator(
        task_id='create_pipeline_script',
        python_callable=create_pipeline_script,
    )
    
    # Run event simulator (for testing)
    run_simulator = BashOperator(
        task_id='run_event_simulator',
        bash_command=f'pip install google-cloud-pubsub && '
                     f'python /home/airflow/gcs/data/streaming/scripts/event_simulator.py '
                     f'--project_id={PROJECT_ID} --topic_name={RAW_TOPIC} --user_count=50 --events_per_user=10 --rate_limit=20',
    )
    
    # Run Dataflow job using gcloud
    start_dataflow_pipeline = BashOperator(
        task_id='start_dataflow_pipeline',
        bash_command=f"""
            pip install google-cloud-dataflow
            gcloud dataflow jobs run {JOB_NAME} \
              --project={PROJECT_ID} \
              --region={REGION} \
              --worker-zone=us-east4-c \
              --worker-machine-type=n1-standard-2 \
              --max-workers=2 \
              --num-workers=1 \
              --staging-location={TEMP_LOCATION}/staging \
              --temp-location={TEMP_LOCATION} \
              --service-account-email=github-actions-deployer@{PROJECT_ID}.iam.gserviceaccount.com \
              --format=json \
              --python-path=/home/airflow/gcs/data/streaming/scripts \
              --runner=DataflowRunner \
              --streaming \
              --setup_file=/dev/null \
              --requirements_file=/dev/null \
              gs://{GCS_BUCKET}/dataflow/scripts/simple_pipeline.py \
              --input_subscription=projects/{PROJECT_ID}/subscriptions/{RAW_SUB} \
              --output_table={PROJECT_ID}.{DATASET_ID}.user_events
        """,
    )
    
    # Set dependencies
    create_raw_topic >> create_raw_subscription
    create_processed_topic >> create_processed_subscription
    [create_raw_subscription, create_processed_subscription] >> check_temp_dir >> create_script >> run_simulator >> start_dataflow_pipeline
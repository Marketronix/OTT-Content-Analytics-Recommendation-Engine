# dags/streaming/streaming_pipeline_dag.py
from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.providers.google.cloud.operators.pubsub import PubSubCreateTopicOperator, PubSubCreateSubscriptionOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowStartFlexTemplateOperator
from airflow.operators.bash import BashOperator
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
    
    # Run event simulator (for testing)
    run_simulator = BashOperator(
        task_id='run_event_simulator',
        bash_command=f'pip install google-cloud-pubsub && '
                     f'python /home/airflow/gcs/data/streaming/scripts/event_simulator.py '
                     f'--project_id={PROJECT_ID} --topic_name={RAW_TOPIC} --user_count=50 --events_per_user=10 --rate_limit=20',
    )
    
    # Deploy the Dataflow pipeline using the manually created template
    start_dataflow_pipeline = DataflowStartFlexTemplateOperator(
        task_id='start_dataflow_pipeline',
        project_id=PROJECT_ID,
        location=REGION,
        body={
            'launchParameter': {
                'jobName': JOB_NAME,
                'containerSpecGcsPath': f"gs://{GCS_BUCKET}/dataflow/templates/event_pipeline.json",
                'parameters': {
                    'input_subscription': f"projects/{PROJECT_ID}/subscriptions/{RAW_SUB}",
                    'output_table': f"{PROJECT_ID}.{DATASET_ID}.user_events",
                    'temp_location': f"{TEMP_LOCATION}"
                },
                'environment': {
                    'numWorkers': 1,
                    'maxWorkers': 2,
                    'machineType': 'n1-standard-2',
                    # 'workerZone': 'us-east4'
                }
            }
        },
    )
    
    # Set dependencies
    create_raw_topic >> create_raw_subscription; create_processed_topic >> create_processed_subscription; [create_raw_subscription, create_processed_subscription] >> run_simulator >> start_dataflow_pipeline

    # create_raw_topic >> create_raw_subscription
    # create_processed_topic >> create_processed_subscription
    # [create_raw_subscription, create_processed_subscription] >> run_simulator >> start_dataflow_pipeline
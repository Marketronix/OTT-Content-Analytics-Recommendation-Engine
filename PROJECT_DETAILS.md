<h1>OTT Content Analytics Pipeline: Detailed Implementation</h1>

<p>This document provides an in-depth explanation of the implementation process, challenges encountered, and solutions developed for each phase of the OTT Content Analytics Pipeline.</p>

<h2>Table of Contents</h2>
<ul>
   <li><a href="#phase-0">Phase 0: Project Setup and Resource Initialization</a></li>
   <li><a href="#phase-1">Phase 1: Data Ingestion</a></li>
   <li><a href="#phase-2">Phase 2: Batch Processing with Dataproc</a></li>
   <li><a href="#phase-3">Phase 3: Exploratory Analysis</a></li>
   <li><a href="#phase-4">Phase 4: Real-Time Event Simulation & Processing</a></li>
   <li><a href="#screenshots">Project Screenshots and Visual Documentation</a></li>
</ul>

<h2 id="phase-0">Phase 0: Project Setup and Resource Initialization</h2>

<h3>GCP Project Creation and API Enablement</h3>

<p>I started by creating a new GCP project dedicated to the OTT analytics pipeline and enabled the necessary APIs:</p>

<pre><code>gcloud services enable bigquery.googleapis.com
gcloud services enable dataproc.googleapis.com
gcloud services enable dataflow.googleapis.com
gcloud services enable pubsub.googleapis.com
gcloud services enable composer.googleapis.com</code></pre>

<h3>Setting Up Cloud Storage Buckets</h3>

<p>I created three separate buckets to maintain a clear data organization strategy:</p>

<pre><code># Created Python script to set up buckets
python create_buckets.py</code></pre>

<p>The script contained:</p>

<pre><code>from google.cloud import storage

def create_buckets():
   client = storage.Client()
   buckets = [
       "raw-data-bucket",
       "processed-data-bucket",
       "archive-data-bucket"
   ]
   
   for bucket_name in buckets:
       bucket = client.bucket(bucket_name)
       bucket.create(location="region-name")
       print(f"Created bucket {bucket_name}")

if __name__ == "__main__":
   create_buckets()</code></pre>

<h3>BigQuery Dataset Creation</h3>

<p>I created a BigQuery dataset to house all tables related to the project:</p>

<pre><code># Created Python script to set up BigQuery dataset
from google.cloud import bigquery

client = bigquery.Client()
dataset_id = "analytics_dataset"
dataset = bigquery.Dataset(f"{client.project}.{dataset_id}")
dataset.location = "region-name"
dataset = client.create_dataset(dataset, exists_ok=True)
print(f"Created dataset {client.project}.{dataset_id}")</code></pre>

<h3>Cloud Composer Environment</h3>

<p>I deployed a Cloud Composer environment to run Apache Airflow for orchestration:</p>

<pre><code>gcloud composer environments create composer-environment-name \
   --location region-name \
   --image-version composer-2.0.0-airflow-2.2.3 \
   --node-count 3 \
   --machine-type n1-standard-2</code></pre>

<h3>CI/CD Pipeline Setup</h3>

<p>I set up a GitHub repository and created a GitHub Actions workflow to automatically deploy DAGs to Cloud Composer whenever changes were made to the DAGs directory.</p>

<p>Key challenges:</p>
<ol>
   <li><strong>Service Account Permissions</strong>: Initially, the GitHub Actions service account lacked permissions to access Cloud Composer. I solved this by granting the appropriate IAM roles.</li>
   <li><strong>Environment Variables</strong>: Managing environment variables securely was challenging. I implemented a solution using GitHub Secrets and a .env file for local development.</li>
</ol>

<h2 id="phase-1">Phase 1: Data Ingestion</h2>

<h3>IMDb Dataset Ingestion Strategy</h3>

<p>IMDb provides data in TSV (Tab-Separated Values) format, compressed with gzip. I created a modular Airflow DAG to handle the ingestion process:</p>

<ol>
   <li><strong>Dataset Change Detection</strong>: I implemented an MD5 hash-based approach to detect changes in datasets.</li>
   <li><strong>Metadata Tracking</strong>: Created a BigQuery table to log all ingestion events with metadata.</li>
</ol>

<h3>Ingestion DAG Implementation</h3>

<p>The DAG handled these steps:</p>
<ol>
   <li>Monitor for new IMDb datasets</li>
   <li>Calculate hash to detect changes</li>
   <li>Upload to Cloud Storage</li>
   <li>Track metadata in BigQuery</li>
</ol>

<p>Challenges faced:</p>
<ol>
   <li><strong>Rate Limiting</strong>: IMDb occasionally rate-limited downloads. I implemented retry mechanisms with exponential backoff.</li>
   <li><strong>Worker Disk Space</strong>: Temporary files filled worker disk space. I resolved this by streaming directly to GCS instead of using temp storage.</li>
</ol>

<p>Key code snippet for change detection:</p>

<pre><code>def check_dataset_changes(datasets, bucket_name, prefix='DATA_PREFIX/', **context):
   """Check if datasets in GCS have changed based on metadata."""
   storage_client = storage.Client()
   bucket = storage_client.bucket(bucket_name)
   
   changes = {}
   
   for dataset in datasets:
       blob_name = f"{prefix}{dataset}"
       blob = bucket.blob(blob_name)
       
       if not blob.exists():
           changes[dataset] = False
           continue
       
       # Get current MD5 hash from GCS metadata
       blob.reload()
       current_md5 = blob.md5_hash
       
       # Get previous MD5 hash from custom metadata if it exists
       previous_md5 = None
       if blob.metadata and 'previous_md5_hash' in blob.metadata:
           previous_md5 = blob.metadata['previous_md5_hash']
       
       # Check if file has changed
       if previous_md5 is None or current_md5 != previous_md5:
           changes[dataset] = True
           
           # Update metadata with current hash
           metadata = blob.metadata or {}
           metadata['previous_md5_hash'] = current_md5
           blob.metadata = metadata
           blob.patch()
       else:
           changes[dataset] = False
   
   # Store results in XCom for later tasks
   context['ti'].xcom_push(key='dataset_changes', value=changes)
   
   return changes</code></pre>

<h2 id="phase-2">Phase 2: Batch Processing with Dataproc</h2>

<h3>Dataproc Architecture Design</h3>

<p>I designed a sequential processing approach using Dataproc with zone fallback for reliability:</p>

<pre><code># Zone fallback mechanism
FALLBACK_ZONES = [
   f"{REGION}-a",
   f"{REGION}-b",
   f"{REGION}-c",
]

# Create cluster tasks with fallback
create_cluster_tasks = []
for index, zone_uri in enumerate(FALLBACK_ZONES):
   create_task = DataprocCreateClusterOperator(
       task_id=f'create_dataproc_cluster_zone_{index+1}',
       project_id=PROJECT_ID,
       cluster_config=get_cluster_config(zone_uri),
       region=REGION,
       cluster_name="processing-cluster-name",
       gcp_conn_id='google_cloud_default',
       trigger_rule=TriggerRule.ALL_FAILED if index > 0 else TriggerRule.ALL_SUCCESS,
   )
   create_cluster_tasks.append(create_task)</code></pre>

<h3>PySpark Transformation Logic</h3>

<p>Each IMDb dataset required specific transformations:</p>

<ol>
   <li><strong>Data Type Conversion</strong>: Converting string representations to appropriate types.</li>
   <li><strong>Array Handling</strong>: Converting pipe-delimited strings to arrays.</li>
   <li><strong>Null Value Handling</strong>: Replacing "\N" markers with proper nulls.</li>
</ol>

<p>Example for title_basics:</p>

<pre><code>transformed_df = df.withColumn(
   "isAdult", 
   F.when(F.col("isAdult") == "1", True)
    .when(F.col("isAdult") == "0", False)
    .otherwise(None)
   .cast(BooleanType())
).withColumn(
   "startYear", 
   F.col("startYear").cast(StringType())
).withColumn(
   "endYear", 
   F.col("endYear").cast(StringType())
).withColumn(
   "runtimeMinutes", 
   F.col("runtimeMinutes").cast(IntegerType())
).withColumn(
   "genres", 
   F.when(
       F.col("genres").isNull(), 
       F.array()
   ).otherwise(
       F.split(F.col("genres"), ",")
   )
)</code></pre>

<h3>Challenges and Solutions</h3>

<ol>
   <li><strong>Resource Exhaustion</strong>: GCP zones frequently ran out of resources for Dataproc clusters. I implemented a zone fallback mechanism that tries alternative zones if the primary zone is unavailable.</li>
   <li><strong>Array Type Handling</strong>: BigQuery's REPEATED fields didn't automatically map from Spark arrays. I resolved this by creating auxiliary tables and SQL views.</li>
   <li><strong>Performance Optimization</strong>: Large datasets caused performance issues. I implemented:
       <ul>
           <li>Sequential processing of tables (smaller tables first)</li>
           <li>Optimized Spark configuration (executor memory and cores)</li>
           <li>Table partitioning and clustering in BigQuery</li>
       </ul>
   </li>
</ol>

<h2 id="phase-3">Phase 3: Exploratory Analysis</h2>

<p>I created a set of analytical views in BigQuery to facilitate data exploration:</p>

<h3>Content Performance Analysis:</h3>
<pre><code>CREATE OR REPLACE VIEW `dataset_name.content_performance` AS
SELECT
 t.tconst,
 t.primaryTitle,
 t.titleType,
 t.startYear,
 r.averageRating,
 r.numVotes,
 t.genres
FROM
 `dataset_name.title_basics` t
LEFT JOIN
 `dataset_name.title_ratings` r
ON
 t.tconst = r.tconst</code></pre>

<h3>Data Quality Monitoring:</h3>
<pre><code>CREATE OR REPLACE VIEW `dataset_name.data_quality_metrics` AS
SELECT
 'title_basics' AS table_name,
 COUNT(*) AS row_count,
 COUNTIF(primaryTitle IS NULL) AS missing_titles,
 COUNTIF(ARRAY_LENGTH(genres) = 0) AS missing_genres
FROM
 `dataset_name.title_basics`
UNION ALL
SELECT
 'title_ratings' AS table_name,
 COUNT(*) AS row_count,
 0 AS missing_titles,
 0 AS missing_genres
FROM
 `dataset_name.title_ratings`
-- Additional tables...</code></pre>

<h2 id="phase-4">Phase 4: Real-Time Event Simulation & Processing</h2>

<p>This phase involved creating a complete streaming data pipeline using Pub/Sub, Dataflow, and BigQuery.</p>

<h3>Event Schema Design</h3>

<p>I designed a comprehensive event schema to capture all relevant user interaction data:</p>

<pre><code>{
 "event_id": "string",
 "user_id": "string",
 "content_id": "string",
 "event_type": "string",
 "timestamp": "timestamp",
 "session_id": "string",
 "position": "integer",
 "device": {
   "type": "string",
   "os": "string",
   "browser": "string",
   "model": "string"
 },
 "location": {
   "country": "string",
   "region": "string",
   "city": "string"
 },
 "quality": {
   "resolution": "string",
   "bitrate": "integer"
 },
 "rating": "integer"
}</code></pre>

<h3>Pub/Sub Setup</h3>

<p>I created Pub/Sub topics and subscriptions for the event pipeline:</p>

<pre><code># Create topics
gcloud pubsub topics create event-topic-name
gcloud pubsub topics create processed-event-topic-name

# Create subscriptions
gcloud pubsub subscriptions create event-subscription-name --topic=event-topic-name
gcloud pubsub subscriptions create processed-event-sub-name --topic=processed-event-topic-name</code></pre>

<h3>Docker Image for Dataflow</h3>

<p>Creating the Docker image for Dataflow was particularly challenging. After several iterations, I successfully used the official Dataflow template base image:</p>

<pre><code>FROM gcr.io/dataflow-templates-base/python39-template-launcher-base:latest

ARG WORKDIR=/dataflow/template
RUN mkdir -p ${WORKDIR}
WORKDIR ${WORKDIR}

COPY event_pipeline.py .

RUN pip install --no-cache-dir \
   google-cloud-pubsub==2.13.6 \
   google-cloud-bigquery==2.34.4 \
   apache-beam[gcp]==2.42.0

ENV FLEX_TEMPLATE_PYTHON_PY_FILE="${WORKDIR}/event_pipeline.py"
ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE="${WORKDIR}/requirements.txt"

RUN echo "apache-beam[gcp]==2.42.0" > requirements.txt && \
   echo "google-cloud-pubsub==2.13.6" >> requirements.txt && \
   echo "google-cloud-bigquery==2.34.4" >> requirements.txt</code></pre>

<h3>Dataflow Flex Template Creation</h3>

<p>I encountered several challenges when creating the Dataflow Flex Template. The most significant was dealing with template metadata format issues. After multiple attempts, I succeeded with this approach:</p>

<pre><code>gcloud dataflow flex-template build \
 gs://bucket-name/dataflow/templates/event_pipeline.json \
 --image-gcr-path gcr.io/project-id/pipeline-image-name:latest \
 --sdk-language "PYTHON" \
 --flex-template-base-image "PYTHON3" \
 --metadata-file template_spec.json \
 --py-path event_pipeline.py \
 --env "FLEX_TEMPLATE_PYTHON_PY_FILE=event_pipeline.py"</code></pre>

<p>The template specification was critical:</p>

<pre><code>{
 "name": "OTT Event Pipeline",
 "description": "Process OTT streaming events from Pub/Sub to BigQuery",
 "parameters": [
   {
     "name": "input_subscription",
     "label": "Input Subscription",
     "helpText": "Full Pub/Sub subscription path",
     "paramType": "TEXT",
     "isOptional": false
   },
   {
     "name": "output_table",
     "label": "Output Table",
     "helpText": "BigQuery table to write events to",
     "paramType": "TEXT",
     "isOptional": false
   },
   {
     "name": "temp_location",
     "label": "Temp Location",
     "helpText": "GCS location for temporary files",
     "paramType": "TEXT",
     "isOptional": false
   }
 ]
}</code></pre>

<h3>Apache Beam Pipeline</h3>

<p>The event processing pipeline was implemented using Apache Beam:</p>

<pre><code>def run(argv=None):
   parser = argparse.ArgumentParser(description='Process streaming events')
   parser.add_argument('--input_subscription', required=True, 
                     help='Pub/Sub subscription to read from')
   parser.add_argument('--output_table', required=True, 
                     help='BigQuery table to write to')
   parser.add_argument('--temp_location', required=True, 
                     help='GCS location for temporary files')
   
   known_args, pipeline_args = parser.parse_known_args(argv)
   
   pipeline_options = PipelineOptions(pipeline_args)
   pipeline_options.view_as(StandardOptions).streaming = True
   
   with beam.Pipeline(options=pipeline_options) as p:
       events = (
           p 
           | 'Read from PubSub' >> ReadFromPubSub(
               subscription=known_args.input_subscription)
           | 'Parse JSON' >> beam.ParDo(ParseJsonDoFn())
           | 'Write to BigQuery' >> WriteToBigQuery(
               known_args.output_table,
               schema='event_id:STRING,user_id:STRING,content_id:STRING,event_type:STRING',
               create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
               write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
           )
       )</code></pre>

<h3>Event Simulator</h3>

<p>I created an event simulator to generate realistic user events for testing:</p>

<pre><code>def generate_event(user_id, content_id, event_type=None):
   """Generate a single event."""
   if not event_type:
       event_type = random.choice(["start", "pause", "resume", "complete", "seek", "rate"])
       
   event = {
       "event_id": f"evt_{uuid.uuid4().hex}",
       "user_id": user_id,
       "content_id": content_id,
       "event_type": event_type,
       "timestamp": datetime.datetime.now().isoformat(),
       "session_id": f"session_{uuid.uuid4().hex[:10]}",
       "position": random.randint(0, 7200) if event_type != "start" else 0,
       "device": {
           "type": random.choice(["mobile", "tv", "web", "tablet"]),
           "os": random.choice(["iOS", "Android", "Windows", "macOS", "tvOS"]),
           "browser": random.choice(["Chrome", "Safari", "Firefox", "Edge", "app"]),
       },
       "location": {
           "country": random.choice(["US", "GB", "CA", "AU", "DE", "FR", "IN"]),
           "region": f"Region_{random.randint(1, 50)}",
           "city": f"City_{random.randint(1, 100)}"
       },
       "quality": {
           "resolution": random.choice(["SD", "HD", "4K"]),
           "bitrate": random.randint(800, 15000)
       }
   }
   
   if event_type == "rate":
       event["rating"] = random.randint(1, 5)
       
   return event</code></pre>

<h3>Airflow DAG for Streaming Pipeline</h3>

<p>I created a dedicated Airflow DAG to orchestrate the streaming pipeline:</p>

<pre><code>with DAG(
   'streaming_pipeline_dag_name',
   default_args=default_args,
   description='Streaming events processing pipeline',
   schedule_interval=None,
   start_date=datetime(2025, 6, 1),
   catchup=False,
   tags=['streaming', 'events'],
) as dag:
   
   # Create Pub/Sub infrastructure
   create_raw_topic = PubSubCreateTopicOperator(...)
   create_raw_subscription = PubSubCreateSubscriptionOperator(...)
   
   # Generate test events
   run_simulator = BashOperator(
       task_id='run_event_simulator',
       bash_command=f'python /home/airflow/gcs/data/streaming/scripts/event_simulator.py ...',
   )
   
   # Start Dataflow job
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
                   'temp_location': TEMP_LOCATION
               },
               'environment': {
                   'numWorkers': 1,
                   'maxWorkers': 2,
                   'machineType': 'n1-standard-2',
                   'workerZone': 'us-east4-c'
               }
           }
       },
   )</code></pre>

<h3>Major Challenges and Resolutions</h3>

<h4>1. Template Metadata Format Issues</h4>

<p>I encountered persistent errors with the Dataflow Flex Template creation:</p>
<div class="error-message">ERROR: gcloud crashed (ValueError): Invalid template metadata. Name field is empty. Template Metadata: &lt;TemplateMetadata parameters: []&gt;</div>

<p>After multiple attempts with different formats, I discovered that the template spec JSON needed very specific formatting and field names. I resolved this by:</p>
<ul>
   <li>Creating the template spec file with exactly the right format</li>
   <li>Using the command with correct flags and parameters</li>
   <li>Manually creating the template instead of automating it through Cloud Build</li>
</ul>

<h4>2. Resource Availability Issues</h4>

<p>When running Dataflow jobs, I encountered zone resource exhaustion:</p>
<div class="error-message">ZONE_RESOURCE_POOL_EXHAUSTED: The zone 'projects/project-id/zones/region-zone' does not have enough resources available to fulfill the request.</div>

<p>I resolved this by specifying alternative zones and reducing resource requirements:</p>
<pre><code>'environment': {
   'numWorkers': 1,
   'maxWorkers': 2,
   'machineType': 'n1-standard-2',
   'workerZone': 'region-zone-c'
}</code></pre>

<h4>3. Docker Image Configuration</h4>

<p>Getting the Docker image right for Dataflow was challenging. I resolved this by:</p>
<ul>
   <li>Using the official base image: <code>gcr.io/dataflow-templates-base/python39-template-launcher-base</code></li>
   <li>Properly setting environment variables for the launcher</li>
   <li>Managing dependencies carefully to avoid conflicts</li>
</ul>

<h4>4. CI/CD Pipeline Permissions</h4>

<p>My GitHub Actions workflow initially lacked permissions for Cloud Build:</p>
<div class="error-message">ERROR: caller does not have permission to act as service account...</div>

<p>I fixed this by granting the appropriate IAM roles to the service account.</p>

<h3>End-to-End Testing</h3>

<p>Once all components were in place, I tested the full pipeline:</p>

<ol>
   <li>Triggered the streaming DAG</li>
   <li>Verified that Pub/Sub topics and subscriptions were created</li>
   <li>Confirmed the event simulator was generating events</li>
   <li>Validated that the Dataflow job was processing events</li>
   <li>Checked that events were appearing in BigQuery</li>
</ol>

<p>The pipeline successfully processed events end-to-end, demonstrating the viability of the architecture.</p>

<h2 id="screenshots">Project Screenshots and Visual Documentation</h2>

<p>To provide visual documentation of the project's implementation, I have captured screenshots showing the successful execution of various components. These screenshots are available in the <code>screenshots</code> folder within the repository and include:</p>

<h3>Airflow DAGs</h3>
<ul>
   <li><strong>Ingestion DAG Graph</strong>: Visual representation of the IMDb data ingestion workflow showing all tasks and their dependencies.</li>
   <li><strong>Batch Processing DAG Graph</strong>: Diagram of the Dataproc-based transformation pipeline, including zone fallback mechanism.</li>
   <li><strong>Streaming Pipeline DAG Graph</strong>: Visualization of the real-time event processing workflow.</li>
   <li><strong>Successful DAG Runs</strong>: Screenshots showing successful execution of all three DAGs.</li>
</ul>

<h3>Data Processing Results</h3>
<ul>
   <li><strong>BigQuery Streaming Events</strong>: Evidence of real-time events being successfully written to BigQuery.</li>
</ul>

<h3>Infrastructure Components</h3>
<ul>
   <li><strong>Dataflow Job Graph</strong>: Visual representation of the streaming pipeline execution in Dataflow.</li>
</ul>

<p>These screenshots serve as visual proof of the successful implementation and execution of each phase of the project. They demonstrate that the architecture functions as designed, with data flowing correctly through each component of the pipeline.</p>

<h2>Conclusion</h2>

<p>This project demonstrates a comprehensive approach to building a scalable, robust data pipeline for OTT analytics using GCP services. The modular design allows for easy extension to include recommendation systems and additional analytics capabilities in the future.</p>

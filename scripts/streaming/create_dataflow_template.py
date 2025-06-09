# scripts/streaming/create_dataflow_template.py
import argparse
import json
import os
import subprocess
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def create_dataflow_template(project_id, region, template_path, python_file_path, image_name):
    """Create a Dataflow flex template."""
    # Create spec file
    spec = {
        "image": f"gcr.io/{project_id}/{image_name}:latest",
        "sdk_info": {
            "language": "PYTHON"
        },
        "metadata": {
            "name": "OTT Event Processing Pipeline",
            "description": "Process OTT streaming events from Pub/Sub to BigQuery",
            "parameters": [
                {
                    "name": "input_subscription",
                    "label": "Input PubSub Subscription",
                    "helpText": "PubSub subscription to read from",
                    "regexes": ["^[a-zA-Z0-9-_.]+$"]
                },
                {
                    "name": "output_table",
                    "label": "Output BigQuery Table",
                    "helpText": "BigQuery table to write to (PROJECT:DATASET.TABLE)",
                    "regexes": ["^[a-zA-Z0-9-_]+:[a-zA-Z0-9-_]+.[a-zA-Z0-9-_]+$"]
                },
                {
                    "name": "temp_location",
                    "label": "Temp Location",
                    "helpText": "GCS location for temporary files",
                    "regexes": ["^gs://[a-zA-Z0-9-_./]+$"]
                }
            ]
        }
    }
    
    # Write spec to file
    temp_spec_file = "/tmp/event_pipeline_spec.json"
    with open(temp_spec_file, "w") as f:
        json.dump(spec, f)
    
    # Build the Docker image
    print(f"Building Docker image: {image_name}")
    build_cmd = [
        "gcloud", "builds", "submit",
        "--project", project_id,
        "--region", region,
        "--tag", f"gcr.io/{project_id}/{image_name}:latest",
        os.path.dirname(python_file_path)
    ]
    subprocess.run(build_cmd, check=True)
    
    # Create the template
    print(f"Creating Dataflow flex template: {template_path}")
    template_cmd = [
        "gcloud", "dataflow", "flex-template", "build",
        template_path,
        "--project", project_id,
        "--region", region,
        "--image", f"gcr.io/{project_id}/{image_name}:latest",
        "--sdk-language", "PYTHON",
        "--metadata-file", temp_spec_file
    ]
    subprocess.run(template_cmd, check=True)
    
    print(f"Dataflow flex template created at: {template_path}")

def parse_arguments():
    parser = argparse.ArgumentParser(description="Create Dataflow flex template")
    parser.add_argument("--project_id", required=False, default=os.getenv('GCP_PROJECT_ID'),
                        help="Google Cloud Project ID")
    parser.add_argument("--region", required=False, default=os.getenv('GCP_REGION'),
                        help="Google Cloud Region")
    parser.add_argument("--bucket", required=False, default=os.getenv('PROCESSED_DATA_BUCKET'),
                        help="GCS bucket for templates")
    parser.add_argument("--file", required=False, default="event_pipeline.py",
                        help="Python file path")
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_arguments()
    template_path = f"gs://{args.bucket}/dataflow/templates/event_pipeline.json"
    image_name = "ott-event-pipeline"
    python_file_path = os.path.abspath(args.file)
    
    create_dataflow_template(args.project_id, args.region, template_path, python_file_path, image_name)
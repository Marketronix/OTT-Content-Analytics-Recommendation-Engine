# scripts/deploy_dags_to_composer.py
import os
from google.cloud import storage
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def deploy_dags_to_composer():
    """Deploy DAGs and supporting files to Cloud Composer environment."""
    project_id = os.getenv('GCP_PROJECT_ID')
    composer_bucket = os.getenv('GCP_COMPOSER_BUCKET')
    
    if not project_id or not composer_bucket:
        raise ValueError("Missing required environment variables")
    
    # Initialize the GCS client
    storage_client = storage.Client(project=project_id)
    bucket = storage_client.bucket(composer_bucket)
    
    # Paths to deploy
    paths_to_deploy = {
        'dags': 'dags',
        'scripts/streaming': 'data/streaming/scripts',
        'scripts/setup': 'data/setup'
    }
    
    for local_path, gcs_path in paths_to_deploy.items():
        if not os.path.exists(local_path):
            print(f"Warning: {local_path} does not exist, skipping")
            continue
            
        print(f"Deploying {local_path} to {gcs_path}")
        deploy_directory(bucket, local_path, gcs_path)
    
    print("Deployment complete!")

def deploy_directory(bucket, local_dir, gcs_dir):
    """Upload a directory to GCS recursively."""
    for root, _, files in os.walk(local_dir):
        for file in files:
            if file.endswith('.py') or file == 'Dockerfile':
                local_file_path = os.path.join(root, file)
                
                # Determine the relative path from the local directory
                relative_path = os.path.relpath(local_file_path, local_dir)
                
                # Create the destination blob name
                blob_name = os.path.join(gcs_dir, relative_path)
                
                # Upload the file
                blob = bucket.blob(blob_name)
                blob.upload_from_filename(local_file_path)
                print(f"Uploaded {local_file_path} to {blob_name}")

if __name__ == '__main__':
    deploy_dags_to_composer()
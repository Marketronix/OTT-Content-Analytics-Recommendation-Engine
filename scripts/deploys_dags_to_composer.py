# scripts/deploy_dags_to_composer.py
import os
from google.cloud import storage

def deploy_dags_to_composer():
    """Deploy DAGs to the Cloud Composer environment."""
    project_id = os.environ.get('GCP_PROJECT_ID')
    composer_bucket = os.environ.get('GCP_COMPOSER_BUCKET')
    
    if not project_id or not composer_bucket:
        raise ValueError("Missing required environment variables")
    
    # Path to the dags folder in Cloud Composer
    dags_folder = 'dags'
    
    # Initialize the GCS client
    storage_client = storage.Client(project=project_id)
    bucket = storage_client.bucket(composer_bucket)
    
    # Local path to the dags directory
    local_dags_path = 'dags'
    
    # Upload all DAG files
    for root, _, files in os.walk(local_dags_path):
        for file in files:
            if file.endswith('.py'):
                local_file_path = os.path.join(root, file)
                
                # Determine the relative path from the dags directory
                relative_path = os.path.relpath(local_file_path, local_dags_path)
                
                # Create the destination blob name
                blob_name = os.path.join(dags_folder, relative_path)
                
                # Upload the file
                blob = bucket.blob(blob_name)
                blob.upload_from_filename(local_file_path)
                print(f"Uploaded {local_file_path} to {blob_name}")

if __name__ == '__main__':
    deploy_dags_to_composer()
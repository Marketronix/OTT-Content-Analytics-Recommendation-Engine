# scripts/upload_pyspark_scripts.py
import os
from google.cloud import storage
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def upload_pyspark_scripts(local_dir, bucket_name, prefix):
    """Upload PySpark scripts to GCS."""
    storage_client = storage.Client(project=os.getenv('GCP_PROJECT_ID'))
    bucket = storage_client.bucket(bucket_name)
    
    # List all Python files in the directory
    script_files = [f for f in os.listdir(local_dir) if f.endswith('.py')]
    
    if not script_files:
        print(f"No Python files found in {local_dir}")
        return
    
    print(f"Found {len(script_files)} PySpark scripts to upload:")
    for script in script_files:
        print(f"  - {script}")
    
    # Upload each script
    for script in script_files:
        local_path = os.path.join(local_dir, script)
        destination_blob_name = f"{prefix}/{script}"
        
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(local_path)
        
        print(f"  Uploaded {script} to gs://{bucket_name}/{destination_blob_name}")

if __name__ == "__main__":
    # Get configuration from environment variables
    SCRIPTS_DIR = os.getenv('PYSPARK_SCRIPTS_DIR')
    BUCKET_NAME = os.getenv('PROCESSED_DATA_BUCKET')
    PREFIX = 'pyspark_scripts'
    
    upload_pyspark_scripts(SCRIPTS_DIR, BUCKET_NAME, PREFIX)
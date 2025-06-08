# upload_to_gcs.py
import os
import glob
import time
from google.cloud import storage
from dotenv import load_dotenv
from tqdm import tqdm

# Load environment variables from .env file
load_dotenv()

def upload_local_files_to_gcs(local_directory, bucket_name, gcs_prefix):
    """
    Upload files from a local directory to Google Cloud Storage with progress tracking.
    
    Args:
        local_directory (str): Path to the local directory containing files
        bucket_name (str): Name of the GCS bucket
        gcs_prefix (str): Prefix (folder) in the bucket to upload files to
    """
    # Initialize the GCS client
    storage_client = storage.Client(project=os.getenv('GCP_PROJECT_ID'))
    bucket = storage_client.bucket(bucket_name)
    
    # Find all .tsv.gz files in the directory
    file_pattern = os.path.join(local_directory, "*.tsv.gz")
    files = glob.glob(file_pattern)
    
    if not files:
        print(f"No .tsv.gz files found in {local_directory}")
        return
    
    print(f"Found {len(files)} files to upload:")
    for file_path in files:
        print(f"  - {os.path.basename(file_path)}")
    
    # Upload each file with progress tracking
    for file_path in files:
        file_name = os.path.basename(file_path)
        destination_blob_name = f"{gcs_prefix}/{file_name}"
        
        file_size = os.path.getsize(file_path)
        print(f"Uploading {file_name} ({file_size / (1024 * 1024):.2f} MB) to {destination_blob_name}...")
        
        # Create a blob with custom chunk size for better performance
        blob = bucket.blob(destination_blob_name, chunk_size=262144 * 4)  # 1MB chunks
        
        # Upload with progress tracking
        start_time = time.time()
        
        # Define a callback for upload progress
        with tqdm(total=file_size, unit='B', unit_scale=True, desc=file_name) as progress_bar:
            def _upload_callback(bytes_transferred):
                progress_bar.update(bytes_transferred - progress_bar.n)
            
            # Upload the file with the callback
            blob.upload_from_filename(file_path, content_type='application/gzip', 
                                      timeout=600)  # 10-minute timeout
        
        elapsed_time = time.time() - start_time
        upload_speed = file_size / elapsed_time / (1024 * 1024) if elapsed_time > 0 else 0
        print(f"  Uploaded {file_name} in {elapsed_time:.1f} seconds ({upload_speed:.2f} MB/s)")
    
    print("All files uploaded successfully!")

if __name__ == "__main__":
    # Get configuration from environment variables
    LOCAL_DIR = os.getenv('LOCAL_IMDB_DIR')  # Directory with IMDb files
    BUCKET_NAME = os.getenv('RAW_DATA_BUCKET')  # GCS bucket name
    GCS_PREFIX = os.getenv('IMDB_PREFIX')  # Folder in the bucket
    
    print(f"Using configuration:")
    print(f"  Project ID: {os.getenv('GCP_PROJECT_ID')}")
    print(f"  Local directory: {LOCAL_DIR}")
    print(f"  Bucket name: {BUCKET_NAME}")
    print(f"  GCS prefix: {GCS_PREFIX}")
    
    upload_local_files_to_gcs(LOCAL_DIR, BUCKET_NAME, GCS_PREFIX)
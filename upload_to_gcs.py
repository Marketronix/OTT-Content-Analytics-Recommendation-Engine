# upload_tsv_to_gcs.py
import os
import glob
from google.cloud import storage
from dotenv import load_dotenv
from tqdm import tqdm

# Load environment variables
load_dotenv()

def upload_tsv_files_to_gcs(local_directory, bucket_name, gcs_prefix):
    """Upload TSV files to GCS bucket."""
    storage_client = storage.Client(project=os.getenv('GCP_PROJECT_ID'))
    bucket = storage_client.bucket(bucket_name)
    
    # Find all .tsv files
    file_pattern = os.path.join(local_directory, "*.tsv")
    files = glob.glob(file_pattern)
    
    if not files:
        print(f"No .tsv files found in {local_directory}")
        return
    
    print(f"Found {len(files)} files to upload:")
    for file_path in files:
        print(f"  - {os.path.basename(file_path)}")
    
    for file_path in files:
        file_name = os.path.basename(file_path)
        destination_blob_name = f"{gcs_prefix}/{file_name}"
        
        file_size = os.path.getsize(file_path)
        print(f"Uploading {file_name} ({file_size / (1024 * 1024):.2f} MB) to {destination_blob_name}...")
        
        # Create blob with appropriate chunk size
        blob = bucket.blob(destination_blob_name, chunk_size=262144 * 4)  # 1MB chunks
        
        # Upload with progress tracking
        with tqdm(total=file_size, unit='B', unit_scale=True, desc=file_name) as progress_bar:
            blob.upload_from_filename(file_path, content_type='text/tab-separated-values',
                                     timeout=1800)  # 30-minute timeout
            progress_bar.update(file_size)
        
        print(f"  Uploaded {file_name} to {destination_blob_name}")
    
    print("All files uploaded successfully!")

if __name__ == "__main__":
    # Get configuration from environment variables
    LOCAL_DIR = os.getenv('LOCAL_IMDB_DIR')  # Directory with IMDb files
    BUCKET_NAME = os.getenv('RAW_DATA_BUCKET')  # GCS bucket name
    GCS_PREFIX = os.getenv('IMDB_PREFIX')  # Folder in the bucket
    
    upload_tsv_files_to_gcs(LOCAL_DIR, BUCKET_NAME, GCS_PREFIX)
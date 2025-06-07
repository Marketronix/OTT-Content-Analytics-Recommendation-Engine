# dags/ingestion/helpers.py
import os
import requests
import gzip
import shutil
import hashlib
import pandas as pd
from google.cloud import storage
from typing import List, Dict, Any
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def download_to_gcs(datasets: List[str], bucket_name: str, prefix: str = 'imdb/') -> List[str]:
    """
    Download IMDb datasets directly to GCS bucket.
    
    Args:
        datasets: List of dataset filenames to download
        bucket_name: GCS bucket name
        prefix: Prefix for objects in the bucket
        
    Returns:
        List of GCS paths where files were saved
    """
    BASE_URL = 'https://datasets.imdbws.com/'
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    
    uploaded_blobs = []
    for dataset in datasets:
        url = BASE_URL + dataset
        destination_blob_name = f"{prefix}{dataset}"
        blob = bucket.blob(destination_blob_name)
        
        # Check if blob already exists
        if blob.exists():
            print(f"File {destination_blob_name} already exists in bucket {bucket_name}")
            uploaded_blobs.append(destination_blob_name)
            continue
        
        print(f"Downloading {dataset} to GCS...")
        
        # Stream the download directly to GCS
        response = requests.get(url, stream=True)
        
        # Create a temporary file for streaming
        with open(f"/tmp/{dataset}", 'wb') as temp_file:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    temp_file.write(chunk)
        
        # Upload the temp file to GCS
        blob.upload_from_filename(f"/tmp/{dataset}")
        
        # Clean up the temp file
        os.remove(f"/tmp/{dataset}")
        
        print(f"Downloaded and uploaded {dataset} to {destination_blob_name}")
        uploaded_blobs.append(destination_blob_name)
    
    return uploaded_blobs

def calculate_file_hash(file_path: str) -> str:
    """Calculate MD5 hash of a file."""
    md5_hash = hashlib.md5()
    with open(file_path, 'rb') as f:
        # Read in chunks for large files
        for chunk in iter(lambda: f.read(4096), b''):
            md5_hash.update(chunk)
    return md5_hash.hexdigest()

def verify_downloads(datasets: List[str], download_path: str) -> bool:
    """Verify that downloaded files are valid gzip files."""
    for dataset in datasets:
        local_path = os.path.join(download_path, dataset)
        try:
            with gzip.open(local_path, 'rt', encoding='utf-8') as f:
                # Read header and first line to verify file is valid
                header = next(f)
                first_line = next(f)
            print(f"Verified {dataset} is valid")
        except Exception as e:
            raise ValueError(f"File {dataset} is invalid or corrupt: {str(e)}")
    return True

def get_file_metadata_from_gcs(bucket_name: str, blob_name: str) -> Dict[str, Any]:
    """Get metadata for a file in GCS."""
    storage_client = storage.Client(project=os.getenv('GCP_PROJECT_ID'))
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    
    if not blob.exists():
        return None
    
    return blob.metadata

def check_dataset_changes(datasets: List[str], bucket_name: str, prefix: str = 'imdb/', **context) -> Dict[str, bool]:
    """
    Check if datasets in GCS have changed based on metadata.
    
    Args:
        datasets: List of dataset filenames
        bucket_name: GCS bucket name
        prefix: Prefix for objects in the bucket
        
    Returns:
        Dictionary with dataset names as keys and boolean change status as values
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    
    changes = {}
    
    for dataset in datasets:
        blob_name = f"{prefix}{dataset}"
        blob = bucket.blob(blob_name)
        
        if not blob.exists():
            changes[dataset] = True
            continue
        
        # Get current MD5 hash from GCS metadata
        blob.reload()  # Ensure we have the latest metadata
        current_md5 = blob.md5_hash
        
        # Get previous MD5 hash from custom metadata if it exists
        previous_md5 = None
        if blob.metadata and 'previous_md5_hash' in blob.metadata:
            previous_md5 = blob.metadata['previous_md5_hash']
        
        # Check if file has changed
        if previous_md5 is None or current_md5 != previous_md5:
            changes[dataset] = True
            
            # Update metadata with current hash
            if not blob.metadata:
                blob.metadata = {}
            blob.metadata['previous_md5_hash'] = current_md5
            blob.patch()
        else:
            changes[dataset] = False
    
    # Store results in XCom for later tasks
    context['ti'].xcom_push(key='dataset_changes', value=changes)
    
    return changes

def count_records_in_gz_tsv(file_path: str) -> int:
    """Count the number of records in a gzipped TSV file."""
    count = 0
    with gzip.open(file_path, 'rt', encoding='utf-8') as f:
        # Skip header
        next(f)
        for _ in f:
            count += 1
    return count

def extract_metadata(datasets: List[str], bucket_name: str, prefix: str = 'imdb/', **context) -> List[Dict[str, Any]]:
    """
    Extract metadata from datasets in GCS.
    
    Args:
        datasets: List of dataset filenames
        bucket_name: GCS bucket name
        prefix: Prefix for objects in the bucket
        
    Returns:
        List of dictionaries with metadata for each dataset
    """
    # Get change status from previous task
    changes = context['ti'].xcom_pull(task_ids='check_dataset_changes', key='dataset_changes')
    
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    
    metadata_list = []
    
    for dataset in datasets:
        blob_name = f"{prefix}{dataset}"
        blob = bucket.blob(blob_name)
        
        if not blob.exists():
            print(f"Warning: Blob {blob_name} does not exist in bucket {bucket_name}")
            continue
        
        # Get file size and md5 hash
        blob.reload()
        file_size = blob.size
        file_hash = blob.md5_hash
        
        # Count records if needed (this requires downloading the file temporarily)
        record_count = 0
        if changes.get(dataset, True):  # Default to True if not in changes dict
            # Download to temp file
            temp_file = f"/tmp/{dataset}_temp"
            blob.download_to_filename(temp_file)
            
            # Count records
            record_count = count_records_in_gz_tsv(temp_file)
            
            # Clean up
            os.remove(temp_file)
        
        metadata = {
            'name': dataset,
            'size': file_size,
            'hash': file_hash,
            'records': record_count,
            'changed': changes.get(dataset, True)
        }
        
        metadata_list.append(metadata)
    
    return metadata_list
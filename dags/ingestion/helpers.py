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

def download_imdb_datasets(datasets: List[str], download_path: str) -> List[str]:
    """
    Download IMDb datasets from the official source with resume capability.
    
    Args:
        datasets: List of dataset filenames to download
        download_path: Local directory to save downloaded files
        
    Returns:
        List of paths to downloaded files
    """
    BASE_URL = 'https://datasets.imdbws.com/'
    
    # Create download directory if it doesn't exist
    os.makedirs(download_path, exist_ok=True)
    
    downloaded_files = []
    for dataset in datasets:
        url = BASE_URL + dataset
        local_path = os.path.join(download_path, dataset)
        
        # Check if file already exists and is complete
        if os.path.exists(local_path):
            try:
                # Try to open and read the file to verify it's not corrupted
                with gzip.open(local_path, 'rt', encoding='utf-8') as f:
                    # Read a small portion to verify it's a valid gzip file
                    f.read(1024)
                print(f"File {dataset} already exists and appears valid, skipping download")
                downloaded_files.append(local_path)
                continue
            except Exception as e:
                print(f"Existing file {dataset} appears corrupt, re-downloading: {str(e)}")
                # If verification fails, we'll re-download
        
        print(f"Downloading {dataset}...")
        response = requests.get(url, stream=True)
        with open(local_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        downloaded_files.append(local_path)
        print(f"Downloaded {dataset} to {local_path}")
    
    return downloaded_files

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

def check_dataset_changes(datasets: List[str], download_path: str, bucket_name: str, **context) -> Dict[str, bool]:
    """
    Check if downloaded datasets have changed compared to versions in GCS.
    
    Args:
        datasets: List of dataset filenames
        download_path: Path to downloaded files
        bucket_name: GCS bucket name
        
    Returns:
        Dictionary with dataset names as keys and boolean change status as values
    """
    changes = {}
    
    for dataset in datasets:
        local_path = os.path.join(download_path, dataset)
        blob_name = f'imdb/{dataset}'
        
        # Calculate hash of downloaded file
        current_hash = calculate_file_hash(local_path)
        
        # Get metadata from GCS
        metadata = get_file_metadata_from_gcs(bucket_name, blob_name)
        
        # Check if file has changed
        if metadata is None or 'md5_hash' not in metadata or metadata['md5_hash'] != current_hash:
            changes[dataset] = True
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

def extract_metadata(datasets: List[str], download_path: str, **context) -> List[Dict[str, Any]]:
    """
    Extract metadata from downloaded datasets.
    
    Args:
        datasets: List of dataset filenames
        download_path: Path to downloaded files
        
    Returns:
        List of dictionaries with metadata for each dataset
    """
    # Get change status from previous task
    changes = context['ti'].xcom_pull(task_ids='check_dataset_changes', key='dataset_changes')
    
    metadata_list = []
    
    for dataset in datasets:
        local_path = os.path.join(download_path, dataset)
        
        # Get file size
        file_size = os.path.getsize(local_path)
        
        # Calculate hash
        file_hash = calculate_file_hash(local_path)
        
        # Count records (only if file has changed to save processing time)
        record_count = 0
        if changes.get(dataset, True):  # Default to True if not in changes dict
            record_count = count_records_in_gz_tsv(local_path)
        
        metadata = {
            'name': dataset,
            'size': file_size,
            'hash': file_hash,
            'records': record_count,
            'changed': changes.get(dataset, True)
        }
        
        metadata_list.append(metadata)
    
    return metadata_list
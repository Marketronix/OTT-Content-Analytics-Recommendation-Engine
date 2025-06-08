# dags/ingestion/helpers.py
import os
import io
from typing import List, Dict, Any
from google.cloud import storage

def check_dataset_changes(datasets: List[str], bucket_name: str, prefix: str = 'IMDB/', **context) -> Dict[str, bool]:
    """
    Check if datasets in GCS have changed based on metadata.
    Works with .tsv files directly.
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    
    changes = {}
    
    for dataset in datasets:
        blob_name = f"{prefix}{dataset}"
        blob = bucket.blob(blob_name)
        
        if not blob.exists():
            print(f"Warning: Blob {blob_name} does not exist in bucket {bucket_name}")
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
    
    return changes

def extract_metadata(datasets: List[str], bucket_name: str, prefix: str = 'IMDB/', **context) -> List[Dict[str, Any]]:
    """
    Extract metadata from datasets in GCS without counting records.
    """
    # Get change status from previous task
    changes = context['ti'].xcom_pull(task_ids='check_dataset_changes', key='dataset_changes')
    if changes is None:
        print("No change data received from previous task, assuming all files changed")
        changes = {dataset: True for dataset in datasets}
    
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    
    metadata_list = []
    
    for dataset in datasets:
        blob_name = f"{prefix}{dataset}"
        blob = bucket.blob(blob_name)
        
        print(f"Processing {blob_name}...")
        
        if not blob.exists():
            print(f"Warning: Blob {blob_name} does not exist in bucket {bucket_name}")
            continue
        
        # Get file size and md5 hash
        blob.reload()
        file_size = blob.size
        file_hash = blob.md5_hash
        
        print(f"File exists. Size: {file_size / (1024 * 1024):.2f} MB, Hash: {file_hash}")
        
        # Use a placeholder or estimated value for record count
        # Options:
        # 1. Use 0 as a placeholder (simplest)
        # 2. Use file size as a rough indicator (e.g., file_size / average_row_size)
        # 3. Use a fixed value like -1 to indicate "not counted"
        
        # Using option 1: simple placeholder
        
        metadata = {
            'name': dataset,
            'size': file_size,
            'hash': file_hash,
            'changed': changes.get(dataset, True)
        }
        
        print(f"Metadata for {dataset}: {metadata}")
        metadata_list.append(metadata)
    
    return metadata_list
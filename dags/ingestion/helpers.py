# dags/ingestion/helpers.py
import os
import gzip
from typing import List, Dict, Any
from google.cloud import storage

def check_dataset_changes(datasets: List[str], bucket_name: str, prefix: str = 'imdb/', **context) -> Dict[str, bool]:
    """
    Check if datasets in GCS have changed based on metadata.
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    
    changes = {}
    
    for dataset in datasets:
        blob_name = f"{prefix}{dataset}"
        blob = bucket.blob(blob_name)
        
        if not blob.exists():
            print(f"Warning: {blob_name} does not exist in bucket {bucket_name}")
            changes[dataset] = False
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
            metadata = blob.metadata or {}
            metadata['previous_md5_hash'] = current_md5
            blob.metadata = metadata
            blob.patch()
        else:
            changes[dataset] = False
    
    # Store results in XCom for later tasks
    context['ti'].xcom_push(key='dataset_changes', value=changes)
    
    return changes

def extract_metadata(datasets: List[str], bucket_name: str, prefix: str = 'imdb/', **context) -> List[Dict[str, Any]]:
    """
    Extract metadata from datasets in GCS.
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
            try:
                # Download to temp file
                temp_file = f"/tmp/{dataset}"
                blob.download_to_filename(temp_file)
                
                # Count records
                with gzip.open(temp_file, 'rt', encoding='utf-8') as f:
                    # Skip header
                    next(f, None)
                    record_count = sum(1 for _ in f)
                
                # Clean up
                os.remove(temp_file)
            except Exception as e:
                print(f"Error counting records in {dataset}: {str(e)}")
                record_count = -1  # Indicate error
        
        metadata = {
            'name': dataset,
            'size': file_size,
            'hash': file_hash,
            'records': record_count,
            'changed': changes.get(dataset, True)
        }
        
        metadata_list.append(metadata)
    
    return metadata_list
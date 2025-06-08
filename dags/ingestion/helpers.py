# dags/ingestion/helpers.py
import os
import gzip
import io
from typing import List, Dict, Any
from google.cloud import storage

def check_dataset_changes(datasets: List[str], bucket_name: str, prefix: str = 'IMDB/', **context) -> Dict[str, bool]:
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

def extract_metadata(datasets: List[str], bucket_name: str, prefix: str = 'IMDB/', **context) -> List[Dict[str, Any]]:
    """
    Extract metadata from datasets in GCS using temp files for reliability.
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
        
        # Count records using temporary file
        record_count = 0
        if changes.get(dataset, True):  # Default to True if not in changes dict
            try:
                print(f"Starting record count for {dataset}...")
                
                # Create a temporary file
                with tempfile.NamedTemporaryFile(delete=False) as temp_file:
                    temp_path = temp_file.name
                
                print(f"Downloading {dataset} to temporary file {temp_path}...")
                blob.download_to_filename(temp_path)
                print(f"Download complete. File size on disk: {os.path.getsize(temp_path) / (1024 * 1024):.2f} MB")
                
                # Count records
                print(f"Counting records in {dataset}...")
                with gzip.open(temp_path, 'rt', encoding='utf-8') as f:
                    # Skip header
                    header = next(f, None)
                    if header:
                        print(f"Header (first 50 chars): {header[:50]}...")
                        # Check if it's TSV by counting tabs
                        tab_count = header.count('\t')
                        print(f"Header contains {tab_count} tabs, suggesting {tab_count + 1} columns")
                    else:
                        print(f"Warning: No header found in {dataset}")
                    
                    # Count records with progress reporting
                    record_count = 0
                    for i, _ in enumerate(f):
                        record_count = i + 1  # +1 because enumerate is zero-indexed
                        if record_count % 1000000 == 0:
                            print(f"Counted {record_count} records in {dataset} so far...")
                
                print(f"Final record count for {dataset}: {record_count}")
                
                # Clean up temp file
                os.unlink(temp_path)
                print(f"Deleted temporary file {temp_path}")
                
            except Exception as e:
                print(f"Error counting records in {dataset}: {str(e)}")
                import traceback
                traceback.print_exc()
                record_count = -1  # Indicate error
        else:
            print(f"Skipping record count for {dataset} as it hasn't changed")
        
        metadata = {
            'name': dataset,
            'size': file_size,
            'hash': file_hash,
            'records': record_count,
            'changed': changes.get(dataset, True)
        }
        
        print(f"Metadata for {dataset}: {metadata}")
        metadata_list.append(metadata)
    
    return metadata_list

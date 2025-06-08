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
    Extract metadata from datasets in GCS.
    Streaming version → no /tmp/ file usage.
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
        
        print(f"Processing {blob_name} - size: {file_size} bytes")
        
        # Count records if needed (streaming mode)
        record_count = 0
        if changes.get(dataset, True):  # Default to True if not in changes dict
            try:
                print(f"Starting record count for {dataset}...")
                # Stream GZIP file directly from GCS
                with blob.open("rb") as blob_stream:
                    print(f"Opened blob stream for {dataset}")
                    with gzip.GzipFile(fileobj=blob_stream) as gzip_stream:
                        print(f"Opened gzip stream for {dataset}")
                        with io.TextIOWrapper(gzip_stream, encoding='utf-8') as text_stream:
                            print(f"Opened text stream for {dataset}")
                            # Skip header
                            header = next(text_stream, None)
                            print(f"Header (first 100 chars): {header[:100] if header else 'None'}")
                            
                            # Count records with progress reporting
                            line_count = 0
                            for line in text_stream:
                                if line.strip():  # Skip blank lines
                                    line_count += 1
                                    if line_count % 1000000 == 0:
                                        print(f"Counted {line_count} lines in {dataset} so far...")
                            
                            record_count = line_count
                            print(f"Final record count for {dataset}: {record_count}")

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

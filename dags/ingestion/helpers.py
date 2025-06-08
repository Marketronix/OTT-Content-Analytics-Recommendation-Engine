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
    Extract metadata from datasets in GCS.
    Works with .tsv files directly.
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
        
        # Check if we need to count records
        should_count = False
        # Always count records if:
        # 1. The file has changed, or
        # 2. We don't have record counts from previous runs
        if changes.get(dataset, True):  # File has changed
            should_count = True
            print(f"File {dataset} has changed, will count records")
        else:
            # Force count if we have 0 records
            try:
                from google.cloud import bigquery
                client = bigquery.Client()
                query = f"""
                SELECT MAX(record_count) as max_count 
                FROM `{os.getenv('GCP_PROJECT_ID')}.{os.getenv('BIGQUERY_DATASET')}.ingestion_log` 
                WHERE dataset_name = '{dataset}'
                """
                query_job = client.query(query)
                results = list(query_job)
                
                if not results or results[0]['max_count'] is None or results[0]['max_count'] <= 0:
                    print(f"No valid record count found for {dataset}, will count records")
                    should_count = True
                else:
                    record_count = results[0]['max_count']
                    print(f"Using existing record count for {dataset}: {record_count}")
            except Exception as e:
                print(f"Error checking previous record count: {str(e)}")
                should_count = True  # Default to counting
        
        # Count records if needed
        record_count = 0
        if should_count:
            try:
                print(f"Counting records for {dataset}...")
                
                # Simple approach for TSV files - stream directly
                line_count = 0
                with blob.open("rt") as f:
                    # Skip header
                    header = next(f, None)
                    if header:
                        print(f"Header (first 50 chars): {header[:50]}...")
                    
                    # Count remaining lines
                    for line_num, _ in enumerate(f, 1):
                        if line_num % 1000000 == 0:
                            print(f"Counted {line_num} records in {dataset} so far...")
                    
                    record_count = line_num
                    print(f"Final record count for {dataset}: {record_count}")
            
            except Exception as e:
                print(f"Error counting records in {dataset}: {str(e)}")
                import traceback
                traceback.print_exc()
                
                # Try fallback method with shell command
                try:
                    import tempfile
                    import subprocess
                    
                    print(f"Trying fallback method for counting records in {dataset}...")
                    with tempfile.NamedTemporaryFile(delete=False) as temp:
                        temp_path = temp.name
                    
                    print(f"Downloading {dataset} to {temp_path}...")
                    blob.download_to_filename(temp_path)
                    
                    # Count lines with wc -l, subtract 1 for header
                    cmd = f"wc -l < {temp_path}"
                    print(f"Running command: {cmd}")
                    process = subprocess.run(cmd, shell=True, capture_output=True, text=True)
                    total_lines = int(process.stdout.strip())
                    record_count = total_lines - 1  # Subtract header
                    
                    print(f"Counted {record_count} records in {dataset} using shell command")
                    
                    # Clean up
                    os.unlink(temp_path)
                except Exception as e2:
                    print(f"Fallback method also failed: {str(e2)}")
                    record_count = 0  # Default to 0 on complete failure
        
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
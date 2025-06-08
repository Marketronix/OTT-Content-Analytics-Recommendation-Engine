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
    Always count records if they're currently zero.
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
        
        # Check if we already have record count for this file
        need_to_count = True
        try:
            # Query BigQuery to see if we have a record count for this file
            from google.cloud import bigquery
            client = bigquery.Client()
            query = f"""
            SELECT record_count 
            FROM `{os.environ.get('GCP_PROJECT_ID')}.{os.environ.get('BIGQUERY_DATASET')}.ingestion_log` 
            WHERE dataset_name = '{dataset}' 
            AND record_count > 0
            ORDER BY ingestion_date DESC
            LIMIT 1
            """
            query_job = client.query(query)
            results = list(query_job)
            
            if results and results[0]['record_count'] > 0:
                # We already have a record count, use it
                record_count = results[0]['record_count']
                print(f"Using existing record count for {dataset}: {record_count}")
                need_to_count = False
            else:
                print(f"No existing record count found for {dataset}, will count records")
                need_to_count = True
        except Exception as e:
            print(f"Error querying for existing record count: {str(e)}")
            need_to_count = True
        
        # Count records if needed
        if need_to_count:
            try:
                print(f"Counting records for {dataset}...")
                
                # Stream GZIP file directly from GCS
                with blob.open("rb") as blob_stream:
                    with gzip.GzipFile(fileobj=blob_stream) as gzip_stream:
                        with io.TextIOWrapper(gzip_stream, encoding='utf-8') as text_stream:
                            # Skip header
                            header = next(text_stream, None)
                            if header:
                                print(f"Header (first 50 chars): {header[:50]}...")
                            
                            # Count records (skip blank lines)
                            record_count = 0
                            for line in text_stream:
                                if line.strip():
                                    record_count += 1
                                    if record_count % 1000000 == 0:
                                        print(f"Counted {record_count} records in {dataset} so far...")
                            
                            print(f"Final record count for {dataset}: {record_count}")

            except Exception as e:
                print(f"Error counting records in {dataset}: {str(e)}")
                import traceback
                traceback.print_exc()
                record_count = 0  # Default to 0 on error
        
        metadata = {
            'name': dataset,
            'size': file_size,
            'hash': file_hash,
            'records': record_count,
            'changed': changes.get(dataset, True) if changes else True
        }
        
        print(f"Metadata for {dataset}: {metadata}")
        metadata_list.append(metadata)
    
    return metadata_list

import hashlib
import os
from google.cloud import storage

def calculate_file_hash(file_path):
    """Calculate MD5 hash of a file."""
    md5_hash = hashlib.md5()
    with open(file_path, 'rb') as f:
        # Read in chunks for large files
        for chunk in iter(lambda: f.read(4096), b''):
            md5_hash.update(chunk)
    return md5_hash.hexdigest()

def get_metadata_from_gcs(bucket_name, blob_name):
    """Get metadata for a file in GCS."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    
    if not blob.exists():
        return None
    
    return blob.metadata

def update_metadata_in_gcs(bucket_name, blob_name, metadata):
    """Update metadata for a file in GCS."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    
    blob.metadata = metadata
    blob.patch()

def has_dataset_changed(local_file_path, bucket_name, blob_name):
    """Check if dataset has changed by comparing hash."""
    current_hash = calculate_file_hash(local_file_path)
    metadata = get_metadata_from_gcs(bucket_name, blob_name)
    
    if metadata is None or 'md5_hash' not in metadata:
        return True
    
    return current_hash != metadata['md5_hash']
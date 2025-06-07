import os
from google.cloud import storage
from google.api_core.exceptions import Conflict
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Read variables
project_id = os.getenv("GCP_PROJECT_ID")
location = os.getenv("GCP_REGION")

# Bucket names from env
buckets_to_create = [
    os.getenv("RAW_BUCKET_NAME"),
    os.getenv("PROCESSED_BUCKET_NAME"),
    os.getenv("ARCHIVE_BUCKET_NAME"),
]

# Initialize client
storage_client = storage.Client(project=project_id)

# Function to create a bucket
def create_bucket(bucket_name, location, enable_versioning=False):
    try:
        bucket = storage_client.bucket(bucket_name)
        bucket.storage_class = "STANDARD"
        new_bucket = storage_client.create_bucket(bucket, location=location)
        
        print(f"Bucket {bucket_name} created in {location}.")

        if enable_versioning:
            new_bucket.versioning_enabled = True
            new_bucket.patch()
            print(f"Object versioning enabled for bucket {bucket_name}.")

    except Conflict:
        print(f"Bucket {bucket_name} already exists.")

# Main loop to create all buckets
for bucket_name in buckets_to_create:
    enable_versioning = bucket_name == os.getenv("ARCHIVE_BUCKET_NAME")
    create_bucket(bucket_name, location, enable_versioning)

print("All requested buckets processed.")

# scripts/test/test_ingestion.py
import os
import sys
sys.path.append('../../dags/ingestion')
from dags.ingestion.helpers import download_imdb_datasets, calculate_file_hash, count_records_in_gz_tsv
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def test_download_and_metadata():
    """Test downloading a small subset of IMDb data and extracting metadata."""
    # Test with just one small dataset for quick verification
    test_datasets = ['title.ratings.tsv.gz']
    download_path = '/tmp/imdb_test'
    
    # Download test dataset
    downloaded_files = download_imdb_datasets(test_datasets, download_path)
    
    # Check if files were downloaded
    for file_path in downloaded_files:
        assert os.path.exists(file_path), f"Failed to download {file_path}"
    
    # Test hash calculation
    for file_path in downloaded_files:
        file_hash = calculate_file_hash(file_path)
        print(f"File: {file_path}, Hash: {file_hash}")
        assert len(file_hash) == 32, "MD5 hash should be 32 characters"
    
    # Test record counting
    for file_path in downloaded_files:
        record_count = count_records_in_gz_tsv(file_path)
        print(f"File: {file_path}, Records: {record_count}")
        assert record_count > 0, "Record count should be greater than 0"
    
    print("All tests passed!")

if __name__ == "__main__":
    test_download_and_metadata()
# /scripts/ingestion/download_imdb_data.py
import requests
import os
import gzip
import shutil

# List of IMDb datasets to download
DATASETS = [
    'name.basics.tsv.gz',
    'title.akas.tsv.gz',
    'title.basics.tsv.gz',
    'title.crew.tsv.gz',
    'title.episode.tsv.gz',
    'title.principals.tsv.gz',
    'title.ratings.tsv.gz'
]

BASE_URL = 'https://datasets.imdbws.com/'
DOWNLOAD_DIR = '/tmp/imdb_data'

def download_datasets():
    # Create download directory if it doesn't exist
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    
    downloaded_files = []
    for dataset in DATASETS:
        url = BASE_URL + dataset
        local_path = os.path.join(DOWNLOAD_DIR, dataset)
        
        print(f"Downloading {dataset}...")
        response = requests.get(url, stream=True)
        with open(local_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        downloaded_files.append(local_path)
        print(f"Downloaded {dataset} to {local_path}")
    
    return downloaded_files

if __name__ == '__main__':
    download_datasets()
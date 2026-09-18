from __future__ import annotations


import pandas as pd
import requests

import os
import sys
from urllib import response
from itertools import product
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from google.cloud import storage
from google.api_core.exceptions import NotFound, Forbidden
import time
import requests

import zipfile
import gzip
import shutil
import os
import io

from ingestion.config import (
    ARCGIS_ITEM_DATA_URL_TEMPLATE,

    BUS_ARRIVAL_DEPARTURE_ITEM_IDS,
    RAW_BUS_HISTORICAL_DIR,
    GCS_BUS_HISTORICAL_PREFIX,

    BUCKET_NAME,
    CREDENTIALS_FILE,
    CHUNK_SIZE
)

MAX_WORKERS = 8
 
client = storage.Client.from_service_account_json(CREDENTIALS_FILE)
bucket = client.bucket(BUCKET_NAME)

def verify_gcs_upload(blob_name):
    return storage.Blob(bucket=bucket, name=blob_name).exists(client)

def check_schema_consistency(zip_path: str) -> None:
    with zipfile.ZipFile(zip_path) as zf:
        csv_names = sorted(n for n in zf.namelist() if n.lower().endswith(".csv"))
        headers = {}
        for name in csv_names:
            with zf.open(name) as f:
                header_line = f.readline().decode("utf-8").strip()
                headers[name] = header_line

        unique_headers = set(headers.values())
        if len(unique_headers) == 1:
            print(f"✅ All {len(csv_names)} files share the same schema.")
        else:
            print(f"⚠️ Found {len(unique_headers)} different schemas:")
            for name, h in headers.items():
                print(f"{h}")


def download_file(year:int, item_id: str):
    file_url = ARCGIS_ITEM_DATA_URL_TEMPLATE.format(item_id=item_id)
    
    file_name = f"MBTA_Bus_Arrival_Departure_Times_{year}.zip"
    file_path = os.path.join(RAW_BUS_HISTORICAL_DIR, f"{file_name}")

    print(file_url, file_name, file_path)

    try:
        print(f"Downloading {file_url}...")
        if os.path.exists(file_path):
            print(f"File already exists: {file_path}. Skipping download.")
            return file_path
    
        urllib.request.urlretrieve(file_url, file_path)
        print(f"Downloaded: {file_path}")
        return file_path
    except Exception as e:
        print(f"Failed to download {file_path}: {e}")
        return None


def stream_zip_entry_to_gcs(zip_path: str, entry_name: str, blob_name: str) -> str:
    """
    Read one CSV entry from the ZIP, gzip-compress it in memory,
    and upload directly to GCS — no intermediate file on disk.
    """
    try:
        with zipfile.ZipFile(zip_path) as zf:
            with zf.open(entry_name) as src:
                buffer = io.BytesIO()
                with gzip.GzipFile(fileobj=buffer, mode="wb") as gz:
                    shutil.copyfileobj(src, gz)
                buffer.seek(0)

                blob = bucket.blob(blob_name)
                blob.content_encoding = "gzip"
                blob.upload_from_file(buffer, content_type="text/csv")

        uri = f"gs://{BUCKET_NAME}/{blob_name}"
        print(f"Uploaded: {uri}")
        return uri
    except FileNotFoundError:
        print(f"ZIP file not found: {zip_path}")
    except zipfile.BadZipFile:
        print(f"Invalid or corrupted ZIP file: {zip_path}")
    except KeyError:
        print(f"File not found inside ZIP: {entry_name}")


def upload_zip_contents_parallel(zip_path: str, year: int, max_workers: int = 4) -> list[str]:
    with zipfile.ZipFile(zip_path) as zf:
        csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
    
    def _process(name: str) -> str:
        base_name = os.path.basename(name)
       
        blob_name = f"{GCS_BUS_HISTORICAL_PREFIX}/{base_name}.gz"
        blob = bucket.blob(blob_name)
        if blob.exists(client):
            print(f"File already exists in GCS: {blob_name}. Skipping upload.")
            return

        return stream_zip_entry_to_gcs(zip_path, name, blob_name)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        results = list(executor.map(_process, csv_names))

    return results

def process_bus_historical_year(year: int, item_id: str) -> list[str]:
    zip_path = download_file(year, item_id)
    if zip_path is None:
        return []

    return upload_zip_contents_parallel(zip_path, year)


# if __name__ == "__main__":
#     # have args for year optional for backfill from 2022 to 2026
#     # This script is intended to be imported and used as a module, not run directly.
#     # for year, item_id in BUS_ARRIVAL_DEPARTURE_ITEM_IDS.items():
#     #     print(f"Processing year: {year}")
#     #     uploaded_uris = process_year(year, item_id)
#     #     print(f"Uploaded {len(uploaded_uris)} files for year {year}.")
#     pass
    
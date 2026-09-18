from concurrent.futures import ThreadPoolExecutor
import os
import time
import urllib.request
import zipfile
import io
import gzip
import shutil
from google.cloud import storage
import sqlite3
import pandas as pd

from ingestion.config import (
    GTFS_ARCHIVE_YEARS,
    RAW_GTFS_ARCHIVE_DIR,


    BUCKET_NAME,
    GCS_GTFS_ARCHIVE_PREFIX,
    CREDENTIALS_FILE,
    CHUNK_SIZE,

    GTFS_ARCHIVE_DB_URL_TEMPLATE
)

client = storage.Client.from_service_account_json(CREDENTIALS_FILE)
bucket = client.bucket(BUCKET_NAME)

def download_file(year:int, output_dir: str):
    file_url = GTFS_ARCHIVE_DB_URL_TEMPLATE.format(year=year)
    
    file_name = f"GTFS_ARCHIVE_{year}.db.gz"
    file_path = os.path.join(output_dir, f"{file_name}")

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


def decompress_db(gz_path: str, output_dir: str) -> str:
    """Decompress GTFS_ARCHIVE_{year}.db.gz -> GTFS_ARCHIVE_{year}.db"""
    db_path = os.path.join(output_dir, gz_path[:-3])  # strip ".gz"

    if os.path.exists(db_path):
        print(f"Already decompressed: {db_path}. Skipping.")
        return db_path
 
    print(f"Decompressing {gz_path} -> {db_path} ...")
    with gzip.open(gz_path, "rb") as f_in, open(db_path, "wb") as f_out:
        shutil.copyfileobj(f_in, f_out)
    print(f"Decompressed: {db_path}")
    return db_path


def upload_to_gcs(file_path: str, year: int, max_retries=3):
    from google.cloud import storage

    client = storage.Client.from_service_account_json(CREDENTIALS_FILE)
    bucket = client.bucket(BUCKET_NAME)

    blob_name = f"{GCS_GTFS_ARCHIVE_PREFIX}/GTFS_ARCHIVE_{year}.db.gz"
    blob = bucket.blob(blob_name)
    blob.chunk_size = CHUNK_SIZE

    if blob.exists(client):
        print(f"File already exists in GCS: {blob_name}. Skipping upload.")
        return

    for attempt in range(max_retries):
        try:
            with open(file_path, "rb") as f:
                blob.upload_from_file(f)
                print(f"Uploaded to GCS: {blob_name}")
                return
        except Exception as e:
            print(f"Attempt {attempt + 1} failed to upload {file_path} to GCS: {e}")
            if attempt < max_retries - 1:
                print("Retrying...")

        time.sleep(5)

    print(f"Giving up on {file_path} after {max_retries} attempts.")

def list_tables(db_path: str) -> list[str]:
    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()
        return [r[0] for r in rows]
    finally:
        conn.close()

def table_to_csv_gz(db_path: str, table: str, output_dir: str, chunksize: int = 500_000) -> str:
    """
    Read one SQLite table in chunks (memory-safe) and write it all into
    a single gzip-compressed CSV file — no schema-matching issues like
    Parquet has, since CSV just appends text.
    """
    os.makedirs(output_dir, exist_ok=True)
    path = os.path.join(output_dir, f"{table}.csv.gz")
 
    conn = sqlite3.connect(db_path)
    total_rows = 0
    
    try:
        if os.path.exists(path):
            print(f"File already exists: {path}. Skipping conversion.")
            return path
        with gzip.open(path, "wt", newline="") as f_out:
            for i, chunk in enumerate(pd.read_sql(f"SELECT * FROM {table}", conn, chunksize=chunksize)):
                chunk.to_csv(f_out, index=False, header=(i == 0))
                total_rows += len(chunk)
    finally:
        conn.close()
 
    print(f"Wrote {path} ({total_rows} rows)")
    return path

def convert_all_tables_to_csv_gz(db_path: str, output_dir: str, max_workers: int = 8) -> dict[str, str | None]:
    """Convert every table in the DB to a local .csv.gz file, in parallel."""
    tables = list_tables(db_path)
    print(f"Found {len(tables)} tables: {tables}")
 
    def _process(table: str) -> str | None:
        try:
            return table_to_csv_gz(db_path, table, output_dir)
        except Exception as e:
            print(f"Failed to convert table '{table}': {e}")
            return None
 
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # executor.map preserves input order, so zip it back with `tables`
        # to rebuild the {table: path} mapping — map() itself returns a
        # plain iterable of results, not a dict.
        paths = list(executor.map(_process, tables))
 
    return dict(zip(tables, paths))

def upload_table_csv_gz(local_path: str, table: str, year: int, max_retries: int = 3) -> str | None:
    """Upload one table's .csv.gz to GCS, with retries. Skips if already uploaded."""
    blob_name = f"{GCS_GTFS_ARCHIVE_PREFIX}/year={year}/{table}.csv.gz"
    blob = bucket.blob(blob_name)
    blob.chunk_size = CHUNK_SIZE
    blob.content_encoding = "gzip"
 
    if blob.exists(client):
        print(f"Already uploaded, skipping: {blob_name}")
        return f"gs://{BUCKET_NAME}/{blob_name}"
 
    for attempt in range(max_retries):
        try:
            blob.upload_from_filename(local_path, content_type="text/csv")
            uri = f"gs://{BUCKET_NAME}/{blob_name}"
            print(f"Uploaded: {uri}")
            return uri
        except Exception as e:
            print(f"Attempt {attempt + 1} failed to upload {local_path}: {e}")
 
    print(f"Giving up on {local_path} after {max_retries} attempts.")
    return None

def upload_all_tables(local_paths: dict[str, str | None], year: int, max_workers: int = 8) -> dict[str, str | None]:
    """Upload every locally-converted table to GCS, in parallel."""
    results: dict[str, str | None] = {}
 
    # Tables that failed conversion have no local file — skip them up front,
    # don't waste a worker on them.
    to_upload = {table: path for table, path in local_paths.items() if path is not None}
    for table, path in local_paths.items():
        if path is None:
            print(f"Skipping upload for '{table}' — no local file (conversion failed earlier).")
            results[table] = None
 
    def _upload_one(item: tuple[str, str]) -> tuple[str, str | None]:
        table, path = item
        try:
            return table, upload_table_csv_gz(path, table, year)
        except Exception as e:
            print(f"Failed to upload table '{table}': {e}")
            return table, None
 
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for table, uri in executor.map(_upload_one, to_upload.items()):
            results[table] = uri
 
    return results
 
    

def process_gtfs_archive_year(year: int, max_retries: int = 3) -> list[str]:
    table_output_dir = os.path.join(RAW_GTFS_ARCHIVE_DIR, f"year={year}")

    zip_path = download_file(year, table_output_dir)
    if zip_path is None:
        return {}

    db_path = decompress_db(zip_path, table_output_dir)
    local_paths = convert_all_tables_to_csv_gz(db_path, table_output_dir, max_workers=8)
    results = upload_all_tables(local_paths, year, max_workers=8)
    return results

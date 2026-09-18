from __future__ import annotations
 
import gzip
import io
import json
import time
 
import pandas as pd
import requests
from google.cloud import storage
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
 
from ingestion.config import (
    BUCKET_NAME,
    CREDENTIALS_FILE,
    GCS_REALTIME_PREFIX,
    MBTA_API_BASE_URL,
    MBTA_API_HEADERS,
)
 
MAX_WORKERS = 40
BUS_ROUTE_TYPE = 3


 
 

# ---------------------------------------------------------------------------
# Fetch: routes, predictions (per route), vehicles (all buses, one call)
# Each returns a plain list[dict] — no pandas involved.
# ---------------------------------------------------------------------------
 
def fetch_bus_routes() -> list[str]:
    """All bus route IDs (route_type=3) — needed because /predictions has no route_type filter."""
    response = requests.get(
        f"{MBTA_API_BASE_URL}/routes",
        headers=MBTA_API_HEADERS,
        params={"filter[type]": BUS_ROUTE_TYPE},
        timeout=10,
    )
    response.raise_for_status()
    data = response.json()["data"]
    return [route["id"] for route in data]

def fetch_prediction_for_route(route_id: str) -> dict:
    """
    Predicted arrival/departure times for buses currently en route,
    joined against the scheduled time via include=schedule so delay
    can actually be computed (not just a disruption count).
    """
    response = requests.get(
        f"{MBTA_API_BASE_URL}/predictions",
        headers=MBTA_API_HEADERS,
        params={
            "filter[route]": route_id,
            "include": "schedule,stop,route,trip,vehicle",
        },
        timeout=10,
    )
    
    collected_at = datetime.now(timezone.utc).isoformat()
    response.raise_for_status()
    payload = response.json()

    raw_dict = {
        "collected_at": collected_at,
        "data": payload["data"]
    } 
    return raw_dict


def fetch_all_routes_prediction(): 
    route_ids = fetch_bus_routes()
    with ThreadPoolExecutor(max_workers=20) as executor:
        results = list(executor.map(fetch_prediction_for_route, route_ids))

    return results

def upload_realtime_predictions_to_gcs(results):
    json_data = "\n".join(
        json.dumps(result)
        for result in results
    )

    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="wb") as gz:
        gz.write(json_data.encode("utf-8"))

    # Reset buffer position before uploading
    buffer.seek(0)
    client = storage.Client.from_service_account_json(CREDENTIALS_FILE)
    bucket = client.bucket(BUCKET_NAME)

    # Unique filename for this collection
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    blob_name = (
        f"{GCS_REALTIME_PREFIX}/"
        f"predictions_{timestamp}.json.gz"
    )

    blob = bucket.blob(blob_name)

    blob.content_encoding = "gzip"
    blob.upload_from_file(
        buffer,
        content_type="application/json",
    )

    print(f"Uploaded: gs://{BUCKET_NAME}/{blob_name}")
    return blob_name

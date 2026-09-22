from __future__ import annotations

import gzip
import hashlib
import io
import json
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import requests
from google.cloud import storage

from kestra import Kestra


from ingestion.config import (
    BUCKET_NAME,
    CREDENTIALS_FILE,
    GCS_SCHEDULES_PREFIX,   # NEW -- add to config.py, e.g. "raw/schedules"
    SCHEDULE_STATE_BLOB,    # NEW -- add to config.py, e.g. "state/mbta_schedule_last_modified.json"
    MBTA_API_BASE_URL,
    MBTA_API_HEADERS,
)

MAX_WORKERS = 10
BUS_ROUTE_TYPE = 3
PAGE_LIMIT = 500

# All this pipeline needs -- shrinks the payload on the routes that actually changed.
SCHEDULE_FIELDS = "arrival_time,departure_time"

def _content_hash(data: list[dict]) -> str:
    """
    Hash of the actual schedule content, independent of Last-Modified.

    Sorted by id first so the hash is stable regardless of the order MBTA
    happens to return records in -- we only want this to change when the
    real arrival/departure values change, not the response ordering.
    """
    normalized = sorted(
        (
            {
                "id": r.get("id"),
                "arrival_time": r.get("attributes", {}).get("arrival_time"),
                "departure_time": r.get("attributes", {}).get("departure_time"),
            }
            for r in data
        ),
        key=lambda r: r["id"] or "",
    )
    blob = json.dumps(normalized, sort_keys=True).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()


def fetch_bus_routes() -> list[str]:
    """All bus route IDs (route_type=3) -- same call the predictions poller makes."""
    response = requests.get(
        f"{MBTA_API_BASE_URL}/routes",
        headers=MBTA_API_HEADERS,
        params={"filter[type]": BUS_ROUTE_TYPE},
        timeout=10,
    )
    response.raise_for_status()
    data = response.json()["data"]
    return [route["id"] for route in data]

def service_date() -> str:
    """MBTA schedules are published per service date, in Eastern time."""
    return datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d")

def _gcs_client() -> storage.Client:
    return storage.Client.from_service_account_json(CREDENTIALS_FILE)

def load_last_modified_state() -> dict:
    """
    route_id -> Last-Modified string, keyed under today's service date.

    Schedules barely change (only when MBTA republishes the feed), so instead
    of re-fetching everything every run we send back the Last-Modified value
    we last saw per route. MBTA tracks Last-Modified per query on the root
    resource type (/schedules with no include= -> schedule is the root type),
    so this state is what lets each route's request come back as a free 304
    when nothing changed.
    """
    client = _gcs_client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(SCHEDULE_STATE_BLOB)
    if not blob.exists():
        return {"last_modified": {}, "content_hash": {}}

    state = json.loads(blob.download_as_text())
    state.setdefault("last_modified", {})
    state.setdefault("content_hash", {})
    return state

def save_last_modified_state(state: dict) -> None:
    client = _gcs_client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(SCHEDULE_STATE_BLOB)
    blob.upload_from_string(json.dumps(state), content_type="application/json")

def fetch_schedule_for_route(
    route_id: str, 
    date_str: str, 
    last_modified: str | None, 
    last_content_hash: str | None,
) -> dict | None:
    """
    Conditional GET against /schedules for one route/date.

    Returns None on 304 Not Modified (nothing changed -- per MBTA's docs,
    304s don't count against the rate limit) or when a route genuinely has
    no schedules. On a real change, returns the same shape
    fetch_prediction_for_route uses, plus route_id/service_date/_last_modified
    so the caller can persist the new checkpoint.
    """
    url = f"{MBTA_API_BASE_URL}/schedules"
    params = {
        "filter[route]": route_id,
        "filter[date]": date_str,
        "fields[schedule]": SCHEDULE_FIELDS,
        "page[limit]": PAGE_LIMIT,
    }
    headers = dict(MBTA_API_HEADERS)
    if last_modified:
        headers["If-Modified-Since"] = last_modified

    collected_at = datetime.now(timezone.utc).isoformat()

    all_data: list[dict] = []
    new_last_modified = last_modified
    next_url = url
    first = True

    while next_url:
        response = requests.get(
            next_url,
            headers=headers,
            params=params if first else None,
            timeout=10,
        )
        first = False

        if response.status_code == 304:
            return None

        response.raise_for_status()

        # Only trust Last-Modified from the first page of a given query.
        if "Last-Modified" in response.headers and new_last_modified == last_modified:
            new_last_modified = response.headers["Last-Modified"]

        payload = response.json()
        all_data.extend(payload.get("data", []))
        next_url = payload.get("links", {}).get("next")

    if not all_data:
        print(f"Route {route_id}: no schedules")
        return None

    new_hash = _content_hash(all_data)
    if new_hash == last_content_hash:
        # Last-Modified said something changed, the content says it didn't.
        # Trust the content. Still worth bumping last_modified so we don't
        # keep re-fetching the full body on every future run for this route.
        print(f"Route {route_id}: Last-Modified changed but content identical -- skipping")
        return {"route_id": route_id, "_last_modified": new_last_modified, "_content_hash": new_hash, "_unchanged": True}

    return {
        "collected_at": collected_at,
        "data": all_data,
        "route_id": route_id,
        "service_date": date_str,
        "_last_modified": new_last_modified,
        "_content_hash": new_hash,
    }


def fetch_all_routes_schedules() -> tuple[list[dict], dict]:
    """
    Returns (changed_results, updated_state). Unchanged routes (304) are
    simply absent from changed_results -- there's nothing to upload for them.
    """
    date_str = service_date()
    state = load_last_modified_state()
    last_modified = state["last_modified"]  # flat, route_id -> header value
    content_hashes = state["content_hash"]  # flat, route_id -> hash

    route_ids = fetch_bus_routes()

    def _fetch(route_id: str) -> dict | None:
        return fetch_schedule_for_route(
            route_id,
            date_str,
            last_modified.get(route_id),
            content_hashes.get(route_id),
        )

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        results = list(executor.map(_fetch, route_ids))

    results = [r for r in results if r is not None]
    changed = []
    for r in results:
        route_id = r["route_id"]
        last_modified[route_id] = r.pop("_last_modified")
        content_hashes[route_id] = r.pop("_content_hash")
        if not r.pop("_unchanged", False):
            changed.append(r)

    print(
        f"{len(changed)}/{len(route_ids)} bus routes had real schedule changes on {date_str} "
        f"({len(results) - len(changed)} were Last-Modified noise or identical to a prior service date)"
    )
    return changed, state

def upload_schedules_to_gcs(results: list[dict]) -> str | None:
    """Mirrors upload_realtime_predictions_to_gcs. No-ops if nothing changed."""
    if not results:
        print("No schedule changes this run -- skipping upload.")
        return None

    batch_time = datetime.now(timezone.utc)
    timestamp = batch_time.strftime("%Y%m%d_%H%M%S")
    batch_loaded_at = batch_time.isoformat()

    json_data = "\n".join(
        json.dumps({**result, "batch_loaded_at": batch_loaded_at})
        for result in results
    )

    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="wb") as gz:
        gz.write(json_data.encode("utf-8"))
    buffer.seek(0)

    client = _gcs_client()
    bucket = client.bucket(BUCKET_NAME)

    blob_name = f"{GCS_SCHEDULES_PREFIX}/schedules_{timestamp}.json.gz"
    blob = bucket.blob(blob_name)

    blob.content_encoding = "gzip"
    blob.upload_from_file(
        buffer,
        content_type="application/json",
    )

    print(f"Uploaded: gs://{BUCKET_NAME}/{blob_name}")
    return blob_name

def run() -> None:
    changed_results, updated_state = fetch_all_routes_schedules()
    blob_name = upload_schedules_to_gcs(changed_results)
    # Persist state after the upload attempt so a failed upload doesn't lose
    # track of which routes still need re-fetching next run.
    save_last_modified_state(updated_state)
    Kestra.outputs({"blob_name": blob_name or ""})



if __name__ == "__main__":
    run()


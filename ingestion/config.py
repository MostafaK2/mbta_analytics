from __future__ import annotations
 
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()



# Project directory structure
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"

RAW_BUS_HISTORICAL_DIR = RAW_DIR / "bus_historical"
RAW_GTFS_ARCHIVE_DIR = RAW_DIR / "gtfs_archive"
RAW_REALTIME_DIR = RAW_DIR / "realtime"

# CLOUD CONFIGS
BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME")
CREDENTIALS_FILE = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
CHUNK_SIZE = 8 * 1024 * 1024

# CLOUD STORAGE PREFIXES
GCS_RAW_PREFIX = "raw"
GCS_BUS_HISTORICAL_PREFIX = f"{GCS_RAW_PREFIX}/bus_historical"
GCS_GTFS_ARCHIVE_PREFIX = f"{GCS_RAW_PREFIX}/gtfs_archive"
GCS_REALTIME_PREFIX = f"{GCS_RAW_PREFIX}/realtime"

for _dir in (RAW_BUS_HISTORICAL_DIR, RAW_GTFS_ARCHIVE_DIR, RAW_REALTIME_DIR):
    _dir.mkdir(parents=True, exist_ok=True)

# # ---------------------------------------------------------------------------
# # MBTA V3 API (real-time layer)
# # ---------------------------------------------------------------------------
MBTA_API_KEY = os.environ.get("MBTA_API_KEY", "")
MBTA_API_BASE_URL = "https://api-v3.mbta.com"
MBTA_API_HEADERS = {"x-api-key": MBTA_API_KEY} if MBTA_API_KEY else {}


# ---------------------------------------------------------------------------
# MBTA V2 API (historical layer)
# ---------------------------------------------------------------------------
BUS_ARRIVAL_DEPARTURE_ITEM_IDS = {
    2022: "ef464a75666349f481353f16514c06d0",
    2023: "b7b36fdb7b3a4728af2fccc78c2ca5b7",
    2024: "96c77138c3144906bce93d0257531b6a",
    2025: "924df13d845f4907bb6a6c3ed380d57a",
    2026: "9d8a8cad277545c984c1b25ed10b7d3c"
}

ARCGIS_ITEM_DATA_URL_TEMPLATE = "https://www.arcgis.com/sharing/rest/content/items/{item_id}/data"


# ---------------------------------------------------------------------------
# GTFS static archive (LAMP)
# ---------------------------------------------------------------------------
 
LAMP_BASE_URL = "https://performancedata.mbta.com/lamp"
GTFS_ARCHIVE_YEARS: list[int] = [2022, 2023, 2024, 2025, 2026]
 
GTFS_ARCHIVE_DB_URL_TEMPLATE = (
    "https://performancedata.mbta.com/lamp/gtfs_archive/{year}/GTFS_ARCHIVE.db.gz"
)

# # ---------------------------------------------------------------------------
# # Real-time polling defaults
# # ---------------------------------------------------------------------------
 
# REALTIME_POLL_ROUTE_IDS: list[str] = []  # empty = all routes; set to a list to scope down
# REALTIME_REQUEST_TIMEOUT_SECONDS = 10

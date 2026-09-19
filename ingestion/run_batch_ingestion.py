# Run Batch Ingestion and GTFS Static Archive Ingestion
from ingestion.extractors.bus_historical import process_bus_historical_year, BUS_ARRIVAL_DEPARTURE_ITEM_IDS
from ingestion.extractors.gtfs_static_ark import process_gtfs_archive_year, GTFS_ARCHIVE_YEARS
import argparse

MIN_SUPPORTED_YEAR = 2022
MAX_SUPPORTED_YEAR = 2026

def run_batch_ingestion(year):
    print(f"Starting batch ingestion for year: {year}")

    # Process Bus Historical Data
    print(f"Processing Bus Historical Data...")
    bus_historical_results = process_bus_historical_year(year, BUS_ARRIVAL_DEPARTURE_ITEM_IDS[year])

    # # Process GTFS Static Archive Data
    # print(f"Processing GTFS Static Archive Data...")
    # gtfs_archive_results = process_gtfs_archive_year(year)

    # print(f"Batch ingestion completed for year...")


def run_backfill_ingestion(start_year, end_year):
    if start_year > end_year:
        raise ValueError(f"start_year ({start_year}) must be <= end_year ({end_year}).")
    if start_year < MIN_SUPPORTED_YEAR or end_year > MAX_SUPPORTED_YEAR:
        raise ValueError(
            f"Requested range {start_year}-{end_year} is outside the supported "
            f"range {MIN_SUPPORTED_YEAR}-{MAX_SUPPORTED_YEAR}. "
            f"No data sources are configured outside this window."
        )
    
    for year in range(start_year, end_year + 1):
        run_batch_ingestion(year)

if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--year",
        type=int,
        help="Process a single year"
    )
    parser.add_argument(
        "--start-year",
        type=int,
        help="Start year for backfill"
    )
    parser.add_argument(
        "--end-year",
        type=int,
        help="End year for backfill"
    )

    args = parser.parse_args()

    if args.year is not None:
        run_batch_ingestion(args.year)
    elif args.start_year is not None and args.end_year is not None:
        run_backfill_ingestion(args.start_year, args.end_year)
    else:
        parser.error(
            "Provide either --year or both --start-year and --end-year for backfilling."
        )
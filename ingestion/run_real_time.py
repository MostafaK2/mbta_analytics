from kestra import Kestra

from ingestion.extractors.gtfs_rt_batch import \
    fetch_all_routes_prediction, \
    upload_realtime_predictions_to_gcs


def run_batch_ingestion():
    print(f"Starting batch ingestion for GTFS-RT")
    results = fetch_all_routes_prediction()

    print(f"Starting GCS upload")
    blob_name = upload_realtime_predictions_to_gcs(results)
    Kestra.outputs({"blob_name": blob_name})
    



if __name__ == "__main__":
    run_batch_ingestion()
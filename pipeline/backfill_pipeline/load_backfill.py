import json

from google.cloud import storage, bigquery

from gcp.bigquery.schema import json_schema
from pipeline.backfill_pipeline.transform_backfill import transform_historical_data
from utils.logger import log
from utils.settings import GCS_BUCKET, PROJECT_ID, BQ_DATASET, BQ_STAGING


def load_backfill_to_staging(**context) -> int:

    manifest_path: str = context["ti"].xcom_pull(task_ids="fetch_historical")

    log.info("Loading backfill_pipeline manifest from: gs://%s/%s", GCS_BUCKET, manifest_path)

    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET)
    manifest_blob = bucket.blob(manifest_path)
    manifest = json.loads(manifest_blob.download_as_text())

    coin_files = manifest["coin_files"]
    total_coins = manifest["total_coins"]
    successful_coins = manifest["successful_coins"]
    failed_coins = manifest["failed_coins"]

    log.info("Backfill manifest: %s coin files, %s/%s coins successful",
             len(coin_files), successful_coins, total_coins)

    if failed_coins:
        log.warning("Failed coins (%s): %s", len(failed_coins), failed_coins[:10])

    all_historical_data = []

    for idx, coin_file in enumerate(coin_files, 1):
        log.info("Reading coin %s/%s from gs://%s/%s",
                 idx, len(coin_files), GCS_BUCKET, coin_file)

        coin_blob = bucket.blob(coin_file)
        coin_data = json.loads(coin_blob.download_as_text())

        coin_data["source_file"] = f"gs://{GCS_BUCKET}/{coin_file}"
        all_historical_data.append(coin_data)

        log.info("Coin %s: loaded %s price points (total coins so far: %s)",
                 idx, len(coin_data.get("prices", [])), len(all_historical_data))

    if not all_historical_data:
        raise ValueError("No historical data found in batches")

    log.info("Total historical data loaded: %s coins", len(all_historical_data))

    rows = transform_historical_data(all_historical_data)

    log.info("Transformed %s coins into %s rows", len(all_historical_data), len(rows))

    bq = bigquery.Client(project=PROJECT_ID)
    table_ref = f"{PROJECT_ID}.{BQ_DATASET}.{BQ_STAGING}"

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema=json_schema,
        ignore_unknown_values=True,
    )

    log.info("Loading %s rows to %s...", len(rows), table_ref)

    job = bq.load_table_from_json(rows, table_ref, job_config=job_config)
    job.result()

    log.info("✓ Backfill loaded: %s rows → %s", len(rows), table_ref)

    return len(rows)

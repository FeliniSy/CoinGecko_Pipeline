import json
from google.cloud import bigquery, storage

from gcp.bigquery.schema import json_schema
from pipeline.backfill_pipeline.transform_backfill import transform_coin_data
from utils.logger import log
from utils.settings import PROJECT_ID, BQ_DATASET, BQ_STAGING, GCS_BUCKET


def load_backfill_to_staging(**context) -> int:

    manifest_path: str = context["ti"].xcom_pull(task_ids="fetch_historical")

    log.info("Loading backfill manifest from: gs://%s/%s", GCS_BUCKET, manifest_path)

    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET)
    manifest_blob = bucket.blob(manifest_path)
    manifest = json.loads(manifest_blob.download_as_text())

    coin_files = manifest["coin_files"]
    total_coins = len(coin_files)

    log.info("Total coins to process: %s", total_coins)

    BATCH_SIZE = 100
    total_rows_loaded = 0

    bq = bigquery.Client(project=PROJECT_ID)
    table_ref = f"{PROJECT_ID}.{BQ_DATASET}.{BQ_STAGING}"

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema=json_schema,
        ignore_unknown_values=True,
    )

    for batch_start in range(0, total_coins, BATCH_SIZE):
        batch_end = min(batch_start + BATCH_SIZE, total_coins)
        batch_coin_files = coin_files[batch_start:batch_end]

        log.info("Processing batch: coins %s-%s (%s coins)",
                 batch_start + 1, batch_end, len(batch_coin_files))

        batch_rows = []

        for idx, coin_file in enumerate(batch_coin_files, 1):
            global_idx = batch_start + idx

            try:
                coin_blob = bucket.blob(coin_file)
                coin_data = json.loads(coin_blob.download_as_text())
                coin_data["source_file"] = f"gs://{GCS_BUCKET}/{coin_file}"

                transformed_row = transform_coin_data(coin_data)

                if transformed_row:
                    batch_rows.append(transformed_row)

                if global_idx % 50 == 0:
                    log.info("Processed %s/%s coins", global_idx, total_coins)

            except Exception as e:
                log.error("Failed to process coin file %s: %s", coin_file, str(e))
                continue

        if not batch_rows:
            log.warning("No valid rows in batch %s-%s, skipping", batch_start + 1, batch_end)
            continue

        log.info("Loading batch to BigQuery: %s rows", len(batch_rows))

        try:
            job = bq.load_table_from_json(batch_rows, table_ref, job_config=job_config)
            job.result()
            total_rows_loaded += len(batch_rows)
            log.info("✓ Batch loaded: %s rows (total so far: %s)", len(batch_rows), total_rows_loaded)
        except Exception as e:
            log.error("Failed to load batch to BigQuery: %s", str(e))
            raise

        del batch_rows

    log.info("✓ All batches loaded: %s total rows → %s", total_rows_loaded, table_ref)
    return total_rows_loaded

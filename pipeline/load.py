import json

from google.cloud import storage, bigquery

from gcp.bigquery.schema import json_schema
from pipeline.transform import transform_market_data
from utils.logger import log
from utils.settings import GCS_BUCKET, PROJECT_ID, BQ_DATASET, BQ_STAGING


def load_to_staging(**context) -> int:

    blob_path: str = context["ti"].xcom_pull(task_ids="fetch_markets")
    gcs_uri = f"gs://{GCS_BUCKET}/{blob_path}"

    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET)
    raw = json.loads(bucket.blob(blob_path).download_as_text())
    coins = raw["coins"]
    fetched_at = raw["fetched_at"]

    rows = transform_market_data(coins, fetched_at, gcs_uri)

    bq = bigquery.Client(project=PROJECT_ID)
    table_ref = f"{PROJECT_ID}.{BQ_DATASET}.{BQ_STAGING}"

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema=json_schema,
        ignore_unknown_values=True,
    )

    job = bq.load_table_from_json(rows, table_ref, job_config=job_config)
    job.result()

    log.info("Loaded %s rows → %s", len(rows), table_ref)
    return len(rows)
from google.cloud import bigquery

from gcp.bigquery.schema import json_schema
from pipeline.backfill_pipeline.transform_backfill import transform_historical_data
from utils.logger import log
from utils.settings import PROJECT_ID, BQ_DATASET, BQ_STAGING


def load_backfill_to_staging(**context) -> int:
    log.info("Starting backfill transform and load to staging...")

    rows = transform_historical_data(**context)

    log.info("Transformed data into %s rows", len(rows))

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

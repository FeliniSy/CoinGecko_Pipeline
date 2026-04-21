from google.cloud import bigquery

from gcp.bigquery.merge_queries import _ingestion_merge_sql, _backfill_merge_sql
from utils.logger import log
from utils.settings import PROJECT_ID


def _run_merge(bq: bigquery.Client, sql: str, label: str) -> None:
    log.info("Running MERGE: %s", label)
    bq.query(sql).result()
    log.info("MERGE complete: %s", label)


def upsert_to_final(**context) -> None:
    _run_merge(bigquery.Client(project=PROJECT_ID), _ingestion_merge_sql(), "ingestion")


def upsert_backfill_to_final(**context) -> None:
    _run_merge(bigquery.Client(project=PROJECT_ID), _backfill_merge_sql(), "backfill")

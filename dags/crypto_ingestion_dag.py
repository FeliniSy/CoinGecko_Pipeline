from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from pipeline.extract import fetch_markets
from pipeline.load import load_to_staging
from pipeline.merge import upsert_to_final
from utils.logger import log

default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "on_failure_callback": lambda ctx: log.error(
        "Task failed: %s | run_id: %s",
        ctx["task_instance"].task_id,
        ctx["run_id"],
    ),
}

with DAG(
        dag_id="crypto_ingestion_dag",
        default_args=default_args,
        description="Fetch current top-1000 crypto prices from CoinGecko every 5 minutes → GCS → BigQuery",
        schedule_interval="*/5 * * * *",
        start_date=datetime(2025, 1, 1),
        catchup=False,
        max_active_runs=1,
        tags=["crypto", "ingestion", "realtime"],
) as dag:

    t1 = PythonOperator(
        task_id="fetch_markets",
        python_callable=fetch_markets,
        execution_timeout=timedelta(minutes=3),

    )

    t2 = PythonOperator(
        task_id="load_to_staging",
        python_callable=load_to_staging,
        execution_timeout=timedelta(minutes=2),

    )

    t3 = PythonOperator(
        task_id="upsert_to_final",
        python_callable=upsert_to_final,
        execution_timeout=timedelta(minutes=2),

    )

    t1 >> t2 >> t3

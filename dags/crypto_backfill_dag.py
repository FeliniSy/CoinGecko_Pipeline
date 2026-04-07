from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from pipeline.backfill_pipeline.extract_backfill import get_coin_list, fetch_historical_data
from pipeline.backfill_pipeline.load_backfill import load_backfill_to_staging
from pipeline.merge import upsert_to_final
from utils.logger import log

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "on_failure_callback": lambda ctx: log.error(
        "Backfill task failed: %s | run_id: %s",
        ctx["task_instance"].task_id,
        ctx["run_id"],
    ),
}

with DAG(
        dag_id="crypto_backfill_dag",
        default_args=default_args,
        description="Backfill 90 days of historical crypto data from CoinGecko → GCS → BigQuery",
        schedule_interval=None,
        start_date=datetime(2025, 1, 1),
        catchup=False,
        max_active_runs=1,
        tags=["crypto", "backfill_pipeline", "historical"],
) as dag:
    t1 = PythonOperator(
        task_id="get_coin_list",
        python_callable=get_coin_list,

    )

    t2 = PythonOperator(
        task_id="fetch_historical",
        python_callable=fetch_historical_data,
        execution_timeout=timedelta(hours=3),

    )

    t3 = PythonOperator(
        task_id="load_to_staging",
        python_callable=load_backfill_to_staging,
        execution_timeout=timedelta(minutes=30),

    )

    t4 = PythonOperator(
        task_id="upsert_to_final",
        python_callable=upsert_to_final,
        execution_timeout=timedelta(minutes=20),

    )

    t1 >> t2 >> t3 >> t4

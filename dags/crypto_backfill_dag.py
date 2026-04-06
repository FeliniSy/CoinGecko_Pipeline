from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from pipeline.extract_backfill import get_coin_list, fetch_historical_data
from pipeline.load_backfill import load_backfill_to_staging
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
        schedule_interval=None,  # Manual trigger only
        start_date=datetime(2025, 1, 1),
        catchup=False,
        max_active_runs=1,
        tags=["crypto", "backfill", "historical"],
) as dag:


    t1 = PythonOperator(
        task_id="get_coin_list",
        python_callable=get_coin_list,
        doc_md="""
        ## Get Coin List

        Fetches the current top 1000 cryptocurrencies by market cap.
        Stores coin IDs in XCom for the next task.

        **Duration:** ~30 seconds
        """,
    )

    t2 = PythonOperator(
        task_id="fetch_historical",
        python_callable=fetch_historical_data,
        execution_timeout=timedelta(hours=3),
        doc_md="""
        ## Fetch Historical Data

        Fetches 90 days of historical price/market_cap/volume data
        for each coin from /coins/{id}/market_chart endpoint.

        **Processing:**
        - Batches of 10 coins
        - 7 second delay between calls
        - 60 second delay between batches

        **Duration:** ~60-90 minutes for 1000 coins
        **Rate limits:** ~8-10 calls/minute
        """,
    )

    t3 = PythonOperator(
        task_id="load_to_staging",
        python_callable=load_backfill_to_staging,
        execution_timeout=timedelta(minutes=30),
        doc_md="""
        ## Load to Staging

        Transforms time-series data into flat rows and loads to
        crypto_prices_staging table.

        **Transformations:**
        - Flatten time-series arrays
        - Convert timestamp from ms to ISO format
        - Clean nulls and convert types

        **Expected rows:** ~90,000 (1000 coins × 90 days)
        """,
    )

    t4 = PythonOperator(
        task_id="upsert_to_final",
        python_callable=upsert_to_final,
        execution_timeout=timedelta(minutes=20),
        doc_md="""
        ## Upsert to Final Table

        Merges staging data into crypto_prices final table.
        Handles deduplication and updates existing records.

        **SQL:** MERGE with ROW_NUMBER() deduplication
        **Partitioning:** By DATE(timestamp)
        **Clustering:** By coin_id
        """,
    )

    t1 >> t2 >> t3 >> t4

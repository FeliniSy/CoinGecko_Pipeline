from google.cloud import bigquery

from utils.logger import log
from utils.settings import PROJECT_ID, BQ_DATASET, BQ_FINAL, BQ_STAGING


def upsert_to_final(**context) -> None:
    row_count: int = context["ti"].xcom_pull(task_ids="load_to_staging")
    bq = bigquery.Client(project=PROJECT_ID)

    merge_sql = f"""
    MERGE `{PROJECT_ID}.{BQ_DATASET}.{BQ_FINAL}` AS T
    USING (
      WITH flattened AS (
        SELECT
          JSON_VALUE(raw_data, '$.coin_id') AS coin_id,
          JSON_VALUE(raw_data, '$.symbol') AS symbol,
          JSON_VALUE(raw_data, '$.name') AS name,
          TIMESTAMP_MILLIS(CAST(JSON_VALUE(price_entry, '$[0]') AS INT64)) AS timestamp,
          CAST(JSON_VALUE(price_entry, '$[1]') AS FLOAT64) AS price,
          CAST(JSON_VALUE(mcap_entry, '$[1]') AS FLOAT64) AS market_cap,
          CAST(JSON_VALUE(vol_entry, '$[1]') AS FLOAT64) AS volume,
          ingested_at,
          source_file
        FROM `{PROJECT_ID}.{BQ_DATASET}.{BQ_STAGING}`,
        UNNEST(JSON_QUERY_ARRAY(raw_data, '$.prices')) AS price_entry WITH OFFSET pos_price
        LEFT JOIN UNNEST(JSON_QUERY_ARRAY(raw_data, '$.market_caps')) AS mcap_entry WITH OFFSET pos_mcap
          ON pos_price = pos_mcap
        LEFT JOIN UNNEST(JSON_QUERY_ARRAY(raw_data, '$.total_volumes')) AS vol_entry WITH OFFSET pos_vol
          ON pos_price = pos_vol
        WHERE JSON_VALUE(price_entry, '$[1]') IS NOT NULL
      )
      SELECT
        coin_id,
        symbol,
        name,
        price,
        market_cap,
        volume,
        timestamp,
        source_file
      FROM flattened
      QUALIFY ROW_NUMBER() OVER (
        PARTITION BY coin_id, TIMESTAMP_TRUNC(timestamp, MINUTE)
        ORDER BY ingested_at DESC
      ) = 1
    ) AS S
    ON  T.coin_id   = S.coin_id
    AND T.timestamp = S.timestamp

    WHEN NOT MATCHED THEN
      INSERT (coin_id, symbol, name, price, market_cap, volume, timestamp)
      VALUES (S.coin_id, S.symbol, S.name, S.price, S.market_cap, S.volume, S.timestamp)

    WHEN MATCHED AND (T.price != S.price OR T.market_cap != S.market_cap) THEN
      UPDATE SET
        price      = S.price,
        market_cap = S.market_cap,
        volume     = S.volume;
    """

    log.info("Running MERGE for ~%s rows...", row_count)
    bq.query(merge_sql).result()
    log.info("Upsert to %s.%s.%s complete.", PROJECT_ID, BQ_DATASET, BQ_FINAL)
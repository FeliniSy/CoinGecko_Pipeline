from utils.settings import PROJECT_ID, BQ_DATASET, BQ_FINAL, BQ_STAGING


def _backfill_merge_sql() -> str:
    return f"""
    MERGE `{PROJECT_ID}.{BQ_DATASET}.{BQ_FINAL}` AS T
    USING (
      WITH flattened AS (
        SELECT
          JSON_VALUE(raw_data, '$.coin_id')                                AS coin_id,
          JSON_VALUE(raw_data, '$.symbol')                                 AS symbol,
          JSON_VALUE(raw_data, '$.name')                                   AS name,
          TIMESTAMP_MILLIS(CAST(JSON_VALUE(price_entry, '$[0]') AS INT64)) AS timestamp,
          CAST(JSON_VALUE(price_entry, '$[1]') AS FLOAT64)                 AS price,
          CAST(JSON_VALUE(mcap_entry,  '$[1]') AS FLOAT64)                 AS market_cap,
          CAST(JSON_VALUE(vol_entry,   '$[1]') AS FLOAT64)                 AS volume,
          ingested_at
        FROM `{PROJECT_ID}.{BQ_DATASET}.{BQ_STAGING}`,
        UNNEST(JSON_QUERY_ARRAY(raw_data, '$.prices'))        AS price_entry WITH OFFSET pos_p,
        UNNEST(JSON_QUERY_ARRAY(raw_data, '$.market_caps'))   AS mcap_entry  WITH OFFSET pos_m,
        UNNEST(JSON_QUERY_ARRAY(raw_data, '$.total_volumes')) AS vol_entry   WITH OFFSET pos_v
        WHERE pos_p = pos_m AND pos_p = pos_v
          AND JSON_VALUE(price_entry, '$[1]') IS NOT NULL
      )
      SELECT coin_id, symbol, name, price, market_cap, volume, timestamp
      FROM flattened
      QUALIFY ROW_NUMBER() OVER (
        PARTITION BY coin_id, TIMESTAMP_TRUNC(timestamp, MINUTE)
        ORDER BY ingested_at DESC
      ) = 1
    ) AS S
    ON T.coin_id = S.coin_id AND T.timestamp = S.timestamp
    WHEN NOT MATCHED THEN
      INSERT (coin_id, symbol, name, price, market_cap, volume, timestamp)
      VALUES (S.coin_id, S.symbol, S.name, S.price, S.market_cap, S.volume, S.timestamp)
    WHEN MATCHED AND (T.price != S.price OR T.market_cap != S.market_cap) THEN
      UPDATE SET price = S.price, market_cap = S.market_cap, volume = S.volume;
    """

def _ingestion_merge_sql() -> str:
    return f"""
    MERGE `{PROJECT_ID}.{BQ_DATASET}.{BQ_FINAL}` AS T
    USING (
      SELECT
        JSON_VALUE(raw_data, '$.coin_id') AS coin_id,
        JSON_VALUE(raw_data, '$.symbol') AS symbol,
        JSON_VALUE(raw_data, '$.name') AS name,
        CAST(JSON_VALUE(raw_data, '$.current_price') AS FLOAT64) AS price,
        CAST(JSON_VALUE(raw_data, '$.market_cap') AS FLOAT64) AS market_cap,
        CAST(JSON_VALUE(raw_data, '$.total_volume') AS FLOAT64) AS volume,
        SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ',
          COALESCE(JSON_VALUE(raw_data, '$.last_updated'), JSON_VALUE(raw_data, '$.fetched_at'))
        ) AS timestamp,
        ingested_at
      FROM `{PROJECT_ID}.{BQ_DATASET}.{BQ_STAGING}`
      WHERE JSON_VALUE(raw_data, '$.current_price') IS NOT NULL
      QUALIFY ROW_NUMBER() OVER (
        PARTITION BY JSON_VALUE(raw_data, '$.coin_id'),
          TIMESTAMP_TRUNC(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ',
            COALESCE(JSON_VALUE(raw_data, '$.last_updated'), JSON_VALUE(raw_data, '$.fetched_at'))
          ), MINUTE)
        ORDER BY ingested_at DESC
      ) = 1
    ) AS S
    ON T.coin_id = S.coin_id AND T.timestamp = S.timestamp
    WHEN NOT MATCHED THEN
      INSERT (coin_id, symbol, name, price, market_cap, volume, timestamp)
      VALUES (S.coin_id, S.symbol, S.name, S.price, S.market_cap, S.volume, S.timestamp)
    WHEN MATCHED AND (T.price != S.price OR T.market_cap != S.market_cap) THEN
      UPDATE SET price = S.price, market_cap = S.market_cap, volume = S.volume;
    """
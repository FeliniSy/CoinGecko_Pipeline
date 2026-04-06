from google.cloud import bigquery

json_schema = [
    bigquery.SchemaField("raw_data", "JSON", mode='REQUIRED'),
    bigquery.SchemaField("source_file", "STRING"),
    bigquery.SchemaField("ingested_at", "TIMESTAMP", mode='REQUIRED')
]
# Schema for crypto_prices_staging and crypto_prices tables
schema = [
    bigquery.SchemaField("coin_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("symbol", "STRING"),
    bigquery.SchemaField("name", "STRING"),
    bigquery.SchemaField("price", "FLOAT64", mode="REQUIRED"),
    bigquery.SchemaField("market_cap", "FLOAT64"),
    bigquery.SchemaField("volume", "FLOAT64"),
    bigquery.SchemaField("timestamp", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("ingested_at", "STRING"),
    bigquery.SchemaField("source_file", "STRING"),
]
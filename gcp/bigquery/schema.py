from google.cloud import bigquery

json_schema = [
    bigquery.SchemaField("raw_data", "JSON", mode='REQUIRED'),
    bigquery.SchemaField("source_file", "STRING"),
    bigquery.SchemaField("ingested_at", "TIMESTAMP", mode='REQUIRED')
]
from google.cloud import bigquery

from utils.settings import PROJECT_ID

client = bigquery.Client(project = PROJECT_ID)
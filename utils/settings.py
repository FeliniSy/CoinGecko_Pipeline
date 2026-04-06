import os
from dotenv import load_dotenv

load_dotenv()

API_COIN_MARKET = os.getenv("API_COIN_MARKET")
COINGECKO_API_KEY = os.getenv("COINGECKO_API_KEY")
API_WITH_ID = os.getenv("API_WITH_ID")
GCS_BUCKET = os.getenv("GCS_BUCKET")
BQ_DATASET = os.getenv("BQ_DATASET")
PROJECT_ID = os.getenv("PROJECT_ID")
BQ_STAGING = os.getenv("BQ_STAGING")
BQ_FINAL = os.getenv("BQ_FINAL")
BASE_URL = os.getenv("BASE_URL")
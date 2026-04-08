import os
from dotenv import load_dotenv

load_dotenv()

COINGECKO_API_KEY = os.getenv("COINGECKO_API_KEY")
GCS_BUCKET = os.getenv("GCS_BUCKET")
BQ_DATASET = os.getenv("BQ_DATASET")
PROJECT_ID = os.getenv("PROJECT_ID")
BQ_STAGING = os.getenv("BQ_STAGING")
BQ_FINAL = os.getenv("BQ_FINAL")
BASE_URL = os.getenv("BASE_URL")


#AirFlow variables
VS_CURRENCY = "usd"
DAYS = 90
GCS_BASE_PATH = "crypto/raw"
PERE_PAGE = 250

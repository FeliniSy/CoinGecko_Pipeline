import time
from datetime import datetime

from gcp.gcs.gcs_client import gsc_client
from utils.helper import _get_with_retry, get_parameters
from utils.logger import log
from utils.settings import BASE_URL


def fetch_markets(**context) -> str:
    actual_dt = datetime.now()
    date_str = actual_dt.strftime("%Y-%m-%d")
    hour_str = actual_dt.strftime("%H")

    all_coins = []
    coins_id = []

    for page in range(1, 5):
        log.info("Fetching page %s/%s ...", page, 5)

        params = get_parameters(page)

        data = _get_with_retry(f"{BASE_URL}/markets", params)

        if not data:
            log.warning("Empty response on page %s — stopping early.", page)
            break

        for d in data:
            coins_id.append(d.get("id"))

        all_coins.extend(data)
        log.info("Page %s: got %s coins (total so far: %s)", page, len(data), len(all_coins))

        if page < 5:
            time.sleep(2)

    if not all_coins:
        raise ValueError("No data fetched from CoinGecko markets endpoint.")

    log.info("Total coins fetched: %s", len(all_coins))

    blob_path = gsc_client.upload_to_gcs(all_coins, date_str, hour_str, actual_dt)

    context["ti"].xcom_push(key="coin_ids", value=coins_id)
    log.info("Stored %s coin IDs in XCom for backfill_pipeline", len(coins_id))

    return blob_path

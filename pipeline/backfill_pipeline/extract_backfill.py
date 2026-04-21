import time
from datetime import datetime
from typing import Dict, Tuple

from gcp.gcs.gcs_client import gsc_client
from utils.helper import get_coin_list, _build_coin_blob_path, _fetch_coin_historical_data, \
    _build_coin_payload, _create_manifest
from utils.logger import log
from utils.settings import BASE_URL


def _process_single_coin(
        coin_metadata: Dict,
        date_str: str,
        hour: str,
        execution_dt: datetime,
        days: int = 90,
        sleep_seconds: int = 2
) -> Tuple[str, bool]:
    coin_id = coin_metadata.get("id")
    coin_blob_path = _build_coin_blob_path(date_str, hour, coin_id)

    if gsc_client.blob_exists(coin_blob_path, timeout=10):
        log.info("⏭ Skipping %s - already exists", coin_id)
        return coin_blob_path, True

    log.info("Fetching historical data for: %s", coin_id)

    data = _fetch_coin_historical_data(coin_id, days)

    if not data:
        log.warning("Empty response for coin: %s", coin_id)
        return None, False

    coin_payload = _build_coin_payload(coin_metadata, data, execution_dt.isoformat())
    gsc_client.upload_json(coin_blob_path, coin_payload, timeout=60)

    log.info("%s: %s price points uploaded", coin_id, len(data.get("prices", [])))

    if sleep_seconds > 0:
        time.sleep(sleep_seconds)

    return coin_blob_path, True


def fetch_historical_data(days: int = 90, sleep_seconds: int = 2, **context) -> str:
    all_coin_data = get_coin_list(BASE_URL)

    if not all_coin_data:
        raise ValueError("No coin IDs found from get_coin_list")

    execution_dt: datetime = context["execution_date"]
    date_str = execution_dt.strftime("%Y-%m-%d")
    hour = execution_dt.strftime('%H')

    log.info("Starting backfill for %s coins (%s days of history)...", len(all_coin_data), days)

    failed_coins = []
    coin_files = []

    for idx, coin_metadata in enumerate(all_coin_data, 1):
        coin_id = coin_metadata.get("id")
        log.info("Processing %s (%s/%s)...", coin_id, idx, len(all_coin_data))

        try:
            blob_path, success = _process_single_coin(
                coin_metadata, date_str, hour, execution_dt, days, sleep_seconds
            )

            if success:
                coin_files.append(blob_path)
            else:
                failed_coins.append(coin_id)

        except Exception as e:
            log.error("Failed to fetch %s: %s", coin_id, str(e))
            failed_coins.append(coin_id)

        if idx % 10 == 0:
            log.info("Progress: %s/%s coins processed (%s failed)",
                     idx, len(all_coin_data), len(failed_coins))

    if failed_coins:
        log.warning("Failed coins (%s): %s", len(failed_coins), failed_coins[:10])

    return _create_manifest(date_str, hour, execution_dt, len(all_coin_data), coin_files, failed_coins)

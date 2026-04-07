import json
from datetime import datetime
from typing import List, Dict
from google.cloud import storage

from utils.logger import log
from utils.settings import GCS_BUCKET


def validate_and_clean_array(array_data: list, array_name: str, coin_id: str) -> list:
    cleaned = []
    invalid_count = 0

    for entry in array_data:
        if not isinstance(entry, list) or len(entry) != 2:
            invalid_count += 1
            continue

        timestamp_ms, value = entry

        if not isinstance(timestamp_ms, (int, float)) or timestamp_ms <= 0:
            invalid_count += 1
            continue

        if value is not None and not isinstance(value, (int, float)):
            invalid_count += 1
            continue

        timestamp_ms = int(timestamp_ms)

        if value is None:
            invalid_count += 1
            continue

        value = float(value)

        if timestamp_ms < 946684800000 or timestamp_ms > 4102444800000:
            invalid_count += 1
            continue

        cleaned.append([timestamp_ms, value])

    if invalid_count > 0:
        log.warning("Coin %s - %s: removed %s invalid entries (kept %s valid)",
                    coin_id, array_name, invalid_count, len(cleaned))

    return cleaned


def transform_historical_data(**context) -> List[Dict]:
    manifest_path: str = context["ti"].xcom_pull(task_ids="fetch_historical")

    log.info("Loading backfill manifest from: gs://%s/%s", GCS_BUCKET, manifest_path)

    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET)
    manifest_blob = bucket.blob(manifest_path)
    manifest = json.loads(manifest_blob.download_as_text())

    coin_files = manifest["coin_files"]
    total_coins = manifest["total_coins"]
    successful_coins = manifest["successful_coins"]
    failed_coins = manifest["failed_coins"]

    log.info("Backfill manifest: %s coin files, %s/%s coins successful",
             len(coin_files), successful_coins, total_coins)

    if failed_coins:
        log.warning("Failed coins (%s): %s", len(failed_coins), failed_coins[:10])

    all_historical_data = []

    for idx, coin_file in enumerate(coin_files, 1):
        log.info("Reading coin %s/%s from gs://%s/%s",
                 idx, len(coin_files), GCS_BUCKET, coin_file)

        coin_blob = bucket.blob(coin_file)
        coin_data = json.loads(coin_blob.download_as_text())

        coin_data["source_file"] = f"gs://{GCS_BUCKET}/{coin_file}"
        all_historical_data.append(coin_data)

        log.info("Coin %s: loaded %s price points (total coins so far: %s)",
                 idx, len(coin_data.get("prices", [])), len(all_historical_data))

    if not all_historical_data:
        raise ValueError("No historical data found in manifest")

    log.info("Total historical data loaded: %s coins", len(all_historical_data))

    rows = []
    skipped_coins = 0

    log.info("Transforming historical data for %s coins...", len(all_historical_data))

    for idx, coin_data in enumerate(all_historical_data, 1):
        coin_id = coin_data.get("coin_id")
        symbol = coin_data.get("symbol")
        name = coin_data.get("name")
        prices = coin_data.get("prices", [])
        market_caps = coin_data.get("market_caps", [])
        total_volumes = coin_data.get("total_volumes", [])
        fetched_at = coin_data.get("fetched_at")
        source_file = coin_data.get("source_file")

        if not coin_id:
            log.warning("Skipping coin - missing coin_id")
            skipped_coins += 1
            continue

        cleaned_prices = validate_and_clean_array(prices, "prices", coin_id)
        cleaned_market_caps = validate_and_clean_array(market_caps, "market_caps", coin_id)
        cleaned_volumes = validate_and_clean_array(total_volumes, "total_volumes", coin_id)

        if not cleaned_prices and not cleaned_market_caps and not cleaned_volumes:
            log.warning("Skipping coin %s - no valid data after cleaning", coin_id)
            skipped_coins += 1
            continue

        raw_data = {
            "coin_id": coin_id,
            "symbol": symbol if symbol else None,
            "name": name if name else None,
            "prices": cleaned_prices,
            "market_caps": cleaned_market_caps,
            "total_volumes": cleaned_volumes,
            "fetched_at": fetched_at
        }

        try:
            ingested_at = datetime.fromisoformat(fetched_at.replace('Z', '+00:00'))
        except Exception as e:
            log.warning("Invalid fetched_at timestamp for %s: %s. Using current time.", coin_id, e)
            ingested_at = datetime.utcnow()

        row = {
            "raw_data": raw_data,
            "source_file": source_file if source_file else None,
            "ingested_at": ingested_at.isoformat()
        }

        rows.append(row)

        log.info("Coin %s/%s (%s): prepared with %s prices, %s market_caps, %s volumes",
                 idx, len(all_historical_data), coin_id, len(cleaned_prices), len(cleaned_market_caps),
                 len(cleaned_volumes))

    if not rows:
        raise ValueError("Zero valid rows after transformation")

    log.info("Transformation complete: %s rows from %s coins (skipped %s coins)",
             len(rows), len(all_historical_data), skipped_coins)

    return rows

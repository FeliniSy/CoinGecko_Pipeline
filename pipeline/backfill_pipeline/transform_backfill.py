import json
from datetime import datetime
from typing import List, Dict, Optional
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

        if value is None or not isinstance(value, (int, float)):
            invalid_count += 1
            continue

        timestamp_ms = int(timestamp_ms)

        cleaned.append([timestamp_ms, float(value)])

    if invalid_count > 0:
        log.warning("Coin %s - %s: removed %s invalid entries (kept %s valid)",
                    coin_id, array_name, invalid_count, len(cleaned))

    return cleaned


def transform_coin_data(coin_data: Dict) -> Optional[Dict]:
    coin_id = coin_data.get("coin_id")

    if not coin_id:
        log.warning("Skipping coin - missing coin_id")
        return None

    cleaned_prices = validate_and_clean_array(coin_data.get("prices", []), "prices", coin_id)
    cleaned_market_caps = validate_and_clean_array(coin_data.get("market_caps", []), "market_caps", coin_id)
    cleaned_volumes = validate_and_clean_array(coin_data.get("total_volumes", []), "total_volumes", coin_id)

    if not cleaned_prices and not cleaned_market_caps and not cleaned_volumes:
        log.warning("Skipping coin %s - no valid data after cleaning", coin_id)
        return None

    raw_data = {
        "coin_id": coin_id,
        "symbol": coin_data.get("symbol"),
        "name": coin_data.get("name"),
        "prices": cleaned_prices,
        "market_caps": cleaned_market_caps,
        "total_volumes": cleaned_volumes,
        "fetched_at": coin_data.get("fetched_at")
    }

    try:
        ingested_at = datetime.fromisoformat(coin_data.get("fetched_at").replace('Z', '+00:00'))
    except Exception as e:
        log.warning("Invalid fetched_at for %s: %s. Using current time.", coin_id, e)
        ingested_at = datetime.utcnow()

    return {
        "raw_data": raw_data,
        "source_file": coin_data.get("source_file"),
        "ingested_at": ingested_at.isoformat()
    }


def transform_historical_data(batch_number: int = None, total_batches: int = 1, **context) -> List[Dict]:
    manifest_path: str = context["ti"].xcom_pull(task_ids="fetch_historical")

    log.info("Loading manifest from: gs://%s/%s", GCS_BUCKET, manifest_path)

    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET)
    manifest = json.loads(bucket.blob(manifest_path).download_as_text())

    coin_files = manifest["coin_files"]
    log.info("Manifest: %s coin files", len(coin_files))

    if batch_number and total_batches > 1:
        batch_size = len(coin_files) // total_batches
        start_idx = (batch_number - 1) * batch_size
        end_idx = len(coin_files) if batch_number == total_batches else start_idx + batch_size
        batch_coin_files = coin_files[start_idx:end_idx]
        log.info("Batch %s/%s: coins %s-%s (%s total)", batch_number, total_batches,
                 start_idx + 1, end_idx, len(batch_coin_files))
    else:
        batch_coin_files = coin_files
        log.info("Processing all %s coins (no batching)", len(coin_files))

    all_coin_data = []
    for idx, coin_file in enumerate(batch_coin_files, 1):
        coin_data = json.loads(bucket.blob(coin_file).download_as_text())
        coin_data["source_file"] = f"gs://{GCS_BUCKET}/{coin_file}"
        all_coin_data.append(coin_data)

        if idx % 50 == 0:
            log.info("Loaded %s/%s coins", idx, len(batch_coin_files))

    log.info("Loaded %s coins from GCS", len(all_coin_data))

    rows = []
    for coin_data in all_coin_data:
        row = transform_coin_data(coin_data)
        if row:
            rows.append(row)

    if not rows:
        raise ValueError("Zero valid rows after transformation")

    log.info("Transformed %s/%s coins", len(rows), len(all_coin_data))
    return rows

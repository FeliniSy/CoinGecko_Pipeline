import json
import time
from datetime import datetime
from typing import List, Dict

from gcp.gcs.gcs_client import gsc_client
from utils.helper import _get_with_retry
from utils.logger import log
from utils.settings import GCS_BUCKET, BASE_URL


def get_coin_list(**context) -> List[str]:
    log.info("Fetching coin list for backfill_pipeline...")

    all_coin_data = []

    for page in range(1, 2):
        log.info("Fetching page %s/4 for coin list...", page)

        params = {
            "vs_currency": "usd",
            "order": "market_cap_desc",
            "per_page": 10,
            "page": page,
            "sparkline": "false",
        }
        data = _get_with_retry(f"{BASE_URL}/markets", params)

        if not data:
            log.warning("Empty response on page %s", page)
            break

        all_coin_data.extend(data)

        log.info("Page %s: collected %s coin IDs", page, len(data))

        if page < 4:
            time.sleep(2)

    log.info("Total coins to backfill_pipeline: %s", len(all_coin_data))

    context["ti"].xcom_push(key="coin_ids", value=all_coin_data)

    return all_coin_data


def fetch_historical_data(**context) -> str:

    coins_data: List[Dict] = context["ti"].xcom_pull(task_ids="get_coin_list", key="coin_ids")

    if not coins_data:
        raise ValueError("No coin IDs found in XCom from get_coin_list task")

    execution_dt: datetime = context["execution_date"]
    date_str = execution_dt.strftime("%Y-%m-%d")
    hour = execution_dt.strftime('%H')  # Just hour: "14", "09", etc.

    log.info("Starting backfill_pipeline for %s coins...", len(coins_data))

    successful_coins = 0
    failed_coins = []
    coin_files = []

    bucket = gsc_client.bucket

    for idx, each_coin in enumerate(coins_data, 1):
        coin_id = each_coin.get("id")
        coin_blob_path = f"crypto/raw/date={date_str}/hour={hour}/coin_{coin_id}.json"
        coin_blob = bucket.blob(coin_blob_path)

        if coin_blob.exists():
            log.info("⏭ Skipping %s (%s/%s) - already processed", coin_id, idx)
            coin_files.append(coin_blob_path)
            successful_coins += 1
            continue

        log.info("Processing %s (%s/%s)...", coin_id, idx)

        try:
            log.info("Fetching historical data for: %s", coin_id)

            url = f"{BASE_URL}/{coin_id}/market_chart"

            params = {
                "vs_currency": "usd",
                "days": "90",
                "interval": "daily",
            }

            data = _get_with_retry(url, params)

            if not data:
                log.warning("Empty response for coin: %s", coin_id)
                failed_coins.append(coin_id)
                continue

            coin_payload = {
                "coin_id": coin_id,
                "symbol" : each_coin.get("symbol"),
                "name" : each_coin.get("name"),
                "prices": data.get("prices", []),
                "market_caps": data.get("market_caps", []),
                "total_volumes": data.get("total_volumes", []),
                "fetched_at": execution_dt.isoformat(),
            }

            coin_blob.upload_from_string(
                json.dumps(coin_payload, ensure_ascii=False),
                content_type="application/json"
            )

            successful_coins += 1
            coin_files.append(coin_blob_path)

            log.info("✓ %s: uploaded %s price points → gs://%s/%s",
                     coin_id, len(data.get("prices", [])), GCS_BUCKET, coin_blob_path)

            time.sleep(2)

        except Exception as e:
            log.error("Failed to fetch %s: %s", coin_id, str(e))
            failed_coins.append(coin_id)
            continue

        if idx % 10 == 0:
            log.info("Progress: %s/%s coins (Success: %s, Failed: %s)",
                     idx, successful_coins, len(failed_coins))

    if successful_coins == 0:
        raise ValueError("Failed to fetch any historical data")

    log.info("Backfill fetch complete. Success: %s/%s coins",
             successful_coins)

    if failed_coins:
        log.warning("Failed coins (%s): %s", len(failed_coins), failed_coins[:10])

    manifest_path = f"crypto/raw/date={date_str}/hour={hour}/manifest.json"
    manifest = {
        "date": date_str,
        "hour": hour,
        "fetched_at": execution_dt.isoformat(),
        "total_coins": len(coins_data),
        "successful_coins": successful_coins,
        "failed_coins": failed_coins,
        "total_files": len(coin_files),
        "coin_files": coin_files,
    }

    manifest_blob = bucket.blob(manifest_path)
    manifest_blob.upload_from_string(
        json.dumps(manifest, ensure_ascii=False),
        content_type="application/json"
    )

    log.info("✓ Uploaded manifest → gs://%s/%s", GCS_BUCKET, manifest_path)

    return manifest_path

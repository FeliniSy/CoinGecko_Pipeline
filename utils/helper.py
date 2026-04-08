import time
from datetime import datetime
from typing import Dict, List

import requests

from gcp.gcs.gcs_client import gsc_client
from utils.logger import log
from utils.settings import COINGECKO_API_KEY, VS_CURRENCY, PERE_PAGE, GCS_BASE_PATH, BASE_URL


def _get_with_retry(url: str, params: Dict, attempt: int = 0) -> dict | list:
    try:
        headers = {"x-cg-demo-api-key": COINGECKO_API_KEY} if COINGECKO_API_KEY else {}
        resp = requests.get(url, params=params, headers=headers, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except requests.HTTPError as e:
        if resp.status_code == 429 and attempt < 5:
            wait = 60
            log.warning("Rate limited. Waiting %ss before retry %s...", wait, attempt + 1)
            time.sleep(wait)
            return _get_with_retry(url, params, attempt + 1)
        raise


def get_coin_list(url) -> List[str]:
    log.info("Fetching coin list for backfill_pipeline...")

    all_coin_data = []

    for page in range(1, 5):
        log.info("Fetching page %s/4 for coin list...", page)

        params = get_parameters(page)
        data = _get_with_retry(f"{url}/markets", params)

        if not data:
            log.warning("Empty response on page %s", page)
            break

        all_coin_data.extend(data)

        log.info("Page %s: collected %s coin IDs", page, len(data))

        if page < 4:
            time.sleep(2)

    log.info("Total coins to backfill_pipeline: %s", len(all_coin_data))

    return all_coin_data


def get_parameters(page):
    return {
        "vs_currency": VS_CURRENCY,
        "order": "market_cap_desc",
        "per_page": PERE_PAGE,
        "page": page,
        "sparkline": "false",
        "price_change_percentage": "24h",
    }


def _build_coin_blob_path(date_str: str, hour: str, coin_id: str) -> str:
    return f"{GCS_BASE_PATH}/date={date_str}/hour={hour}/coin_{coin_id}.json"


def _build_manifest_path(date_str: str, hour: str) -> str:
    return f"{GCS_BASE_PATH}/date={date_str}/hour={hour}/manifest.json"


def _fetch_coin_historical_data(coin_id: str, days: int = 90) -> Dict:
    url = f"{BASE_URL}/{coin_id}/market_chart"
    params = {"vs_currency": VS_CURRENCY, "days": str(days)}
    return _get_with_retry(url, params)


def _build_coin_payload(coin_metadata: Dict, market_data: Dict, fetched_at: str) -> Dict:
    return {
        "coin_id": coin_metadata.get("id"),
        "symbol": coin_metadata.get("symbol"),
        "name": coin_metadata.get("name"),
        "prices": market_data.get("prices", []),
        "market_caps": market_data.get("market_caps", []),
        "total_volumes": market_data.get("total_volumes", []),
        "fetched_at": fetched_at,
    }


def _create_manifest(
        date_str: str,
        hour: str,
        execution_dt: datetime,
        total_coins: int,
        coin_files: List[str],
        failed_coins: List[str]
) -> str:
    manifest_path = _build_manifest_path(date_str, hour)
    manifest = {
        "date": date_str,
        "hour": hour,
        "fetched_at": execution_dt.isoformat(),
        "total_coins": total_coins,
        "successful_coins": len(coin_files),
        "failed_coins": failed_coins,
        "total_files": len(coin_files),
        "coin_files": coin_files,
    }

    gsc_client.upload_json(manifest_path, manifest, timeout=60)
    log.info("✓ Manifest uploaded with %s coin files", len(coin_files))

    return manifest_path

from datetime import datetime
from typing import List, Dict

from utils.logger import log


def transform_historical_data(historical_data: List[Dict]) -> List[Dict]:
    rows = []
    total_coins = len(historical_data)

    log.info("Transforming historical data for %s coins...", total_coins)

    for idx, coin_data in enumerate(historical_data, 1):
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
            continue

        raw_data = {
            "coin_id": coin_id,
            "symbol": symbol,
            "name": name,
            "prices": prices,
            "market_caps": market_caps,
            "total_volumes": total_volumes,
            "fetched_at" : fetched_at
        }

        try:
            ingested_at = datetime.fromisoformat(fetched_at.replace('Z', '+00:00'))
        except Exception as e:
            log.warning("Invalid fetched_at timestamp for %s: %s. Using current time.", coin_id, e)
            ingested_at = datetime.utcnow()

        row = {
            "raw_data": raw_data,
            "source_file": source_file,
            "ingested_at": ingested_at.isoformat()
        }

        rows.append(row)

        log.info("Coin %s/%s (%s): prepared with %s prices, %s market_caps, %s volumes",
                 idx, total_coins, coin_id, len(prices), len(market_caps), len(total_volumes))

    if not rows:
        raise ValueError("Zero valid rows after transformation")

    log.info("Transformation complete: %s rows from %s coins", len(rows), total_coins)

    return rows

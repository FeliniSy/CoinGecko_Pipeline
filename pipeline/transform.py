from datetime import datetime
from utils.logger import log


def transform_market_data(coins: list, fetched_at: str, gcs_uri: str) -> list[dict]:
    rows = []

    for c in coins:
        coin_id = c.get("id", "")
        price = c.get("current_price")

        if not coin_id or price is None:
            log.warning("Skipping coin %s — missing coin_id or price.", coin_id)
            continue

        raw_data = {
            "coin_id": coin_id,
            "symbol": c.get("symbol", "").upper(),
            "name": c.get("name", ""),
            "current_price": float(price),
            "market_cap": float(c.get("market_cap")) if c.get("market_cap") is not None else None,
            "total_volume": float(c.get("total_volume")) if c.get("total_volume") is not None else None,
            "last_updated": c.get("last_updated"),
            "fetched_at": fetched_at,
        }

        try:
            ingested_at = datetime.fromisoformat(fetched_at.replace('Z', '+00:00'))
        except Exception as e:
            log.warning("Invalid fetched_at timestamp: %s. Using current time.", e)
            ingested_at = datetime.utcnow()

        rows.append({
            "raw_data": raw_data,
            "source_file": gcs_uri,
            "ingested_at": ingested_at.isoformat(),
        })

    if not rows:
        raise ValueError("Zero valid rows after transformation — all records were filtered out.")

    log.info("Transformed %s/%s coins (filtered %s due to missing data)",
             len(rows), len(coins), len(coins) - len(rows))

    return rows

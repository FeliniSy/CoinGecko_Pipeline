import time
from typing import Dict, List

import requests

from utils.logger import log
from utils.settings import COINGECKO_API_KEY


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


def get_coin_id(url: str) -> List:
    pass

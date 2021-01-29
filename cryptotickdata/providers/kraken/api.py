import datetime
import os

import httpx

from ...utils import iter_api
from .constants import (
    API_URL,
    KRAKEN_PAGINATION_ID,
    MAX_RESULTS,
    MIN_ELAPSED_PER_REQUEST,
)


def get_kraken_api_pagination_id(timestamp, last_data=[], data=[]):
    return os.environ.get(KRAKEN_PAGINATION_ID, None)


def get_kraken_api_timestamp(trade):
    timestamp = datetime.datetime.fromtimestamp(trade[2])
    return timestamp.replace(tzinfo=datetime.timezone.utc)


def get_trades(symbol, timestamp_from, pagination_id, log_prefix=None):
    url = f"{API_URL}/Trades?pair={symbol}"
    return iter_api(
        url,
        get_kraken_api_pagination_id,
        get_kraken_api_timestamp,
        get_kraken_api_response,
        MAX_RESULTS,
        MIN_ELAPSED_PER_REQUEST,
        timestamp_from=timestamp_from,
        pagination_id=pagination_id,
        log_prefix=log_prefix,
    )


def get_kraken_api_response(url, pagination_id=None):
    if pagination_id:
        url += f"&since={pagination_id}"
    response = httpx.get(url)
    if response.status_code == 200:
        data = response.json()
        result = data["result"]
        error = data["error"]
        if len(error):
            del os.environ[KRAKEN_PAGINATION_ID]
            raise Exception(str(error))
        os.environ[KRAKEN_PAGINATION_ID] = result["last"]
        for key in result:
            trades = result[key]
            break
        trades.reverse()
        return trades
    else:
        raise Exception(f"HTTP {response.status_code}: {response.reason_phrase}")

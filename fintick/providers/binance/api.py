import os
import time

import httpx

from ...constants import BINANCE_API_KEY, HTTPX_ERRORS
from ...utils import iter_api, parse_datetime
from .constants import API_URL, MAX_RESULTS, MIN_ELAPSED_PER_REQUEST


def get_binance_api_url(url, pagination_id):
    if pagination_id:
        return url + f"&fromId={pagination_id}"
    return url


def get_binance_api_pagination_id(timestamp, last_data=[], data=[]):
    # Like bybit, binance pagination feels like an IQ test
    if len(data):
        last_trade = data[-1]
        pagination_id = last_trade["id"] - len(data)
        assert pagination_id > 0
        return pagination_id


def get_binance_api_timestamp(trade):
    return parse_datetime(trade["time"], unit="ms")


def get_trades(symbol, timestamp_from, pagination_id, log_prefix=None):
    url = f"{API_URL}/historicalTrades?symbol={symbol}&limit={MAX_RESULTS}"
    return iter_api(
        url,
        get_binance_api_pagination_id,
        get_binance_api_timestamp,
        get_binance_api_response,
        MAX_RESULTS,
        MIN_ELAPSED_PER_REQUEST,
        timestamp_from=timestamp_from,
        pagination_id=pagination_id,
        log_prefix=log_prefix,
    )


def get_binance_api_response(url, pagination_id=None, retry=30):
    try:
        headers = {"X-MBX-APIKEY": os.environ.get(BINANCE_API_KEY, None)}
        response = httpx.get(get_binance_api_url(url, pagination_id), headers=headers)
        if response.status_code == 200:
            data = response.json()
            data.reverse()  # Descending order, please
            return data
        else:
            raise Exception(f"HTTP {response.status_code}: {response.reason_phrase}")
    except HTTPX_ERRORS as e:
        if retry > 0:
            time.sleep(1)
            retry -= 1
            return get_binance_api_response(url, pagination_id, retry)
        else:
            raise e

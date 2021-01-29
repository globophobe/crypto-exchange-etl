import time

import httpx
from ciso8601 import parse_datetime

from ...utils import iter_api
from .constants import API_URL, MAX_RESULTS, MIN_ELAPSED_PER_REQUEST


def get_bybit_api_url(url, pagination_id):
    if pagination_id:
        return url + f"&from={pagination_id}"
    return url


def get_bybit_api_pagination_id(timestamp, last_data=[], data=[]):
    # Like binance, bybit pagination feels like an IQ test
    if len(data):
        last_trade = data[-1]
        pagination_id = last_trade["id"] - len(data)
        assert pagination_id > 0
        return pagination_id


def get_bybit_api_timestamp(trade):
    return parse_datetime(trade["time"])


def get_trades(symbol, timestamp_from, pagination_id, log_prefix=None):
    url = f"{API_URL}/trading-records?symbol={symbol}&limit={MAX_RESULTS}"
    return iter_api(
        url,
        get_bybit_api_pagination_id,
        get_bybit_api_timestamp,
        get_bybit_api_response,
        MAX_RESULTS,
        MIN_ELAPSED_PER_REQUEST,
        timestamp_from=timestamp_from,
        pagination_id=pagination_id,
        log_prefix=log_prefix,
    )


def get_bybit_api_response(url, pagination_id=None, retry=5):
    try:
        response = httpx.get(get_bybit_api_url(url, pagination_id))
        if response.status_code == 200:
            data = response.json()
            assert data["ret_msg"] == "OK"
            result = data["result"]
            # If no pagination_id, ascending order
            if pagination_id:
                # Descending order, please
                result.reverse()
            return result
        else:
            raise Exception(f"HTTP {response.status_code}: {response.reason_phrase}")
    except httpx.ReadTimeout:
        time.sleep(1)
        retry -= 1
        return get_bybit_api_response(url, pagination_id, retry)

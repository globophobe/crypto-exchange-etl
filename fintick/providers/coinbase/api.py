import time

import httpx

from ...constants import HTTPX_ERRORS
from ...utils import iter_api, parse_datetime
from .constants import MAX_RESULTS, MIN_ELAPSED_PER_REQUEST, URL


def get_coinbase_api_url(url, pagination_id):
    if pagination_id:
        url = f"{url}?after={pagination_id}"
    return url


def get_coinbase_api_pagination_id(timestamp, last_data=[], data=[]):
    """
    Pagination details: https://docs.pro.coinbase.com/#pagination
    """
    if len(data):
        return data[-1]["trade_id"]


def get_coinbase_api_timestamp(trade):
    return parse_datetime(trade["time"])


def get_trades(symbol, timestamp_from, pagination_id, log_prefix=None):
    url = f"{URL}/products/{symbol}/trades"
    return iter_api(
        url,
        get_coinbase_api_pagination_id,
        get_coinbase_api_timestamp,
        get_coinbase_api_response,
        MAX_RESULTS,
        MIN_ELAPSED_PER_REQUEST,
        timestamp_from=timestamp_from,
        pagination_id=pagination_id,
        log_prefix=log_prefix,
    )


def get_coinbase_api_response(url, pagination_id=None, retry=30):
    try:
        response = httpx.get(get_coinbase_api_url(url, pagination_id))
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"HTTP {response.status_code}: {response.reason_phrase}")
    except HTTPX_ERRORS:
        if retry > 0:
            time.sleep(1)
            retry -= 1
            return get_coinbase_api_response(url, pagination_id, retry)
        raise

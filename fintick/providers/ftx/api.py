import json
import time
from decimal import Decimal

import httpx

from ...utils import iter_api, parse_datetime
from .constants import BTC, MAX_RESULTS, MIN_ELAPSED_PER_REQUEST, URL


def get_ftx_api_url(url, pagination_id):
    url += f"?limit={MAX_RESULTS}"
    if pagination_id:
        return url + f"&end_time={pagination_id}"
    return url


def get_ftx_api_pagination_id(timestamp, last_data=[], data=[]):
    """
    FTX API is seriously donkey balls. If more than 100 trades at same timestamp,
    the next timestamp is same as first. In such case, the pagination_id needs
    to be adjusted a small amount until unique trades are captured again.
    """
    pagination_id = format_ftx_api_timestamp(timestamp)
    ids = [trade["id"] for trade in last_data]
    unique = [trade["id"] for trade in data if trade["id"] not in ids]
    if len(data) == MAX_RESULTS and not len(unique):
        pagination_id -= 1e-6
        return round(pagination_id, 6)
    return pagination_id


def get_ftx_api_timestamp(trade):
    return parse_datetime(trade["time"])


def format_ftx_api_timestamp(timestamp):
    return timestamp.timestamp()


def get_trades(symbol, timestamp_from, pagination_id, log_prefix=None):
    url = f"{URL}/markets/{symbol}/trades"
    return iter_api(
        url,
        get_ftx_api_pagination_id,
        get_ftx_api_timestamp,
        get_ftx_api_response,
        MAX_RESULTS,
        MIN_ELAPSED_PER_REQUEST,
        timestamp_from=timestamp_from,
        pagination_id=pagination_id,
        log_prefix=log_prefix,
    )


def get_active_futures(root_symbol=BTC, verbose=True):
    # Not currently paginated
    url = f"{URL}/futures"
    return get_futures(url, root_symbol, verbose=verbose)


def get_expired_futures(root_symbol=BTC, verbose=True):
    # Not currently paginated
    url = f"{URL}/expired_futures"
    return get_futures(url, root_symbol, verbose=verbose)


def get_futures(url, root_symbol, verbose=True):
    response = httpx.get(url)
    data = response.json()
    result = data["result"]
    success = data["success"]
    futures = []
    if len(result) and success:
        for future in result:
            if future["underlying"] == root_symbol:
                if future["expiry"]:
                    futures.append(
                        {
                            "api_symbol": future["name"],
                            "expiry": parse_datetime(future["expiry"]),
                        }
                    )
    return futures


def get_ftx_api_response(url, pagination_id=None, retry=30):
    try:
        response = httpx.get(get_ftx_api_url(url, pagination_id))
        if response.status_code == 200:
            result = response.read()
            data = response.json()
            data = json.loads(result, parse_float=Decimal)
            if data["success"]:
                return data["result"]
            else:
                raise Exception(data["success"])
        else:
            raise Exception(f"HTTP {response.status_code}: {response.reason_phrase}")
    except (httpx.ConnectError, httpx.ConnectTimeout, httpx.ReadTimeout) as e:
        if retry > 0:
            time.sleep(1)
            retry -= 1
            return get_ftx_api_response(url, pagination_id, retry)
        else:
            raise e

import json
import re
import time

import httpx
from ciso8601 import parse_datetime

from ...utils import iter_api
from .constants import API_URL, MAX_RESULTS, MIN_ELAPSED_PER_REQUEST, MONTHS


def get_bitmex_api_url(url, pagination_id):
    url += f"&count={MAX_RESULTS}&reverse=true"
    if pagination_id:
        return url + f"&endTime={pagination_id}"
    return url


def get_bitmex_api_pagination_id(timestamp, last_data=[], data=[]):
    return format_bitmex_api_timestamp(timestamp)


def get_bitmex_api_timestamp(trade):
    return parse_datetime(trade["timestamp"])


def format_bitmex_api_timestamp(timestamp):
    return timestamp.replace(tzinfo=None).isoformat()


def get_active_futures(root_symbol, timestamp_from, pagination_id, log_prefix=None):
    endpoint = "instrument/active"
    return get_futures(
        endpoint, root_symbol, timestamp_from, pagination_id, log_prefix=log_prefix
    )


def get_expired_futures(root_symbol, timestamp_from, pagination_id, log_prefix=None):
    endpoint = "instrument"
    return get_futures(
        endpoint, root_symbol, timestamp_from, pagination_id, log_prefix=log_prefix
    )


def get_futures(endpoint, root_symbol, timestamp_from, pagination_id, log_prefix=None):
    filters = json.dumps({"rootSymbol": root_symbol})
    url = f"{API_URL}/{endpoint}?filter={filters}"
    timestamp_key = "timestamp"
    results = iter_api(
        url,
        timestamp_key,
        get_bitmex_api_pagination_id,
        get_bitmex_api_response,
        MAX_RESULTS,
        MIN_ELAPSED_PER_REQUEST,
        timestamp_from=timestamp_from,
        pagination_id=pagination_id,
        log_prefix=log_prefix,
    )
    instruments = []
    regex = re.compile(f"^{root_symbol}" + r"(\w)\d+$")
    for instrument in results:
        symbol = instrument["symbol"]
        match = regex.match(symbol)
        if match:
            is_future = match.group(1) in MONTHS
            if is_future:
                listing = parse_datetime(instrument["listing"])
                expiry = parse_datetime(instrument["expiry"])
                if expiry >= timestamp_from:
                    instruments.append(
                        {"symbol": symbol, "listing": listing, "expiry": expiry}
                    )
    return instruments


def get_funding(symbol, timestamp_from, pagination_id, log_prefix=None):
    url = f"{API_URL}/funding?symbol={symbol}"
    timestamp_key = "timestamp"
    return [
        {
            "timestamp": parse_datetime(f["timestamp"]),
            "rate": f["fundingRate"],
        }
        for f in iter_api(
            url,
            timestamp_key,
            get_bitmex_api_pagination_id,
            get_bitmex_api_response,
            parse_datetime,
            MAX_RESULTS,
            MIN_ELAPSED_PER_REQUEST,
            timestamp_from=timestamp_from,
            pagination_id=pagination_id,
            log_prefix=log_prefix,
        )
    ]


def get_trades(symbol, timestamp_from, pagination_id, log_prefix=None):
    url = f"{API_URL}/trade?symbol={symbol}"
    return iter_api(
        url,
        get_bitmex_api_pagination_id,
        get_bitmex_api_timestamp,
        get_bitmex_api_response,
        MAX_RESULTS,
        MIN_ELAPSED_PER_REQUEST,
        timestamp_from=timestamp_from,
        pagination_id=pagination_id,
        log_prefix=log_prefix,
    )


def get_bitmex_api_response(url, pagination_id=None, retry=5):
    try:
        response = httpx.get(get_bitmex_api_url(url, pagination_id))
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 429:
            retry = response.headers.get("Retry-After", 1)
            time.sleep(int(retry))
        else:
            raise Exception(f"HTTP {response.status_code}: {response.reason_phrase}")
    except httpx.ReadTimeout:
        time.sleep(1)
        retry -= 1
        return get_bitmex_api_response(url, pagination_id, retry)
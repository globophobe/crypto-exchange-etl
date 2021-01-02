import httpx
from ciso8601 import parse_datetime

from .constants import BTC, MAX_RESULTS, URL


def parse_timestamp(timestamp):
    if timestamp:
        return parse_datetime(timestamp)


def get_active_futures(root_symbol=BTC, verbose=True):
    url = f"{URL}/futures?limit={MAX_RESULTS}"
    return get_futures(url, root_symbol=root_symbol, verbose=verbose)


def get_expired_futures(root_symbol=BTC, verbose=True):
    url = f"{URL}/expired_futures?limit={MAX_RESULTS}"
    return get_futures(url, root_symbol=root_symbol, verbose=verbose)


def get_futures(url, root_symbol=BTC, verbose=True):
    response = httpx.get(url)
    data = response.json()
    result = data["result"]
    success = data["success"]
    futures = []
    if len(result) and success:
        for future in result:
            if future["underlying"] == root_symbol:
                listing = parse_timestamp(future["moveStart"])
                expiry = parse_timestamp(future["expiry"])
                if not future["name"].endswith("PERP"):
                    futures.append(
                        {
                            "api_symbol": future["name"],
                            "listing": listing,
                            "expiry": expiry,
                        }
                    )
    return futures

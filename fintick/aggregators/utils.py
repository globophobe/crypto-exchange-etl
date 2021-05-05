from copy import deepcopy

from fintick.fscache import FirestoreCache

from ..constants import FINTICK, FINTICK_AGGREGATE
from ..utils import normalize_symbol, publish
from .constants import AGGREGATORS


def get_decimal_value_for_table_id(value):
    string = str(value)
    if "." in string:
        head, tail = str(value).split(".")
        if any([int(item) for item in tail]):
            return f"{head}d{tail}"
        return head
    return string


def assert_aggregator(aggregator):
    aggregators = ", ".join(AGGREGATORS)
    assert aggregator in AGGREGATORS, f"aggregator should be one of {aggregators}"


def aggregate_callback(provider: str = None, symbol: str = None):
    """
    {"ftx": [{
        "BTC-MOVE": {
            "aggregators": [
                {
                    "aggregator": "threshold",
                    "thresh_attr": "notional",
                    "thresh_value": 1000,
                    "top_n": 100,
                    "callbacks: []
                }
            ],
            "futures": True
        }]
    }
    """
    data = FirestoreCache(FINTICK).get_one()
    if data is not None:
        if provider in data:
            symbols = data[provider]
            if symbol in symbols:
                for params in symbols[symbol]:
                    d = deepcopy(data)
                    d["provider"] = provider
                    d["symbol"] = symbol
                    publish(FINTICK_AGGREGATE, d)


def get_source_table(provider, symbol, futures=False, hot=False):
    symbol = normalize_symbol(provider, symbol)
    source_table = f"{provider}_{symbol}"
    if futures:
        source_table += "_futures"
    if hot:
        source_table += "_hot"
    return source_table + "_aggregated"

from ..bqloader import get_table_id
from ..utils import normalize_symbol
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


def get_source_table(provider, symbol, futures=False, hot=False):
    suffix = normalize_symbol(symbol, provider=provider)
    if futures:
        suffix += "_futures"
    if hot:
        suffix += "_hot"
    return get_table_id(provider, suffix=suffix)

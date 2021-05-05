from copy import copy

from fintick.aggregators import (
    candle_aggregator,
    renko_aggregator,
    thresh_aggregator,
    trade_aggregator,
)
from fintick.aggregators.constants import CANDLES, RENKO, THRESHOLD
from fintick.constants import (
    FINTICK,
    FINTICK_AGGREGATE,
    FINTICK_AGGREGATE_BARS,
    FINTICK_API,
)
from fintick.fscache import FirestoreCache
from fintick.functions import fintick_api
from fintick.pubsub.lib import publish
from fintick.pubsub.utils import base64_decode_event


def fintick_scheduler_gcp(event, context):
    """
    {
        "ftx": [
            {"BTC-MOVE": {"futures": True}},
            {"BTC-PERP": {"aggregations": [
                    {"type": "candles" "timeframe": "1m", "top_n": 25},
                ]}
            }
        ]
    }
    """
    data = FirestoreCache(FINTICK).get_one()
    if data is not None:
        for provider in data["providers"]:
            for symbol, kwargs in data[provider].items():
                params = {
                    "provider": provider,
                    "symbol": symbol,
                    "period_from": "3h",
                    "futures": kwargs.get("futures", False),
                }
                publish(FINTICK_API, params)


def fintick_api_gcp(event, context):
    params = base64_decode_event(event)
    keys = ("provider", "symbol", "period_from", "period_to")
    kwargs = {key: value for key, value in params.items() if key in keys}
    fintick_api(**kwargs)
    # Callback
    publish(FINTICK_AGGREGATE, params)


def fintick_aggregate_gcp(event, context):
    params = base64_decode_event(event)
    p = copy(params)
    provider = params.pop("provider")
    symbol = params.pop("symbol")
    trade_aggregator(provider, symbol, **params)
    # Callback
    data = FirestoreCache(FINTICK).get_one()
    if data is not None:
        for data_provider in data["providers"]:
            if data_provider == provider:
                for data_symbol, config in data[provider].items():
                    if data_symbol == symbol and "aggregations" in config:
                        for aggregation in config["aggregations"]:
                            p.update(aggregation)
                            publish(FINTICK_AGGREGATE_BARS, p)
                            break


def fintick_aggregate_bars_gcp(event, context):
    params = base64_decode_event(event)
    aggregation_type = params.pop("type")
    provider = params.pop("provider")
    symbol = params.pop("symbol")
    if aggregation_type == CANDLES:
        candle_aggregator(provider, symbol, **params)
    elif aggregation_type == THRESHOLD:
        thresh_aggregator(provider, symbol, **params)
    elif aggregation_type == RENKO:
        renko_aggregator(provider, symbol, **params)

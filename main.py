from flask import jsonify

from fintick import fintick_api
from fintick.aggregators import candle_aggregator, renko_aggregator, trade_aggregator
from fintick.utils import get_request_data


def fintick_api_gcp(request):
    params = (
        "provider",
        "symbol",
        "period_from",
        "period_to",
        "futures",
    )
    kwargs = get_request_data(request, params)
    fintick_api(**kwargs)
    return jsonify({"ok": True})


def fintick_aggregator_gcp(request):
    params = (
        "aggregator",
        "source_table",
        "period_from",
        "period_to",
        "futures",
    )
    kwargs = get_request_data(request, params)
    aggregator = kwargs.pop("aggregator", None)
    if aggregator == "trade_aggregator":
        trade_aggregator(**kwargs)
    elif aggregator == "candle_aggregator":
        kwargs.update(get_request_data(request, ("timeframe", "top_n")))
        candle_aggregator(**kwargs)
    elif aggregator == "renko_aggregator":
        kwargs.update(get_request_data(request, ("box_size", "top_n")))
        renko_aggregator(**kwargs)
    else:
        raise NotImplementedError
    return jsonify({"ok": True})

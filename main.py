from flask import jsonify

from fintick import fintick_api
from fintick.aggregators import candle_aggregator, trade_aggregator

from .utils import get_request_data


def fintick_api_gcp(request):
    params = ("provider", "symbol", "period_from", "period_to", "futures")
    kwargs = get_request_data(request, params)
    fintick_api(**kwargs)
    return jsonify({"ok": True})


def trade_aggregator_gcp(request):
    params = ("source_table", "period_from", "period_to", "futures")
    kwargs = get_request_data(request, params)
    trade_aggregator(**kwargs)
    return jsonify({"ok": True})


def candle_aggregator_gcp(request):
    params = (
        "source_table",
        "timeframe",
        "top_n",
        "period_from",
        "period_to",
        "futures",
    )
    kwargs = get_request_data(request, params)
    candle_aggregator(**kwargs)
    return jsonify({"OK": True})


def renko_aggregator_gcp(request):
    params = (
        "source_table",
        "box_size" "top_n",
        "period_from",
        "period_to",
        "futures",
    )
    kwargs = get_request_data(request, params)
    candle_aggregator(**kwargs)
    return jsonify({"OK": True})

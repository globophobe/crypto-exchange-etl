from flask import jsonify

from fintick.aggregators import trade_aggregator
from fintick.providers.binance import binance_perpetual
from fintick.providers.bitfinex import bitfinex_perpetual
from fintick.providers.bitmex import bitmex_futures, bitmex_perpetual
from fintick.providers.bybit import bybit_perpetual
from fintick.providers.coinbase import coinbase_spot
from fintick.providers.ftx import ftx_move, ftx_perpetual

from .utils import get_request_data

SPOT_OR_PERPETUAL = ("symbol", "period_from", "period_to")
FUTURE = ("root_symbol", "period_from", "period_to")
AGGREGATOR = ("source_table", "period_from", "period_to", "has_multiple_symbols")


def binance_perpetual_gcp(request):
    kwargs = get_request_data(request, SPOT_OR_PERPETUAL)
    binance_perpetual(**kwargs)
    return jsonify({"ok": True})


def bitfinex_perpetual_gcp(request):
    kwargs = get_request_data(request, SPOT_OR_PERPETUAL)
    bitfinex_perpetual(**kwargs)
    return jsonify({"ok": True})


def bitmex_perpetual_gcp(request):
    kwargs = get_request_data(request, SPOT_OR_PERPETUAL)
    bitmex_perpetual(**kwargs)
    return jsonify({"ok": True})


def bitmex_futures_gcp(request):
    kwargs = get_request_data(request, FUTURE)
    bitmex_futures(**kwargs)
    return jsonify({"ok": True})


def bybit_perpetual_gcp(request):
    kwargs = get_request_data(request, SPOT_OR_PERPETUAL)
    bybit_perpetual(**kwargs)
    return jsonify({"ok": True})


def coinbase_spot_gcp(request):
    kwargs = get_request_data(request, SPOT_OR_PERPETUAL)
    coinbase_spot(**kwargs)
    return jsonify({"ok": True})


def ftx_perpetual_gcp(request):
    kwargs = get_request_data(request, SPOT_OR_PERPETUAL)
    ftx_perpetual(**kwargs)
    return jsonify({"ok": True})


def ftx_move_gcp(request):
    kwargs = get_request_data(request, FUTURE)
    ftx_move(**kwargs)
    return jsonify({"ok": True})


def trade_aggregator_gcp(request):
    kwargs = get_request_data(request, AGGREGATOR)
    trade_aggregator(**kwargs)
    return jsonify({"ok": True})


# def candlestick_aggregator_gcp(request):
#     kwargs = get_request_data(request, AGGREGATOR)
#     candlestick_aggregator(**kwargs)
#     return jsonify({"OK": True})


def coinbase_spot_ai_platform_trigger(event, context):
    """
    Some Coinbase symbols, such as BTCUSD may run longer than 540 seconds.
    GCP AI platform training jobs, are essentially background jobs that
    run in Docker containers.  Although not the cheapest option, it is sufficient.
    """
    data = base64_decode_event(event)
    api_symbol = data.get("api_symbol", None)
    date_from = data.get("date", get_delta(days=-1).isoformat())
    date_to = get_delta(parse_datetime(date_from), days=1).isoformat()
    aggregate = data.get("aggregate", True)
    verbose = data.get("verbose", False)
    if api_symbol:
        CoinbaseSpotETLAIPlatformTrigger(
            api_symbol=api_symbol,
            date_from=date_from,
            date_to=date_to,
            aggregate=aggregate,
            verbose=verbose,
        ).main()

from flask import jsonify

from cryptotickdata.aggregators import trade_aggregator
from cryptotickdata.providers.binance import binance_perpetual
from cryptotickdata.providers.bitfinex import bitfinex_perpetual
from cryptotickdata.providers.bitmex import bitmex_futures, bitmex_perpetual
from cryptotickdata.providers.bybit import bybit_perpetual
from cryptotickdata.providers.coinbase import coinbase_spot
from cryptotickdata.providers.ftx import ftx_move, ftx_perpetual

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

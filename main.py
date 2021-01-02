from ciso8601 import parse_datetime

from cryptotick.aggregators import TradeAggregator
from cryptotick.providers.bitmex import (
    BitmexFuturesETL,
    BitmexFuturesETLTrigger,
    BitmexPerpetualETL,
    BitmexPerpetualETLTrigger,
)
from cryptotick.providers.bybit import BybitPerpetualETL, BybitPerpetualETLTrigger
from cryptotick.providers.coinbase import CoinbaseSpotETL, CoinbaseSpotETLDockerTrigger
from cryptotick.providers.ftx import FTXMOVEETL, FTXPerpetualETL
from cryptotick.utils import base64_decode_event, get_delta


def bitmex_futures_trigger(event, context):
    data = base64_decode_event(event)
    date = data.get("date", get_delta(days=-1).isoformat())
    root_symbols = [s for s in data.get("root_symbols", "").split(" ") if s]
    aggregate = data.get("aggregate", False)
    post_aggregation = data.get("post_aggregation", [])
    for root_symbol in root_symbols:
        BitmexFuturesETLTrigger(
            root_symbol,
            date_from=date,
            date_to=date,
            aggregate=aggregate,
            post_aggregation=post_aggregation,
        ).main()


def bitmex_perpetual_trigger(event, context):
    data = base64_decode_event(event)
    date = data.get("date", get_delta(days=-1).isoformat())
    symbols = [s for s in data.get("symbols", "").split(" ") if s]
    aggregate = data.get("aggregate", False)
    post_aggregation = data.get("post_aggregation", [])
    if symbols:
        BitmexPerpetualETLTrigger(
            symbols,
            date_from=date,
            date_to=date,
            aggregate=aggregate,
            post_aggregation=post_aggregation,
        ).main()


def bitmex_futures(event, context):
    data = base64_decode_event(event)
    root_symbol = data.get("root_symbol", None)
    date = data.get("date", get_delta(days=-1).isoformat())
    aggregate = data.get("aggregate", False)
    post_aggregation = data.get("post_aggregation", [])
    if root_symbol:
        BitmexFuturesETL(
            root_symbol,
            date_from=date,
            date_to=date,
            aggregate=aggregate,
            post_aggregation=post_aggregation,
        ).main()


def bitmex_perpetual(event, context):
    data = base64_decode_event(event)
    symbols = [s for s in data.get("symbols", "").split(" ") if s]
    date = data.get("date", get_delta(days=-1).isoformat())
    aggregate = data.get("aggregate", False)
    post_aggregation = data.get("post_aggregation", [])
    if symbols:
        BitmexPerpetualETL(
            symbols,
            date_from=date,
            date_to=date,
            aggregate=aggregate,
            post_aggregation=post_aggregation,
        ).main()


def bybit_trigger(event, context):
    data = base64_decode_event(event)
    date = data.get("date", get_delta(days=-1).isoformat())
    symbols = [s for s in data.get("symbols", "").split(" ") if s]
    aggregate = data.get("aggregate", False)
    post_aggregation = data.get("post_aggregation", [])
    for symbol in symbols:
        BybitPerpetualETLTrigger(
            symbol,
            date_from=date,
            date_to=date,
            aggregate=aggregate,
            post_aggregation=post_aggregation,
        ).main()


def bybit_perpetual(event, context):
    data = base64_decode_event(event)
    symbol = data.get("symbol", None)
    date = data.get("date", get_delta(days=-1).isoformat())
    aggregate = data.get("aggregate", False)
    post_aggregation = data.get("post_aggregation", [])
    if symbol:
        BybitPerpetualETL(
            symbol,
            date_from=date,
            date_to=date,
            aggregate=aggregate,
            post_aggregation=post_aggregation,
        ).main()


def ftx_move(event, context):
    data = base64_decode_event(event)
    date_from = data.get("date", get_delta(days=-1).isoformat())
    date_to = get_delta(parse_datetime(date_from), days=1).isoformat()
    aggregate = data.get("aggregate", False)
    post_aggregation = data.get("post_aggregation", [])
    FTXMOVEETL(
        date_from=date_from,
        date_to=date_to,
        aggregate=aggregate,
        post_aggregation=post_aggregation,
    ).main()


def ftx_perpetual(event, context):
    data = base64_decode_event(event)
    api_symbol = data.get("api_symbol", None)
    date_from = data.get("date", get_delta(days=-1).isoformat())
    date_to = get_delta(parse_datetime(date_from), days=1).isoformat()
    aggregate = data.get("aggregate", False)
    post_aggregation = data.get("post_aggregation", [])
    if api_symbol:
        FTXPerpetualETL(
            api_symbol,
            date_from=date_from,
            date_to=date_to,
            aggregate=aggregate,
            post_aggregation=post_aggregation,
        ).main()


def coinbase_spot(event, context):
    data = base64_decode_event(event)
    api_symbol = data.get("api_symbol", None)
    date_from = data.get("date", get_delta(days=-1).isoformat())
    date_to = get_delta(parse_datetime(date_from), days=1).isoformat()
    aggregate = data.get("aggregate", False)
    post_aggregation = data.get("post_aggregation", [])
    if api_symbol:
        CoinbaseSpotETL(
            api_symbol=api_symbol,
            date_from=date_from,
            date_to=date_to,
            aggregate=aggregate,
            post_aggregation=post_aggregation,
        ).main()


def coinbase_spot_ai_platform_trigger(event, context):
    """
    Some Coinbase symbols, such as BTCUSD may run longer than 540 seconds.
    GCP AI platform training jobs, are essentially background jobs that
    run in Docker containers.  Although not the cheapest option, it is sufficient.

    Coinbase trade ID is a sequential integer. So, if live data is collected, and
    there are no gaps between trade IDs, there is no reason to trigger a GCP AI platform
    training job.
    """
    data = base64_decode_event(event)
    api_symbol = data.get("api_symbol", None)
    date_from = data.get("date", get_delta(days=-1).isoformat())
    date_to = get_delta(parse_datetime(date_from), days=1).isoformat()
    aggregate = data.get("aggregate", False)
    post_aggregation = data.get("post_aggregation", [])
    if api_symbol:
        CoinbaseSpotETLDockerTrigger(
            api_symbol=api_symbol,
            date_from=date_from,
            date_to=date_to,
            aggregate=aggregate,
            post_aggregation=post_aggregation,
        ).main()


def trade_aggregator(event, context):
    data = base64_decode_event(event)
    table_name = data.get("table_name", None)
    date = data.get("date", get_delta(days=-1).isoformat())
    has_multiple_symbols = data.get("has_multiple_symbols", False)
    post_aggregation = data.get("post_aggregation", [])
    if table_name:
        TradeAggregator(
            table_name,
            date_from=date,
            date_to=date,
            has_multiple_symbols=has_multiple_symbols,
            post_aggregation=post_aggregation,
        ).main()

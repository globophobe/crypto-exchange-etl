from ...utils import parse_period_from_to
from .futures import BitmexFuturesDailyPartition, BitmexFuturesHourlyPartition
from .perpetual import BitmexPerpetualDailyPartition, BitmexPerpetualHourlyPartition


def bitmex_perpetual(
    symbol: str = None,
    period_from: str = None,
    period_to: str = None,
    verbose: bool = False,
):
    assert symbol, 'Required param "symbol" not provided'
    timestamp_from, timestamp_to, date_from, date_to = parse_period_from_to(
        period_from=period_from, period_to=period_to
    )
    if timestamp_from and timestamp_to:
        BitmexPerpetualHourlyPartition(
            symbol,
            period_from=timestamp_from,
            period_to=timestamp_to,
        ).main()
    if date_from and date_to:
        BitmexPerpetualDailyPartition(
            symbol,
            period_from=date_from,
            period_to=date_to,
            verbose=verbose,
        ).main()


def bitmex_futures(
    root_symbol: str = None,
    period_from: str = None,
    period_to: str = None,
    verbose: bool = False,
):
    assert root_symbol, 'Required param "root_symbol" not provided'
    timestamp_from, timestamp_to, date_from, date_to = parse_period_from_to(
        period_from=period_from, period_to=period_to
    )
    if timestamp_from and timestamp_to:
        BitmexFuturesHourlyPartition(
            root_symbol,
            period_from=timestamp_from,
            period_to=timestamp_to,
        ).main()
    if date_from and date_to:
        BitmexFuturesDailyPartition(
            root_symbol,
            period_from=date_from,
            period_to=date_to,
            verbose=verbose,
        ).main()

from ...utils import parse_period_from_to
from ..utils import get_source_table
from .candles import CandleAggregatorDailyPartition, CandleAggregatorHourlyPartition


def candle_aggregator(
    provider: str,
    symbol: str,
    period_from: str,
    period_to: str,
    timeframe: str,
    top_n: int = 0,
    futures: bool = False,
    verbose: bool = False,
):
    assert timeframe, 'Required param "timeframe" not provided'
    timestamp_from, timestamp_to, date_from, date_to = parse_period_from_to(
        period_from=period_from, period_to=period_to
    )
    # Reversed, daily then hourly
    if date_from and date_to:
        CandleAggregatorDailyPartition(
            get_source_table(provider, symbol, futures=futures),
            period_from=date_from,
            period_to=date_to,
            futures=futures,
            timeframe=timeframe,
            top_n=top_n,
            verbose=verbose,
        ).main()
    if timestamp_from and timestamp_to:
        CandleAggregatorHourlyPartition(
            get_source_table(provider, symbol, futures=futures, hot=True),
            period_from=timestamp_from,
            period_to=timestamp_to,
            futures=futures,
            timeframe=timeframe,
            top_n=top_n,
            verbose=verbose,
        ).main()

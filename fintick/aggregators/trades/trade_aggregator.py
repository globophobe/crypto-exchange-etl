import pandas as pd

from ...utils import get_hot_date, parse_period_from_to
from ..utils import get_source_table
from .trades import (
    TradeAggregatorDailyPartition,
    TradeAggregatorDailyPartitionFromHourly,
    TradeAggregatorHourlyPartition,
)


def trade_aggregator(
    provider: str,
    symbol: str,
    period_from: str = None,
    period_to: str = None,
    futures: bool = False,
    verbose: bool = False,
):
    timestamp_from, timestamp_to, date_from, date_to = parse_period_from_to(
        period_from=period_from, period_to=period_to
    )
    if timestamp_from and timestamp_to:
        TradeAggregatorHourlyPartition(
            get_source_table(provider, symbol, futures=futures, hot=True),
            period_from=timestamp_from,
            period_to=timestamp_to,
            futures=futures,
            verbose=verbose,
        ).main()
    if date_from and date_to:
        # Try loading most recent daily data from hourly
        if date_to == get_hot_date():
            TradeAggregatorDailyPartitionFromHourly(
                get_source_table(provider, symbol, futures=futures),
                period_from=date_to,
                period_to=date_to,
                verbose=verbose,
            ).main()
            # Modify date_to by 1 day
            date_to -= pd.Timedelta("1d")
        TradeAggregatorDailyPartition(
            get_source_table(provider, symbol, futures=futures),
            period_from=date_from,
            period_to=date_to,
            futures=futures,
            verbose=verbose,
        ).main()

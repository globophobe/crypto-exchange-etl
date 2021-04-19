from ...utils import get_hot_date, parse_period_from_to
from .trades import (
    TradeAggregatorDailyPartition,
    TradeAggregatorDailyPartitionFromHourly,
    TradeAggregatorHourlyPartition,
)


def trade_aggregator(
    source_table: str = None,
    period_from: str = None,
    period_to: str = None,
    has_multiple_symbols: bool = False,
    verbose: bool = False,
):
    assert source_table, 'Required param "source_table" not provided'
    timestamp_from, timestamp_to, date_from, date_to = parse_period_from_to(
        period_from=period_from, period_to=period_to
    )
    if timestamp_from and timestamp_to:
        TradeAggregatorHourlyPartition(
            f"{source_table}_hot",
            period_from=timestamp_from,
            period_to=timestamp_to,
            has_multiple_symbols=has_multiple_symbols,
            verbose=verbose,
        ).main()
    if date_from and date_to:
        # Try loading most recent daily data from hourly
        if date_to == get_hot_date():
            TradeAggregatorDailyPartitionFromHourly(
                source_table,
                period_from=date_to,
                period_to=date_to,
                verbose=verbose,
            ).main()
        TradeAggregatorDailyPartition(
            source_table,
            period_from=date_from,
            period_to=date_to,
            has_multiple_symbols=has_multiple_symbols,
            verbose=verbose,
        ).main()

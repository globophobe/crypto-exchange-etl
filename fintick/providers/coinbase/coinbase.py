from ...utils import get_hot_date, parse_period_from_to
from .spot import (
    CoinbaseDailyPartition,
    CoinbaseDailyPartitionFromHourly,
    CoinbaseHourlyPartition,
)


def coinbase_spot(
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
        CoinbaseHourlyPartition(
            symbol,
            period_from=timestamp_from,
            period_to=timestamp_to,
            verbose=verbose,
        ).main()
    if date_from and date_to:
        # Try loading most recent daily data from hourly
        if date_to == get_hot_date():
            CoinbaseDailyPartitionFromHourly(
                symbol,
                period_from=date_to,
                period_to=date_to,
                verbose=verbose,
            ).main()
        CoinbaseDailyPartition(
            symbol,
            period_from=date_from,
            period_to=date_to,
            verbose=verbose,
        ).main()

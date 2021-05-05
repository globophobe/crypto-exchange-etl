import datetime

import pandas as pd

from ...utils import get_max_hot_date, parse_period_from_to
from .perpetual import (
    BitflyerDailyPartition,
    BitflyerDailyPartitionFromHourly,
    BitflyerHourlyPartition,
)


def bitflyer_perpetual(
    symbol: str = None,
    period_from: str = None,
    period_to: str = None,
    verbose: bool = False,
):
    assert symbol, 'Required param "symbol" not provided'
    timestamp_from, timestamp_to, date_from, date_to = parse_period_from_to(
        period_from=period_from, period_to=period_to
    )
    thirty_one_days_ago = datetime.datetime.utcnow().date() - pd.Timedelta("31d")
    if date_from < thirty_one_days_ago:
        date_from = thirty_one_days_ago
        date = date_from.isoformat()
        print(f"Bitflyer limits trades to 31 days, period_from modified to {date}.")
    if timestamp_from and timestamp_to:
        BitflyerHourlyPartition(
            symbol,
            period_from=timestamp_from,
            period_to=timestamp_to,
            verbose=verbose,
        ).main()
    if date_from and date_to:
        # Try loading most recent daily data from hourly
        if date_to >= get_max_hot_date():
            ok = BitflyerDailyPartitionFromHourly(
                symbol,
                period_from=date_to,
                period_to=date_to,
                verbose=verbose,
            ).main()
            if ok:
                # Modify date_to by 1 day
                date_to -= pd.Timedelta("1d")
        BitflyerDailyPartition(
            symbol,
            period_from=date_from,
            period_to=date_to,
            verbose=verbose,
        ).main()

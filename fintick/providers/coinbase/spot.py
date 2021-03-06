import datetime

from ...controllers import (
    FinTick,
    FinTickDailyMixin,
    FinTickDailyPartitionFromHourlyMixin,
    FinTickDailySequentialIntegerMixin,
    FinTickHourlyMixin,
    FinTickREST,
)
from ...s3downloader import assert_type_decimal
from .base import CoinbaseMixin
from .constants import BTCUSD


class CoinbaseHourlyPartition(FinTickHourlyMixin, CoinbaseMixin, FinTickREST):
    pass


class CoinbaseDailyPartitionFromHourly(
    FinTickDailyPartitionFromHourlyMixin, CoinbaseMixin, FinTick
):
    pass


class CoinbaseDailyPartition(
    FinTickDailySequentialIntegerMixin,
    FinTickDailyMixin,
    CoinbaseMixin,
    FinTickREST,
):
    def assert_data_frame(self, data_frame, trades):
        # Duplicates.
        assert len(data_frame["uid"].unique()) == len(trades)
        # Missing orders.
        expected = len(trades) - 1
        if self.api_symbol == BTCUSD:
            # It seems 45 ids may have been skipped for BTC-USD on 2021-06-09
            if self.partition == datetime.date(2021, 6, 9):
                expected = len(trades) + 45
            # There was a missing order for BTC-USD on 2019-04-11
            elif self.partition == datetime.date(2019, 4, 11):
                expected = len(trades)
        diff = data_frame["index"].diff().dropna()
        assert abs(diff.sum()) == expected
        # Decimal
        assert_type_decimal(data_frame, ("price", "volume", "notional"))
        # Timestamps
        self.assert_timestamps(data_frame)

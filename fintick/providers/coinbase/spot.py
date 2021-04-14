import datetime

from ...fintick import (
    FinTick,
    FinTickDailyHourlyMixin,
    FinTickDailyMixin,
    FinTickDailyPartitionFromHourlySequentialIntegerMixin,
    FinTickHourlyMixin,
    FinTickREST,
)
from .base import CoinbaseMixin
from .constants import BTCUSD, ETHUSD


class CoinbaseHourlyPartition(FinTickHourlyMixin, CoinbaseMixin, FinTickREST):
    pass


class CoinbaseDailyPartitionFromHourly(
    FinTickDailyPartitionFromHourlySequentialIntegerMixin, CoinbaseMixin, FinTick
):
    pass


class CoinbaseDailyPartition(
    FinTickDailyMixin, FinTickDailyHourlyMixin, CoinbaseMixin, FinTickREST
):
    def assert_data_frame(self, data_frame, trades):
        # Duplicates.
        assert len(data_frame["uid"].unique()) == len(trades)
        # Missing orders.
        expected = len(trades) - 1
        if self.api_symbol == BTCUSD:
            # There was a missing order for BTC-USD on 2019-04-11.
            if self.partition == datetime.date(2019, 4, 11):
                expected = len(trades)
        if self.api_symbol == ETHUSD:
            # There were 22 missing orders for ETH-USD on 2020-09-04.
            if self.partition == datetime.date(2020, 9, 4):
                expected = len(trades) + 21
        diff = data_frame["index"].diff().dropna()
        assert abs(diff.sum()) == expected
        super().assert_data_frame(data_frame, trades)

from ...cryptotickdata import CryptoTickDailyPartitionFromHourlyMixin
from ..base import DailyAggregatorMixin, HourlyAggregatorMixin
from .base import BaseTradeAggregator


class TradeAggregatorHourlyPartition(HourlyAggregatorMixin, BaseTradeAggregator):
    pass


class TradeAggregatorDailyPartitionFromHourly(
    CryptoTickDailyPartitionFromHourlyMixin, BaseTradeAggregator
):
    pass


class TradeAggregatorDailyPartition(DailyAggregatorMixin, BaseTradeAggregator):
    pass

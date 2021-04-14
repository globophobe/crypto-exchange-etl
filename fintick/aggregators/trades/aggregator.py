from ...fintick import FinTickDailyPartitionFromHourlyMixin
from ..base import DailyAggregatorMixin, HourlyAggregatorMixin
from .base import BaseTradeAggregator


class TradeAggregatorHourlyPartition(HourlyAggregatorMixin, BaseTradeAggregator):
    pass


class TradeAggregatorDailyPartitionFromHourly(
    FinTickDailyPartitionFromHourlyMixin, BaseTradeAggregator
):
    pass


class TradeAggregatorDailyPartition(DailyAggregatorMixin, BaseTradeAggregator):
    pass

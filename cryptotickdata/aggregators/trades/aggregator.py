from ..base import DailyAggregatorMixin, HourlyAggregatorMixin
from .base import BaseTradeAggregator


class TradeAggregatorHourlyPartition(HourlyAggregatorMixin, BaseTradeAggregator):
    pass


class TradeAggregatorDailyPartition(DailyAggregatorMixin, BaseTradeAggregator):
    pass

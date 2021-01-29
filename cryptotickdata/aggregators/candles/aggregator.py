from ..base import DailyAggregatorMixin, HourlyAggregatorMixin
from .base import BaseCandleAggregator


class CandleAggregatorHourlyPartition(HourlyAggregatorMixin, BaseCandleAggregator):
    pass


class CandleAggregatorDailyPartition(DailyAggregatorMixin, BaseCandleAggregator):
    pass

from ..base import DailyCacheAggregatorMixin, HourlyCacheAggregatorMixin
from .base import BaseCandleAggregator


class CandleAggregatorHourlyPartition(HourlyCacheAggregatorMixin, BaseCandleAggregator):
    pass


class CandleAggregatorDailyPartition(DailyCacheAggregatorMixin, BaseCandleAggregator):
    pass

from ..base import DailyCacheAggregatorMixin, HourlyCacheAggregatorMixin
from .base import BaseThreshAggregator


class ThreshAggregatorHourlyPartition(HourlyCacheAggregatorMixin, BaseThreshAggregator):
    pass


class ThreshAggregatorDailyPartition(DailyCacheAggregatorMixin, BaseThreshAggregator):
    pass

from ..base import DailyCacheAggregatorMixin, HourlyCacheAggregatorMixin
from .base import BaseCumsumAggregator


class CumsumAggregatorHourlyPartition(HourlyCacheAggregatorMixin, BaseCumsumAggregator):
    pass


class CumsumAggregatorDailyPartition(DailyCacheAggregatorMixin, BaseCumsumAggregator):
    pass

from ..base import DailyCacheAggregatorMixin, HourlyCacheAggregatorMixin
from .base import BaseRenkoAggregator


class RenkoAggregatorHourlyPartition(HourlyCacheAggregatorMixin, BaseRenkoAggregator):
    pass


class RenkoAggregatorDailyPartition(DailyCacheAggregatorMixin, BaseRenkoAggregator):
    pass

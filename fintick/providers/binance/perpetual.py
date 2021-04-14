from ...fintick import (
    FinTick,
    FinTickDailyMixin,
    FinTickDailyPartitionFromHourlySequentialIntegerMixin,
    FinTickHourlyMixin,
    FinTickREST,
)
from .base import BinanceMixin


class BinancePerpetualHourlyPartition(FinTickHourlyMixin, BinanceMixin, FinTickREST):
    pass


class BinanceDailyPartitionFromHourly(
    FinTickDailyPartitionFromHourlySequentialIntegerMixin, BinanceMixin, FinTick
):
    pass


class BinancePerpetualDailyPartition(FinTickDailyMixin, BinanceMixin, FinTickREST):
    pass

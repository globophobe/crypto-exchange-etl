from ...fintick import (
    FinTick,
    FinTickDailyHourlySequentialIntegerMixin,
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


class BinancePerpetualDailyPartition(
    FinTickDailyMixin,
    FinTickDailyHourlySequentialIntegerMixin,
    BinanceMixin,
    FinTickREST,
):
    pass

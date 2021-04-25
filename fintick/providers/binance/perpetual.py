from ...fintick import (
    FinTick,
    FinTickDailyPartitionFromHourlyMixin,
    FinTickDailySequentialIntegerMixin,
    FinTickHourlyMixin,
    FinTickREST,
)
from .base import BinanceMixin


class BinancePerpetualHourlyPartition(FinTickHourlyMixin, BinanceMixin, FinTickREST):
    pass


class BinanceDailyPartitionFromHourly(
    FinTickDailyPartitionFromHourlyMixin, BinanceMixin, FinTick
):
    pass


class BinancePerpetualDailyPartition(
    FinTickDailySequentialIntegerMixin,
    BinanceMixin,
    FinTickREST,
):
    pass
